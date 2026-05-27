'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const net = require('net');
const dgram = require('dgram');
const path = require('path');
const fs = require('fs');

const LOG_FILE = path.join(__dirname, 'status-changes.log');

// Maps `${group}:${name}` → boolean (last known availability)
const prevStatus = new Map();

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

/**
 * Return the last 500 status-change log entries as a JSON array (oldest first).
 * Returns an empty array if the log file does not yet exist.
 */
app.get('/api/log', (req, res) => {
  fs.readFile(LOG_FILE, 'utf8', (err, data) => {
    if (err) {
      if (err.code === 'ENOENT') return res.json([]);
      return res.status(500).json({ error: err.message });
    }
    const entries = data
      .split('\n')
      .filter(line => line.trim())
      .map(line => { try { return JSON.parse(line); } catch (_) { return null; } })
      .filter(Boolean);
    // Return only the most recent 500 to keep payload small
    res.json(entries.slice(-500));
  });
});

app.use(express.static(path.join(__dirname, 'public')));

const SERVICES = {
  puppet: [
    { name: 'puppetserver',   node: 'localhost',    port: 8140, protocol: 'TCP' },
    { name: 'puppetdb',       node: 'localhost',    port: 8081, protocol: 'TCP' },
    { name: 'consoleweb',     node: 'example1.com', port: 4430, protocol: 'TCP' },
    { name: 'nodeclassifier', node: 'example1.com', port: 4433, protocol: 'TCP' },
    { name: 'nginx',          node: 'localhost',    port: 443,  protocol: 'TCP' },
    { name: 'orchestration1', node: 'example2.com', port: 8142, protocol: 'TCP' },
    { name: 'orchestration2', node: 'example2.com', port: 8143, protocol: 'TCP' },
    { name: 'postgresql',     node: 'localhost',    port: 5432, protocol: 'TCP' },
  ],
  gitlab: [
    { name: 'postgresql',  node: 'localhost',    port: 5432, protocol: 'TCP' },
    { name: 'gitlabrails', node: 'example1.com', port: 443,  protocol: 'TCP' },
    { name: 'puma',        node: 'localhost',    port: 8080, protocol: 'TCP' },
    { name: 'redis',       node: 'localhost',    port: 6379, protocol: 'TCP' },
  ],
  suma: [
    { name: 'postgresql',  node: 'localhost', port: 5432, protocol: 'TCP' },
    { name: 'ssh',         node: 'suma.com',  port: 22,   protocol: 'TCP' },
    { name: 'gitlabrails', node: 'localhost', port: 443,  protocol: 'TCP' },
    { name: 'salt',        node: 'suma.com',  port: 8080, protocol: 'TCP' },
    { name: 'redis',       node: 'suma.com',  port: 6379, protocol: 'TCP' },
  ],
};

/**
 * TCP reachability check: resolve true on connect, false on timeout or error.
 * @param {string} host
 * @param {number} port
 * @returns {Promise<boolean>}
 */
function checkTCP(host, port) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    socket.setTimeout(2000);
    socket.connect(port, host);
    socket.on('connect', () => { socket.destroy(); resolve(true); });
    socket.on('timeout', () => { socket.destroy(); resolve(false); });
    socket.on('error',   () => { socket.destroy(); resolve(false); });
  });
}

/**
 * UDP reachability check: send an empty datagram and wait 2 s.
 * An ICMP port-unreachable reply triggers an error event → false.
 * Silence within the timeout is treated optimistically as true.
 * @param {string} host
 * @param {number} port
 * @returns {Promise<boolean>}
 */
function checkUDP(host, port) {
  return new Promise((resolve) => {
    const socket = dgram.createSocket('udp4');
    let settled = false;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      try { socket.close(); } catch (_) {}
      resolve(result);
    };
    const timer = setTimeout(() => finish(true), 2000);
    socket.on('error', () => { clearTimeout(timer); finish(false); });
    socket.send(Buffer.alloc(0), port, host, (err) => {
      if (err) { clearTimeout(timer); finish(false); }
    });
  });
}

/**
 * Dispatch to the correct protocol checker.
 * @param {string} host
 * @param {number} port
 * @param {'TCP'|'UDP'} protocol
 * @returns {Promise<boolean>}
 */
function checkService(host, port, protocol) {
  return protocol === 'UDP' ? checkUDP(host, port) : checkTCP(host, port);
}

/**
 * Check all configured services in parallel and return a status snapshot.
 * @returns {Promise<{timestamp: string, groups: Record<string, Array>}>}
 */
async function checkAllServices() {
  const groups = {};
  await Promise.all(
    Object.entries(SERVICES).map(async ([group, services]) => {
      groups[group] = await Promise.all(
        services.map(async ({ name, node, port, protocol }) => ({
          name,
          node,
          port,
          protocol,
          available: await checkService(node, port, protocol),
        }))
      );
    })
  );
  return { timestamp: new Date().toISOString(), groups };
}

/**
 * Compare current check results against the previous snapshot.
 * Append any state transitions to the log file as NDJSON lines.
 * @param {{timestamp: string, groups: Record<string, Array>}} snapshot
 */
function detectAndLogChanges({ timestamp, groups }) {
  const lines = [];

  Object.entries(groups).forEach(([group, services]) => {
    services.forEach(({ name, node, port, available }) => {
      const key  = `${group}:${name}`;
      const prev = prevStatus.get(key);

      if (prev !== undefined && prev !== available) {
        const entry = { timestamp, group, service: name, node, port, from: prev, to: available };
        lines.push(JSON.stringify(entry));
        console.log(
          `[${timestamp}] ${group}:${name} (${node}:${port}) ` +
          `${prev ? 'UP' : 'DOWN'} → ${available ? 'UP' : 'DOWN'}`
        );
      }

      prevStatus.set(key, available);
    });
  });

  if (lines.length > 0) {
    fs.appendFile(LOG_FILE, lines.join('\n') + '\n', (err) => {
      if (err) console.error('Failed to write status log:', err.message);
    });
  }
}

let currentStatus = null;

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

async function runChecks() {
  currentStatus = await checkAllServices();
  detectAndLogChanges(currentStatus);
  broadcast({ type: 'status', ...currentStatus });
}

wss.on('connection', (ws) => {
  if (currentStatus) {
    ws.send(JSON.stringify({ type: 'status', ...currentStatus }));
  }
});

const CHECK_INTERVAL_MS = 10_000;
setInterval(runChecks, CHECK_INTERVAL_MS);
runChecks();

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`SYSMON running  →  http://localhost:${PORT}`);
});
