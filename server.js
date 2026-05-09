'use strict';

const express = require('express');
const http    = require('http');
const WebSocket = require('ws');
const net     = require('net');
const path    = require('path');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(express.static(path.join(__dirname, 'public')));

const SERVICES = {
  puppet: [
    { name: 'puppetserver',   port: 8140 },
    { name: 'puppetdb',       port: 8081 },
    { name: 'consoleweb',     port: 4430 },
    { name: 'nodeclassifier', port: 4433 },
    { name: 'nginx',          port: 443  },
    { name: 'orchestration1', port: 8142 },
    { name: 'orchestration2', port: 8143 },
    { name: 'postgresql',     port: 5432 },
  ],
  gitlab: [
    { name: 'postgresql',   port: 5432 },
    { name: 'gitlabrails',  port: 443  },
    { name: 'puma',         port: 8080 },
    { name: 'redis',        port: 6379 },
  ],
};

/**
 * Attempt a TCP connection to determine if a port is open.
 * Resolves true on connect, false on timeout or error.
 * @param {number} port
 * @returns {Promise<boolean>}
 */
function checkPort(port) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    socket.setTimeout(2000);
    socket.connect(port, '127.0.0.1');
    socket.on('connect', () => { socket.destroy(); resolve(true);  });
    socket.on('timeout', () => { socket.destroy(); resolve(false); });
    socket.on('error',   () => { socket.destroy(); resolve(false); });
  });
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
        services.map(async ({ name, port }) => ({
          name,
          port,
          available: await checkPort(port),
        }))
      );
    })
  );
  return { timestamp: new Date().toISOString(), groups };
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
