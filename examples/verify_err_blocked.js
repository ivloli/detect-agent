#!/usr/bin/env node

const WebSocket = require('ws');

const wsUrl = process.env.WS_URL || process.argv[2];
const targetUrl = process.env.TARGET_URL || process.argv[3] || 'https://example.com';
const timeoutMs = Number(process.env.TIMEOUT_MS || process.argv[4] || 12000);

if (!wsUrl) {
  console.error('usage: WS_URL=<ws://127.0.0.1:9222/devtools/page/<id>> node verify_err_blocked.js [ws_url] [target_url] [timeout_ms]');
  process.exit(1);
}

const ws = new WebSocket(wsUrl);
let nextId = 1;

function send(method, params = {}) {
  ws.send(JSON.stringify({ id: nextId++, method, params }));
}

ws.on('open', () => {
  send('Page.enable');
  send('Network.enable');
  // Intercept requests and fail with BlockedByClient to force ERR_BLOCKED_*.
  send('Fetch.enable', { patterns: [{ urlPattern: '*', requestStage: 'Request' }] });
  send('Page.navigate', { url: targetUrl });
});

ws.on('message', (raw) => {
  let msg;
  try {
    msg = JSON.parse(raw.toString());
  } catch {
    return;
  }

  if (msg.method === 'Fetch.requestPaused') {
    const rid = msg?.params?.requestId;
    if (rid) {
      send('Fetch.failRequest', { requestId: rid, errorReason: 'BlockedByClient' });
    }
  }

  if (msg.method === 'Network.loadingFailed') {
    const errorText = msg?.params?.errorText || '';
    const blockedReason = msg?.params?.blockedReason || '';
    console.log(`loadingFailed: ${errorText} ${blockedReason}`.trim());

    if (errorText.includes('ERR_BLOCKED') || String(blockedReason).toLowerCase().includes('blocked')) {
      console.log('HIT_ERR_BLOCKED');
      process.exit(0);
    }
  }
});

ws.on('error', (err) => {
  console.error('websocket error:', err.message);
});

setTimeout(() => {
  console.log('timeout');
  process.exit(2);
}, timeoutMs);
