import WebSocket, { WebSocketServer } from 'ws';
import fetch from 'node-fetch';
import express from 'express';

// ── CONFIG ───────────────────────────────────────────────────
const WHALE_THRESHOLD = 20000;        // $20,000 notional
const WINDOW_SECONDS = 300;           // 5‑min rolling window
const TOP_N = 5;                      // 👈 now only 5 pairs
const REFRESH_INTERVAL_MS = 5 * 60_000; // refresh ranking every 5 min
const BROADCAST_INTERVAL_MS = 2000;   // push scores every 2 sec

// ── STATE ────────────────────────────────────────────────────
const whaleTrades = new Map();        // pair → [{time, side, notional}]
let topPairs = [];                    // e.g. ["BTC/USDT","ETH/USDT",…]
let pairScores = new Map();
let gateWS = null;
let subscriptionChannels = [];

// ── HELPERS ──────────────────────────────────────────────────
async function fetchTopPairs(n = TOP_N) {
  try {
    const res = await fetch('https://api.gateio.ws/api/v4/spot/tickers');
    const tickers = await res.json();
    const usdtPairs = tickers
      .filter(t => t.currency_pair.endsWith('_USDT'))
      .sort((a, b) => parseFloat(b.quote_volume) - parseFloat(a.quote_volume))
      .slice(0, n)
      .map(t => t.currency_pair.replace('_', '/'));
    return usdtPairs;
  } catch (e) {
    console.error('❌ fetchTopPairs error:', e);
    return [];
  }
}

function cleanOldTrades() {
  const cutoff = Date.now() / 1000 - WINDOW_SECONDS;
  for (let [pair, trades] of whaleTrades) {
    whaleTrades.set(pair, trades.filter(t => t.time >= cutoff));
  }
}

function computeScores() {
  cleanOldTrades();
  const scores = new Map();
  for (let pair of topPairs) {
    const trades = whaleTrades.get(pair) || [];
    let buyVol = 0, sellVol = 0;
    for (let t of trades) {
      if (t.side === 'buy') buyVol += t.notional;
      else sellVol += t.notional;
    }
    const total = buyVol + sellVol;
    scores.set(pair, total === 0 ? 0 : (buyVol - sellVol) / total);
  }
  return scores;
}

// ── GATE.IO WEBSOCKET ───────────────────────────────────────
function connectGateIO(channels) {
  gateWS = new WebSocket('wss://ws.gate.io/v4/');

  gateWS.on('open', () => {
    console.log(`✅ Gate.io WS open. Subscribing after 500ms delay...`);
    // 🚨 Delay to avoid immediate disconnect
    setTimeout(() => {
      gateWS.send(JSON.stringify({
        time: Math.floor(Date.now() / 1000),
        channel: 'spot.trades',
        event: 'subscribe',
        payload: channels
      }));
      console.log(`📡 Subscribed to: ${channels.join(', ')}`);
    }, 1000);
  });

  gateWS.on('message', (data) => {

    try {
      const msg = JSON.parse(data);
      if (msg.channel === 'spot.trades' && msg.event === 'update') {
          console.log('📥 Trade:', msg.result.currency_pair, msg.result.side, msg.result.price, msg.result.amount);
      }
      // 👀 Log subscription / unsubscribe responses
      if (msg.event === 'subscribe') {
        console.log('🔔 Subscription response:', JSON.stringify(msg).slice(0, 200));
        return;
      }
      if (msg.event === 'unsubscribe') {
        console.log('🔕 Unsubscribe response:', JSON.stringify(msg).slice(0, 200));
        return;
      }

      // Process trades
      if (msg.channel === 'spot.trades' && msg.event === 'update') {
        const trade = msg.result;
        const pair = trade.currency_pair.replace('_', '/');
        const side = trade.side;
        const price = parseFloat(trade.price);
        const amount = parseFloat(trade.amount);
        const notional = price * amount;

        if (notional >= WHALE_THRESHOLD) {
          if (!whaleTrades.has(pair)) whaleTrades.set(pair, []);
          whaleTrades.get(pair).push({
            time: trade.time,
            side,
            notional
          });
        }
      }
    } catch (e) {
      // ignore malformed JSON
    }
  });

  gateWS.on('close', (code, reason) => {
    console.log(`🔴 Disconnected – code: ${code}, reason: ${reason}`);
    console.log('🔄 Reconnecting in 5s...');
    setTimeout(() => connectGateIO(subscriptionChannels), 5000);
  });

  gateWS.on('error', (err) => {
    console.error('🚨 Gate WS error:', err.message);
  });
}

// ── DYNAMIC SUBSCRIPTION UPDATE ─────────────────────────────
function updateGateSubscription(newPairs) {
  if (!gateWS || gateWS.readyState !== WebSocket.OPEN) return;

  const newChannels = newPairs.map(p => p.replace('/', '_'));
  const oldSet = new Set(subscriptionChannels);
  const newSet = new Set(newChannels);

  const toRemove = [...oldSet].filter(ch => !newSet.has(ch));
  if (toRemove.length) {
    gateWS.send(JSON.stringify({
      time: Math.floor(Date.now() / 1000),
      channel: 'spot.trades',
      event: 'unsubscribe',
      payload: toRemove
    }));
    console.log('🗑 Unsubscribed:', toRemove);
  }

  const toAdd = [...newSet].filter(ch => !oldSet.has(ch));
  if (toAdd.length) {
    gateWS.send(JSON.stringify({
      time: Math.floor(Date.now() / 1000),
      channel: 'spot.trades',
      event: 'subscribe',
      payload: toAdd
    }));
    console.log('➕ Subscribed:', toAdd);
  }

  subscriptionChannels = newChannels;
}

// ── RANKING REFRESH LOOP ─────────────────────────────────────
async function refreshPairs() {
  const newPairs = await fetchTopPairs(TOP_N);
  if (newPairs.length === 0) return;

  if (JSON.stringify(newPairs) !== JSON.stringify(topPairs)) {
    console.log('🔄 Top pairs changed. Updating subscriptions...');
    topPairs = newPairs;
    updateGateSubscription(topPairs);
    // clear stale data
    for (let pair of whaleTrades.keys()) {
      if (!topPairs.includes(pair)) whaleTrades.delete(pair);
    }
  } else {
    topPairs = newPairs; // keep order
  }
}

// ── EXPRESS + CLIENT WEBSOCKET ──────────────────────────────
const app = express();
const PORT = 4000;
app.get('/health', (_, res) => res.send('OK'));
const server = app.listen(PORT, () => console.log(`🚀 Server on http://localhost:${PORT}`));

const wss = new WebSocketServer({ server });
wss.on('connection', (ws) => {
  console.log('🖥 Frontend connected');
  ws.send(JSON.stringify({ type: 'scores', data: Array.from(pairScores) }));
});

setInterval(() => {
  pairScores = computeScores();
  const payload = JSON.stringify({ type: 'scores', data: Array.from(pairScores) });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}, BROADCAST_INTERVAL_MS);

// ── STARTUP ──────────────────────────────────────────────────
(async () => {
  topPairs = await fetchTopPairs(TOP_N);
  if (topPairs.length === 0) {
    console.error('❌ No pairs fetched. Exiting.');
    process.exit(1);
  }
  console.log('🏆 Top 5 pairs:', topPairs.join(', '));
  subscriptionChannels = topPairs.map(p => p.replace('/', '_'));
  connectGateIO(subscriptionChannels);

  setInterval(refreshPairs, REFRESH_INTERVAL_MS);
})();
