import WebSocket, { WebSocketServer } from 'ws';
import fetch from 'node-fetch';
import express from 'express';

// ── CONFIG ───────────────────────────────────────────────────
const WHALE_THRESHOLD = 20000;        // $20,000 notional
const WINDOW_SECONDS = 300;           // 5‑min rolling window
const TOP_N = 5;                      // number of pairs to track
const REFRESH_INTERVAL_MS = 5 * 60_000; // refresh top pairs every 5 min
const BROADCAST_INTERVAL_MS = 2000;   // push scores every 2 sec

// ── STATE ────────────────────────────────────────────────────
const whaleTrades = new Map();        // pair -> [{time, side, notional}]
let topPairs = [];                    // e.g. ['BTCUSDT', 'ETHUSDT', …]
let pairScores = new Map();
let bybitWS = null;

// ── HELPERS ──────────────────────────────────────────────────

/** Fetch top USDT spot pairs by 24h turnover from Bybit (public) */
async function fetchTopPairs(n = TOP_N) {
  try {
    const res = await fetch('https://api.bybit.com/v5/market/tickers?category=spot');
    const json = await res.json();
    if (json.retCode !== 0 || !json.result?.list) {
      console.error('❌ Bybit ticker error:', json.retMsg);
      return [];
    }
    const all = json.result.list;
    // Filter USDT pairs, sort by turnover (quote volume)
    const usdt = all
      .filter(t => t.symbol.endsWith('USDT'))
      .sort((a, b) => parseFloat(b.turnover24h) - parseFloat(a.turnover24h))
      .slice(0, n)
      .map(t => t.symbol);   // e.g. "BTCUSDT"
    return usdt;
  } catch (e) {
    console.error('❌ fetchTopPairs error:', e.message);
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
      if (t.side === 'Buy') buyVol += t.notional;
      else sellVol += t.notional;
    }
    const total = buyVol + sellVol;
    scores.set(pair, total === 0 ? 0 : (buyVol - sellVol) / total);
  }
  return scores;
}

// ── BYBIT WEBSOCKET (public spot trades) ────────────────────

function connectBybitStream(pairs) {
  if (bybitWS) {
    bybitWS.close(1000, 'Refreshing pairs');
    bybitWS = null;
  }

  bybitWS = new WebSocket('wss://stream.bybit.com/v5/public/spot');

  bybitWS.on('open', () => {
    console.log('✅ Bybit WS open. Subscribing to trades...');
    // Subscribe to each pair's trade channel
    const args = pairs.map(p => `publicTrade.${p}`);
    bybitWS.send(JSON.stringify({ op: 'subscribe', args }));
    console.log(`📡 Subscribed to: ${args.join(', ')}`);
  });

  bybitWS.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      // Bybit trade message: { topic: "publicTrade.BTCUSDT", type: "snapshot"|"delta", data: [...] }
      if (msg.topic && msg.topic.startsWith('publicTrade.') && msg.data) {
        const pair = msg.topic.replace('publicTrade.', '');
        const trades = msg.data;
        for (let trade of trades) {
          const price = parseFloat(trade.p);
          const amount = parseFloat(trade.v);
          const notional = price * amount;
          // Bybit: S = "Buy" or "Sell" (aggressor side)
          const side = trade.S;

          if (notional >= WHALE_THRESHOLD) {
            if (!whaleTrades.has(pair)) whaleTrades.set(pair, []);
            whaleTrades.get(pair).push({
              time: trade.T / 1000,   // trade time in ms → seconds
              side,                    // "Buy" or "Sell"
              notional,
            });
          }
        }
      }
    } catch (e) {}
  });

  bybitWS.on('close', (code, reason) => {
    console.log(`🔴 Bybit disconnected – code: ${code}, reason: ${reason}`);
    bybitWS = null;
    setTimeout(() => {
      if (topPairs.length > 0) {
        console.log('🔄 Reconnecting to Bybit…');
        connectBybitStream(topPairs);
      }
    }, 5000);
  });

  bybitWS.on('error', (err) => console.error('🚨 Bybit WS error:', err.message));
}

// ── DYNAMIC PAIR REFRESH ─────────────────────────────────────
async function refreshPairs() {
  const newPairs = await fetchTopPairs(TOP_N);
  if (newPairs.length === 0) {
    console.warn('⚠️ Could not refresh pairs, keeping current list.');
    return;
  }

  if (JSON.stringify(newPairs) !== JSON.stringify(topPairs)) {
    console.log('🔄 Top pairs changed. Updating Bybit stream…');
    for (let pair of whaleTrades.keys()) {
      if (!newPairs.includes(pair)) whaleTrades.delete(pair);
    }
    topPairs = newPairs;
    connectBybitStream(topPairs);
  } else {
    topPairs = newPairs;
  }
}

// ── EXPRESS + CLIENT WEBSOCKET ──────────────────────────────
const app = express();
const PORT = process.env.PORT || 4000;
app.get('/health', (_, res) => res.send('OK'));
const server = app.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));

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

setInterval(refreshPairs, REFRESH_INTERVAL_MS);

// ── STARTUP with retry ───────────────────────────────────────
async function initPairs() {
  let pairs = [];
  let attempts = 0;
  while (pairs.length === 0 && attempts < 10) {
    pairs = await fetchTopPairs(TOP_N);
    if (pairs.length === 0) {
      console.log(`⏳ No pairs fetched, retrying in 5s (attempt ${attempts + 1}/10)...`);
      await new Promise(r => setTimeout(r, 5000));
    }
    attempts++;
  }
  if (pairs.length === 0) {
    console.error('❌ Could not get pairs after 10 attempts. Retrying in background.');
    return;
  }
  topPairs = pairs;
  console.log('🏆 Top pairs:', topPairs.join(', '));
  connectBybitStream(topPairs);
}

initPairs();
