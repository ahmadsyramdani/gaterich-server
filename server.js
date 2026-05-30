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
const whaleTrades = new Map();
let topPairs = [];
let pairScores = new Map();
let binanceWS = null;
let currentStreamUrl = '';

// ── HELPERS ──────────────────────────────────────────────────

async function fetchTopPairs(n = TOP_N) {
  try {
    const res = await fetch('https://api.binance.com/api/v3/ticker/24hr', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      }
    });
    const data = await res.json();

    // 🚨 If not an array, log what we got
    if (!Array.isArray(data)) {
      console.error('❌ Binance returned non-array:', JSON.stringify(data).slice(0, 300));
      return [];
    }

    const usdt = data
      .filter(t => t.symbol && t.symbol.endsWith('USDT'))
      .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
      .slice(0, n)
      .map(t => t.symbol);
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
      if (t.side === 'buy') buyVol += t.notional;
      else sellVol += t.notional;
    }
    const total = buyVol + sellVol;
    scores.set(pair, total === 0 ? 0 : (buyVol - sellVol) / total);
  }
  return scores;
}

function buildStreamUrl(pairs) {
  const streams = pairs.map(s => `${s.toLowerCase()}@trade`).join('/');
  return `wss://stream.binance.com:9443/stream?streams=${streams}`;
}

// ── BINANCE WEBSOCKET ────────────────────────────────────────

function connectBinanceStream(pairs) {
  if (binanceWS) {
    binanceWS.close(1000, 'Refreshing pairs');
    binanceWS = null;
  }

  currentStreamUrl = buildStreamUrl(pairs);
  binanceWS = new WebSocket(currentStreamUrl);

  binanceWS.on('open', () => {
    console.log(`✅ Binance stream opened (${pairs.length} pairs)`);
  });

  binanceWS.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.data) {
        const trade = msg.data;
        const pair = trade.s;
        const price = parseFloat(trade.p);
        const amount = parseFloat(trade.q);
        const notional = price * amount;
        const side = trade.m ? 'sell' : 'buy';

        if (notional >= WHALE_THRESHOLD) {
          if (!whaleTrades.has(pair)) whaleTrades.set(pair, []);
          whaleTrades.get(pair).push({
            time: trade.E / 1000,
            side,
            notional,
          });
        }
      }
    } catch (e) {}
  });

  binanceWS.on('close', (code, reason) => {
    console.log(`🔴 Binance disconnected – code: ${code}, reason: ${reason}`);
    binanceWS = null;
    setTimeout(() => {
      if (topPairs.length > 0) {
        console.log('🔄 Reconnecting to Binance…');
        connectBinanceStream(topPairs);
      }
    }, 5000);
  });

  binanceWS.on('error', (err) => {
    console.error('🚨 Binance WS error:', err.message);
  });
}

// ── DYNAMIC PAIR REFRESH ─────────────────────────────────────
async function refreshPairs() {
  const newPairs = await fetchTopPairs(TOP_N);
  if (newPairs.length === 0) {
    console.warn('⚠️ Could not refresh pairs, keeping current list.');
    return;
  }

  if (JSON.stringify(newPairs) !== JSON.stringify(topPairs)) {
    console.log('🔄 Top pairs changed. Updating Binance stream…');
    for (let pair of whaleTrades.keys()) {
      if (!newPairs.includes(pair)) whaleTrades.delete(pair);
    }
    topPairs = newPairs;
    connectBinanceStream(topPairs);
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
    console.error('❌ Failed to get pairs after 10 attempts. Starting with empty list, will retry in background.');
    // Don't exit – the refresh interval will eventually get pairs.
    return;
  }
  topPairs = pairs;
  console.log('🏆 Top pairs:', topPairs.join(', '));
  connectBinanceStream(topPairs);
}

initPairs();
