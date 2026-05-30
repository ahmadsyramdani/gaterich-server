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
let topPairs = [];                    // e.g. ['BTCUSDT', 'ETHUSDT', …]   ← Binance format (no slash)
let pairScores = new Map();
let binanceWS = null;                 // connection to Binance
let currentStreamUrl = '';

// ── HELPERS ──────────────────────────────────────────────────

/** Fetch top USDT pairs by 24h volume from Binance (no API key). */
async function fetchTopPairs(n = TOP_N) {
  try {
    const res = await fetch('https://api.binance.com/api/v3/ticker/24hr');
    const tickers = await res.json();
    // Filter only USDT pairs, sort by quote volume descending
    const usdt = tickers
      .filter(t => t.symbol.endsWith('USDT'))
      .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
      .slice(0, n)
      .map(t => t.symbol);   // e.g. 'BTCUSDT'
    return usdt;
  } catch (e) {
    console.error('❌ fetchTopPairs error:', e.message);
    return [];
  }
}

/** Remove trades older than the rolling window. */
function cleanOldTrades() {
  const cutoff = Date.now() / 1000 - WINDOW_SECONDS;
  for (let [pair, trades] of whaleTrades) {
    whaleTrades.set(pair, trades.filter(t => t.time >= cutoff));
  }
}

/** Compute pressure score (-1 to +1) for each tracked pair. */
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

/** Build the combined Binance stream URL for given pairs. */
function buildStreamUrl(pairs) {
  const streams = pairs.map(s => `${s.toLowerCase()}@trade`).join('/');
  return `wss://stream.binance.com:9443/stream?streams=${streams}`;
}

// ── BINANCE WEBSOCKET ────────────────────────────────────────

function connectBinanceStream(pairs) {
  if (binanceWS) {
    // Close previous connection before opening a new one
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
      // Each message: { stream: 'btcusdt@trade', data: { s, p, q, m, E, ... } }
      if (msg.data) {
        const trade = msg.data;
        const pair = trade.s;            // e.g. 'BTCUSDT'
        const price = parseFloat(trade.p);
        const amount = parseFloat(trade.q);
        const notional = price * amount;
        // Binance: 'm' = buyer is the maker? true → maker, so aggressive side is seller
        const side = trade.m ? 'sell' : 'buy';

        if (notional >= WHALE_THRESHOLD) {
          if (!whaleTrades.has(pair)) whaleTrades.set(pair, []);
          whaleTrades.get(pair).push({
            time: trade.E / 1000,   // event time in seconds
            side,
            notional,
          });
        }
      }
    } catch (e) {
      // ignore malformed JSON
    }
  });

  binanceWS.on('close', (code, reason) => {
    console.log(`🔴 Binance disconnected – code: ${code}, reason: ${reason}`);
    // Attempt to reconnect after 5 seconds (with the same pairs)
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
  if (newPairs.length === 0) return;

  // If the list of pairs changed, reconnect with the new set
  if (JSON.stringify(newPairs) !== JSON.stringify(topPairs)) {
    console.log('🔄 Top pairs changed. Updating Binance stream…');
    // Remove old trades for pairs no longer tracked
    for (let pair of whaleTrades.keys()) {
      if (!newPairs.includes(pair)) whaleTrades.delete(pair);
    }
    topPairs = newPairs;
    connectBinanceStream(topPairs);
  } else {
    topPairs = newPairs;   // keep order up-to-date
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

// Broadcast scores every 2 seconds
setInterval(() => {
  pairScores = computeScores();
  const payload = JSON.stringify({ type: 'scores', data: Array.from(pairScores) });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}, BROADCAST_INTERVAL_MS);

// Periodic pair refresh
setInterval(refreshPairs, REFRESH_INTERVAL_MS);

// ── STARTUP ──────────────────────────────────────────────────
(async () => {
  topPairs = await fetchTopPairs(TOP_N);
  if (topPairs.length === 0) {
    console.error('❌ No pairs fetched. Exiting.');
    process.exit(1);
  }
  console.log('🏆 Top pairs:', topPairs.join(', '));
  connectBinanceStream(topPairs);
})();
