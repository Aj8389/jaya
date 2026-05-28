// ============================================================
// DerivBot Pro — Fully Integrated Backend  (server.js)
// Connects: Browser (Angular) ←→ This Server ←→ Deriv WS API
// ============================================================

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { WebSocketServer } = WebSocket;
const path = require("path");
const fs = require("fs");

const app = express();

// Allow requests from the frontend (Render static site + local dev)
const ALLOWED_ORIGINS = [
  "https://jaya-1-cmi6.onrender.com/",
  "http://localhost:4200",
  "http://localhost:3000",
];
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (!origin || ALLOWED_ORIGINS.includes(origin)) {
    if (origin) res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  }
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.use(express.static(path.join(__dirname, "dist/derivbot-pro/browser")));
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const DERIV_WS = "wss://ws.binaryws.com/websockets/v3?app_id=1089";
const PORT = process.env.PORT || 3000;
const ENV_FILE = path.join(__dirname, ".env");
const CONFIG_FILE = path.join(__dirname, "config.json");

// ─────────────────────────────────────────────
// CONFIG (token persistence)
// ─────────────────────────────────────────────
function parseEnv(text) {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"))
    .reduce((acc, line) => {
      const idx = line.indexOf("=");
      if (idx === -1) return acc;
      const key = line.slice(0, idx).trim();
      let value = line.slice(idx + 1).trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      acc[key] = value;
      return acc;
    }, {});
}

function formatEnv(env) {
  return Object.entries(env)
    .map(([key, value]) => `${key}="${value}"`)
    .join("\n") + "\n";
}

// Deriv API tokens are alphanumeric, typically 15-64 chars, no spaces
function isValidToken(t) {
  return typeof t === 'string' && /^[A-Za-z0-9_-]{10,64}$/.test(t);
}

function loadConfig() {
  // Priority 1: runtime environment variable
  if (process.env.DERIV_TOKEN && isValidToken(process.env.DERIV_TOKEN)) return { token: process.env.DERIV_TOKEN };

  // Priority 2: local .env file
  try {
    if (fs.existsSync(ENV_FILE)) {
      const envData = parseEnv(fs.readFileSync(ENV_FILE, "utf8"));
      if (envData.DERIV_TOKEN && isValidToken(envData.DERIV_TOKEN)) {
        process.env.DERIV_TOKEN = envData.DERIV_TOKEN;
        return { token: envData.DERIV_TOKEN };
      }
    }
  } catch (_) {}

  // Fallback: legacy config.json
  try {
    if (fs.existsSync(CONFIG_FILE)) {
      return JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"));
    }
  } catch (_) {}

  return {};
}

function saveConfig(data) {
  try {
    // Save token to .env
    if (data.token) {
      const envValues = fs.existsSync(ENV_FILE)
        ? parseEnv(fs.readFileSync(ENV_FILE, "utf8"))
        : {};
      envValues.DERIV_TOKEN = data.token;
      process.env.DERIV_TOKEN = data.token;
      fs.writeFileSync(ENV_FILE, formatEnv(envValues));
    }

    // Save bot settings to config.json
    const existing = (() => {
      try { return fs.existsSync(CONFIG_FILE) ? JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8")) : {}; }
      catch (_) { return {}; }
    })();
    const merged = { ...existing, ...data };
    delete merged.token; // keep token only in .env
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(merged, null, 2));
  } catch (_) {}
}

function saveSettings() {
  saveConfig({
    symbol: state.symbol,
    strategy: state.strategy,
    stake: state.baseStake,
    stopLossPct: state.stopLossPct,
    takeProfitPct: state.takeProfitPct,
    maxTradesPerDay: state.maxTradesPerDay,
    dailyLossLimit: state.dailyLossLimit,
    martingaleEnabled: state.martingaleEnabled,
    martingaleMultiplier: state.martingaleMultiplier,
    martingaleMaxSteps: state.martingaleMaxSteps,
    contractType: state.contractType,
    duration: state.duration,
    durationUnit: state.durationUnit,
    pauseOn3Losses: state.pauseOn3Losses,
    botWasRunning: state.botRunning,
  });
}

// ─────────────────────────────────────────────
// GLOBAL STATE
// ─────────────────────────────────────────────
let state = {
  derivSocket: null,
  browserClients: new Set(),

  token: null,
  authorized: false,
  loginid: null,

  balance: 0,
  currency: "USD",

  botRunning: false,

  symbol: "R_100",
  strategy: "SMART",

  stake: 10,
  baseStake: 10,
  stopLossPct: 15,
  takeProfitPct: 30,

  maxTradesPerDay: 10,
  dailyLossLimit: 100,

  martingaleEnabled: false,
  martingaleMultiplier: 2,
  martingaleMaxSteps: 3,
  martStep: 0,

  contractType: "AUTO",
  duration: 1,
  durationUnit: "m",

  pauseOn3Losses: true,
  consecutiveLosses: 0,
  paused: false,
  pauseTimer: null,

  todayWins: 0,
  todayLosses: 0,
  todayPnl: 0,
  todayTradeCount: 0,
  bestStreak: 0,
  currentStreak: 0,

  trades: [],
  activeTrade: null,
  activeContractId: null,

  tickBuffer: [],
  priceHistory: [],

  currentPrice: 0,
  lastPrice: 0,

  reqId: 1,
  lastSignalTime: 0,
  signalInterval: null,

  pendingProposal: null,
  tradeTimeout: null,
  autoStartPending: false,
};

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function nextId() { return ++state.reqId; }

function sendDeriv(data) {
  if (state.derivSocket && state.derivSocket.readyState === WebSocket.OPEN) {
    state.derivSocket.send(JSON.stringify(data));
  }
}

function broadcast(data) {
  const raw = JSON.stringify(data);
  state.browserClients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      try { client.send(raw); } catch (e) {}
    }
  });
}

function log(msg, level = "info") {
  const ts = new Date().toTimeString().slice(0, 8);
  console.log(`[${ts}][${level.toUpperCase()}] ${msg}`);
  broadcast({ type: "LOG", msg, level });
}

function getStats() {
  const total = state.todayWins + state.todayLosses;
  return {
    wins: state.todayWins,
    losses: state.todayLosses,
    total: state.todayTradeCount,
    pnl: parseFloat(state.todayPnl.toFixed(2)),
    winRate: total > 0 ? Math.round((state.todayWins / total) * 100) : 0,
    bestStreak: state.bestStreak,
    remaining: Math.max(0, state.maxTradesPerDay - state.todayTradeCount),
  };
}

// ─────────────────────────────────────────────
// TECHNICAL INDICATORS
// ─────────────────────────────────────────────
function calcEMA(prices, period) {
  if (prices.length < period) return null;
  const k = 2 / (period + 1);
  let ema = prices.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < prices.length; i++) {
    ema = prices[i] * k + ema * (1 - k);
  }
  return ema;
}

function calcRSI(prices, period = 14) {
  if (prices.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = prices.length - period; i < prices.length; i++) {
    const diff = prices[i] - prices[i - 1];
    if (diff > 0) gains += diff;
    else losses += Math.abs(diff);
  }
  const avgGain = gains / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return parseFloat((100 - 100 / (1 + rs)).toFixed(2));
}

function calcMACD(prices) {
  const ema12 = calcEMA(prices, 12);
  const ema26 = calcEMA(prices, 26);
  if (!ema12 || !ema26) return 0;
  return parseFloat((ema12 - ema26).toFixed(6));
}

function calcBollinger(prices, period = 20) {
  if (prices.length < period) return null;
  const slice = prices.slice(-period);
  const mean = slice.reduce((a, b) => a + b, 0) / period;
  const variance = slice.reduce((sum, p) => sum + Math.pow(p - mean, 2), 0) / period;
  const std = Math.sqrt(variance);
  return { upper: mean + 2 * std, middle: mean, lower: mean - 2 * std };
}

function analyzeSignal() {
  const prices = state.priceHistory;
  if (prices.length < 20) {
    return { signal: "WAIT", reason: "Collecting data...", rsi: 50, ema9: 0, ema21: 0, strength: 0, macd: 0 };
  }

  const rsi = calcRSI(prices, 14);
  const ema9 = calcEMA(prices, 9) || prices[prices.length - 1];
  const ema21 = calcEMA(prices, 21) || prices[prices.length - 1];
  const macd = calcMACD(prices);
  const boll = calcBollinger(prices, 20);
  const price = state.currentPrice;

  let signal = "WAIT";
  let reason = "Scanning...";
  let strength = 0;

  if (state.strategy === "SMART") {
    // Score 4 independent signals; fire when majority agree
    const recent = prices.slice(-6);
    const upTicks = recent.filter((p, i) => i > 0 && p > recent[i - 1]).length;
    const downTicks = (recent.length - 1) - upTicks;

    const emaBull  = ema9 > ema21;
    const rsiBull  = rsi < 50;
    const momBull  = upTicks >= 3;
    const macdBull = macd >= 0;

    const emaBear  = !emaBull;
    const rsiBear  = !rsiBull;
    const momBear  = downTicks >= 3;
    const macdBear = macd < 0;

    const bullScore = (emaBull ? 1 : 0) + (rsiBull ? 1 : 0) + (momBull ? 1 : 0) + (macdBull ? 1 : 0);
    const bearScore = (emaBear ? 1 : 0) + (rsiBear ? 1 : 0) + (momBear ? 1 : 0) + (macdBear ? 1 : 0);

    if (bullScore >= 3 && bullScore > bearScore) {
      signal = "BUY";
      reason = `SMART BUY ${bullScore}/4: EMA${emaBull?"↑":"↓"} RSI${rsi} Mom${upTicks}/5 MACD${macdBull?"↑":"↓"}`;
      strength = 50 + bullScore * 12;
    } else if (bearScore >= 3 && bearScore > bullScore) {
      signal = "SELL";
      reason = `SMART SELL ${bearScore}/4: EMA${emaBear?"↓":"↑"} RSI${rsi} Mom${downTicks}/5 MACD${macdBear?"↓":"↑"}`;
      strength = 50 + bearScore * 12;
    } else if (bullScore === 2 && bullScore > bearScore) {
      signal = "BUY";
      reason = `SMART BUY 2/4: RSI${rsi} EMA${emaBull?"Bull":"Bear"} Mom${upTicks}/5`;
      strength = 52;
    } else if (bearScore === 2 && bearScore > bullScore) {
      signal = "SELL";
      reason = `SMART SELL 2/4: RSI${rsi} EMA${emaBear?"Bear":"Bull"} Mom${downTicks}/5`;
      strength = 52;
    } else {
      reason = `SMART: Signals tied (Bull:${bullScore} Bear:${bearScore} RSI:${rsi})`;
      strength = 20;
    }
  } else if (state.strategy === "RSI_EMA") {
    if (rsi < 40 && ema9 > ema21) {
      signal = "BUY";
      reason = `RSI low (${rsi}) + bullish EMA`;
      strength = Math.round(60 + (40 - rsi) * 1.5);
    } else if (rsi > 60 && ema9 < ema21) {
      signal = "SELL";
      reason = `RSI high (${rsi}) + bearish EMA`;
      strength = Math.round(60 + (rsi - 60) * 1.5);
    } else if (rsi < 45 && ema9 > ema21) {
      signal = "BUY";
      reason = `RSI leaning low (${rsi}) + bullish EMA`;
      strength = 52;
    } else if (rsi > 55 && ema9 < ema21) {
      signal = "SELL";
      reason = `RSI leaning high (${rsi}) + bearish EMA`;
      strength = 52;
    } else {
      reason = `RSI: ${rsi} | EMA: ${ema9 > ema21 ? "Bull" : "Bear"}`;
      strength = 20;
    }
  } else if (state.strategy === "BOLLINGER" && boll) {
    if (price < boll.lower) {
      signal = "BUY";
      reason = `Price below lower band (${boll.lower.toFixed(2)})`;
      strength = Math.round(65 + Math.min(30, ((boll.lower - price) / boll.lower) * 1000));
    } else if (price > boll.upper) {
      signal = "SELL";
      reason = `Price above upper band (${boll.upper.toFixed(2)})`;
      strength = Math.round(65 + Math.min(30, ((price - boll.upper) / boll.upper) * 1000));
    } else if (price <= boll.middle) {
      signal = "BUY";
      reason = `Price below mid-band — bounce expected`;
      strength = 53;
    } else {
      signal = "SELL";
      reason = `Price above mid-band — pullback expected`;
      strength = 53;
    }
  } else if (state.strategy === "MACD") {
    const prevMacd = calcMACD(prices.slice(0, -1));
    if (macd > 0 && prevMacd <= 0) {
      signal = "BUY";
      reason = `MACD bullish crossover (${macd.toFixed(4)})`;
      strength = Math.round(70 + Math.min(25, Math.abs(macd) * 5000));
    } else if (macd < 0 && prevMacd >= 0) {
      signal = "SELL";
      reason = `MACD bearish crossover (${macd.toFixed(4)})`;
      strength = Math.round(70 + Math.min(25, Math.abs(macd) * 5000));
    } else if (macd > 0) {
      signal = "BUY";
      reason = `MACD bullish trend (${macd.toFixed(4)})`;
      strength = 52;
    } else if (macd < 0) {
      signal = "SELL";
      reason = `MACD bearish trend (${macd.toFixed(4)})`;
      strength = 52;
    } else {
      reason = `MACD flat`;
      strength = 20;
    }
  } else if (state.strategy === "SCALPER") {
    if (prices.length >= 5) {
      const recent = prices.slice(-5);
      const up = recent.filter((p, i) => i > 0 && p > recent[i - 1]).length;
      if (up >= 3) {
        signal = "SELL";
        reason = `Scalper: ${up}/4 up ticks → reversal`;
        strength = 50 + up * 10;
      } else if (up <= 1) {
        signal = "BUY";
        reason = `Scalper: ${4 - up}/4 down ticks → reversal`;
        strength = 50 + (4 - up) * 10;
      } else {
        reason = `Scalper: mixed ticks (${up}/4 up)`;
        strength = 20;
      }
    }
  }

  strength = Math.min(100, Math.max(0, strength));
  return { signal, reason, rsi, ema9: parseFloat(ema9.toFixed(4)), ema21: parseFloat(ema21.toFixed(4)), strength, macd };
}

// ─────────────────────────────────────────────
// CONNECT TO DERIV
// ─────────────────────────────────────────────
function connectDeriv(token) {
  if (!isValidToken(token)) {
    log("Invalid or missing API token — enter your token in the UI", "err");
    broadcast({ type: "CONN_STATUS", status: "error", label: "NO TOKEN — enter API token" });
    broadcast({ type: "DERIV_ERROR", message: "Invalid API token. Please enter a valid Deriv API token." });
    return;
  }

  state.token = token;

  if (state.derivSocket) {
    try { state.derivSocket.close(); } catch (e) {}
  }

  log("Connecting to Deriv WebSocket...", "info");
  broadcast({ type: "CONN_STATUS", status: "connecting", label: "CONNECTING..." });

  const ws = new WebSocket(DERIV_WS);
  state.derivSocket = ws;

  ws.on("open", () => {
    log("Deriv WS connected. Authorizing...", "ok");
    sendDeriv({ authorize: token, req_id: nextId() });
  });

  ws.on("message", (raw) => {
    let data;
    try { data = JSON.parse(raw.toString()); } catch (e) { return; }
    handleDerivMessage(data);
  });

  ws.on("error", (err) => {
    log("Deriv WS Error: " + err.message, "err");
    broadcast({ type: "CONN_STATUS", status: "error", label: "WS ERROR" });
  });

  ws.on("close", () => {
    log("Deriv WS closed — reconnecting in 5s...", "warn");
    state.authorized = false;
    broadcast({ type: "CONN_STATUS", status: "error", label: "RECONNECTING..." });

    // Auto-reconnect using saved token
    setTimeout(() => {
      const savedToken = state.token || (loadConfig().token);
      if (savedToken) {
        log("Auto-reconnecting to Deriv...", "info");
        connectDeriv(savedToken);
      }
    }, 5000);
  });
}

// ─────────────────────────────────────────────
// HANDLE DERIV MESSAGES
// ─────────────────────────────────────────────
function handleDerivMessage(data) {
  if (data.error) {
    const message = data.error.message || "Unknown Deriv error.";
    log(`Deriv Error: ${message}`, "err");
    broadcast({ type: "LOG", level: "err", msg: message });

    let connLabel = "Deriv auth failed — see log";
    if (message.includes("Account is disabled")) {
      connLabel = "Account disabled — verify Deriv account";
    } else if (message.includes("Parameters sanity check failed")) {
      connLabel = "Invalid API token — verify token format";
    }

    broadcast({ type: "CONN_STATUS", status: "error", label: connLabel });
    broadcast({ type: "DERIV_ERROR", message });
    state.authorized = false;
    return;
  }

  // PROPOSAL RESPONSE — step 2: buy the quoted contract
  if (data.msg_type === "proposal") {
    if (!state.pendingProposal) return;

    if (data.error) {
      log(`Proposal failed: ${data.error.message}`, "err");
      state.activeTrade = null;
      state.pendingProposal = null;
      return;
    }

    const proposal = data.proposal;
    log(`Quote received — buying at $${proposal.ask_price}`, "info");

    sendDeriv({
      buy: proposal.id,
      price: proposal.ask_price,
      req_id: nextId(),
    });

    state.pendingProposal = null;
    return;
  }

  // AUTHORIZE
  if (data.msg_type === "authorize") {
    const auth = data.authorize;
    state.authorized = true;
    state.loginid = auth.loginid;
    state.balance = parseFloat(auth.balance);
    state.currency = auth.currency;

    log(`Authorized: ${auth.loginid} | Balance: ${auth.balance} ${auth.currency}`, "ok");
    broadcast({
      type: "AUTHORIZED",
      loginid: auth.loginid,
      balance: auth.balance,
      currency: auth.currency,
    });

    // Subscribe to balance & ticks
    sendDeriv({ balance: 1, subscribe: 1, req_id: nextId() });
    subscribeTicks(state.symbol);

    // Resume bot if it was running before the reconnect/restart
    if (state.autoStartPending || state.botRunning) {
      state.autoStartPending = false;
      const wasRunning = state.botRunning;
      state.botRunning = false; // reset so startBot re-initialises cleanly
      setTimeout(() => {
        log("Auto-resuming bot after reconnect", "ok");
        startBot();
      }, 3000);
    }
    return;
  }

  // BALANCE
  if (data.msg_type === "balance") {
    state.balance = parseFloat(data.balance.balance);
    broadcast({ type: "BALANCE_UPDATE", balance: state.balance, currency: state.currency });
    return;
  }

  // TICK
  if (data.msg_type === "tick") {
    const price = parseFloat(data.tick.quote);
    state.lastPrice = state.currentPrice || price;
    state.currentPrice = price;

    state.tickBuffer.push(price);
    if (state.tickBuffer.length > 300) state.tickBuffer.shift();

    state.priceHistory.push(price);
    if (state.priceHistory.length > 100) state.priceHistory.shift();

    broadcast({ type: "TICK", price, symbol: data.tick.symbol });

    // Run signal + bot logic on each tick
    if (state.botRunning && !state.paused) {
      runBotLogic();
    }
    return;
  }

  // TICKS HISTORY
  if (data.msg_type === "ticks_history" && data.history) {
    const prices = data.history.prices.map(parseFloat);
    state.tickBuffer = prices;
    state.priceHistory = prices.slice(-100);
    broadcast({ type: "PRICE_HISTORY", prices: state.priceHistory });
    return;
  }

  // BUY CONTRACT RESPONSE
  if (data.msg_type === "buy") {
    if (data.error) {
      log(`Buy failed: ${data.error.message}`, "err");
      state.activeTrade = null;
      state.activeContractId = null;
      state.pendingProposal = null;
      return;
    }

    const contract = data.buy;
    state.activeContractId = contract.contract_id;

    // Use the actual buy price from Deriv as entry price
    const trade = {
      openTime: Date.now(),
      direction: state.activeTrade?.direction || "BUY",
      stake: state.stake,
      entryPrice: parseFloat(contract.buy_price) || state.currentPrice,
      status: "open",
    };
    state.activeTrade = trade;
    state.todayTradeCount++;

    log(`Trade opened: ${trade.direction} | Stake: $${trade.stake} | Entry: ${trade.entryPrice}`, "ok");
    broadcast({ type: "TRADE_OPENED", trade });

    // Subscribe to live contract updates
    sendDeriv({ proposal_open_contract: 1, contract_id: state.activeContractId, subscribe: 1, req_id: nextId() });

    // Fallback: if settlement message never arrives, re-query after duration + buffer
    const durationMs = state.durationUnit === "t" ? state.duration * 2000 + 5000
                     : state.durationUnit === "m" ? state.duration * 60000 + 15000
                     : state.durationUnit === "h" ? state.duration * 3600000 + 30000
                     : state.duration * 1000 + 10000;
    clearTimeout(state.tradeTimeout);
    state.tradeTimeout = setTimeout(() => {
      if (state.activeTrade && state.activeContractId) {
        log("Trade timeout — re-querying contract status", "warn");
        sendDeriv({ proposal_open_contract: 1, contract_id: state.activeContractId, req_id: nextId() });
      }
    }, durationMs);

    return;
  }

  // CONTRACT UPDATE (proposal_open_contract)
  if (data.msg_type === "proposal_open_contract") {
    const poc = data.proposal_open_contract;
    if (!poc) return;

    // Ignore stale updates from previous contracts
    if (poc.contract_id !== state.activeContractId) return;

    broadcast({ type: "CONTRACT_UPDATE", contract: poc });

    const settled = poc.is_expired === 1 || poc.is_sold === 1
                 || poc.status === "won" || poc.status === "lost";

    if (settled && state.activeTrade) {
      clearTimeout(state.tradeTimeout);
      state.tradeTimeout = null;
      handleContractClose(poc);
    }
    return;
  }

  // SELL RESPONSE
  if (data.msg_type === "sell") {
    log("Contract sold", "ok");
    return;
  }
}

// ─────────────────────────────────────────────
// HANDLE CONTRACT CLOSE
// ─────────────────────────────────────────────
function handleContractClose(poc) {
  if (!state.activeTrade) return;

  clearTimeout(state.tradeTimeout);
  state.tradeTimeout = null;

  // Stop receiving further updates for this contract
  sendDeriv({ forget_all: "proposal_open_contract", req_id: nextId() });

  const isWin = poc.status === "won" || (poc.profit && parseFloat(poc.profit) > 0);
  const profit = poc.profit ? parseFloat(poc.profit) : (isWin ? state.stake * 0.85 : -state.stake);

  const trade = {
    ...state.activeTrade,
    exitPrice: poc.sell_price || state.currentPrice,
    profit: parseFloat(profit.toFixed(2)),
    status: isWin ? "win" : "loss",
  };

  state.trades.unshift(trade);
  if (state.trades.length > 200) state.trades.pop();

  // Update stats
  state.todayPnl += profit;
  if (isWin) {
    state.todayWins++;
    state.consecutiveLosses = 0;
    state.currentStreak++;
    if (state.currentStreak > state.bestStreak) state.bestStreak = state.currentStreak;
    // Reset martingale on win
    state.martStep = 0;
    state.stake = state.baseStake;
  } else {
    state.todayLosses++;
    state.currentStreak = 0;
    state.consecutiveLosses++;

    // Martingale
    if (state.martingaleEnabled && state.martStep < state.martingaleMaxSteps) {
      state.martStep++;
      state.stake = parseFloat((state.baseStake * Math.pow(state.martingaleMultiplier, state.martStep)).toFixed(2));
      log(`Martingale step ${state.martStep}: Stake → $${state.stake}`, "warn");
    }

    // Pause on 3 consecutive losses
    if (state.pauseOn3Losses && state.consecutiveLosses >= 3) {
      state.paused = true;
      log("3 consecutive losses — pausing bot 60s", "warn");
      broadcast({ type: "LOG", level: "warn", msg: "Paused 60s after 3 losses" });
      clearTimeout(state.pauseTimer);
      state.pauseTimer = setTimeout(() => {
        state.paused = false;
        state.consecutiveLosses = 0;
        log("Bot resumed after cooldown", "ok");
      }, 60000);
    }
  }

  const stats = getStats();
  state.activeTrade = null;
  state.activeContractId = null;

  log(`Trade closed: ${trade.status.toUpperCase()} | P&L: ${profit >= 0 ? "+" : ""}$${profit.toFixed(2)}`, isWin ? "ok" : "err");
  broadcast({ type: "TRADE_CLOSED", trade, stats });

  // Check daily limits
  if (state.todayTradeCount >= state.maxTradesPerDay) {
    log("Max daily trades reached — stopping bot", "warn");
    stopBot();
  }
  if (state.todayPnl < 0 && Math.abs(state.todayPnl) >= state.dailyLossLimit) {
    log(`Daily loss limit hit (${state.todayPnl.toFixed(2)}) — stopping bot`, "err");
    stopBot();
  }
  const takeProfitTarget = parseFloat((state.balance * state.takeProfitPct / 100).toFixed(2));
  if (state.todayPnl > 0 && state.todayPnl >= takeProfitTarget) {
    log(`Take profit reached +${state.todayPnl.toFixed(2)} (target: +${takeProfitTarget}) — stopping bot`, "ok");
    stopBot();
  }
}

// ─────────────────────────────────────────────
// SUBSCRIBE TICKS
// ─────────────────────────────────────────────
function subscribeTicks(symbol) {
  sendDeriv({ ticks_history: symbol, count: 200, end: "latest", style: "ticks", req_id: nextId() });
  sendDeriv({ ticks: symbol, subscribe: 1, req_id: nextId() });
  log(`Subscribed to ticks: ${symbol}`, "ok");
}

// ─────────────────────────────────────────────
// BOT LOGIC — runs on each tick
// ─────────────────────────────────────────────
function runBotLogic() {
  if (!state.authorized || !state.botRunning || state.paused) return;
  if (state.activeTrade) return; // already in a trade
  if (state.todayTradeCount >= state.maxTradesPerDay) return;
  if (state.todayPnl < -Math.abs(state.dailyLossLimit)) return;

  // Rate limit: don't spam signals
  const now = Date.now();
  if (now - state.lastSignalTime < 3000) return;

  const analysis = analyzeSignal();
  broadcast({ type: "SIGNAL_UPDATE", analysis });
  state.lastSignalTime = now;

  if (analysis.signal === "WAIT" || analysis.strength < 50) return;

  // Determine contract type
  let contractType = state.contractType;
  if (contractType === "AUTO") {
    contractType = analysis.signal === "BUY" ? "CALL" : "PUT";
  }

  placeTrade(contractType, analysis.signal);
}

// ─────────────────────────────────────────────
// PLACE TRADE — step 1: request proposal
// ─────────────────────────────────────────────
function placeTrade(contractType, direction) {
  if (!state.authorized) {
    log("Not authorized — cannot place trade", "err");
    return;
  }

  // Mark trade pending to block duplicate signals
  state.activeTrade = {
    openTime: Date.now(),
    direction,
    stake: state.stake,
    entryPrice: state.currentPrice,
    status: "open",
  };
  state.pendingProposal = { contractType, direction };

  // Cancel any lingering subscriptions from the previous trade
  sendDeriv({ forget_all: "proposal_open_contract", req_id: nextId() });

  log(`Requesting quote: ${direction} | ${contractType} | $${state.stake}`, "info");

  sendDeriv({
    proposal: 1,
    contract_type: contractType,
    symbol: state.symbol,
    duration: state.duration,
    duration_unit: state.durationUnit,
    basis: "stake",
    amount: state.stake,
    currency: state.currency,
    req_id: nextId(),
  });
}

// ─────────────────────────────────────────────
// APPLY SETTINGS
// ─────────────────────────────────────────────
function applySettings(s) {
  if (s.symbol !== undefined) state.symbol = s.symbol;
  if (s.strategy !== undefined) state.strategy = s.strategy;
  if (s.stake !== undefined) { state.stake = parseFloat(s.stake); state.baseStake = state.stake; }
  if (s.stopLossPct !== undefined) state.stopLossPct = parseFloat(s.stopLossPct);
  if (s.takeProfitPct !== undefined) state.takeProfitPct = parseFloat(s.takeProfitPct);
  if (s.maxTradesPerDay !== undefined) state.maxTradesPerDay = parseInt(s.maxTradesPerDay);
  if (s.dailyLossLimit !== undefined) state.dailyLossLimit = parseFloat(s.dailyLossLimit);
  if (s.martingaleEnabled !== undefined) state.martingaleEnabled = s.martingaleEnabled;
  if (s.martingaleMultiplier !== undefined) state.martingaleMultiplier = parseFloat(s.martingaleMultiplier);
  if (s.martingaleMaxSteps !== undefined) state.martingaleMaxSteps = parseInt(s.martingaleMaxSteps);
  if (s.contractType !== undefined) state.contractType = s.contractType;
  if (s.duration !== undefined) state.duration = parseInt(s.duration);
  if (s.durationUnit !== undefined) state.durationUnit = s.durationUnit;
  if (s.pauseOn3Losses !== undefined) state.pauseOn3Losses = s.pauseOn3Losses;
  saveSettings();
}

// ─────────────────────────────────────────────
// BOT CONTROL
// ─────────────────────────────────────────────
function startBot() {
  state.botRunning = true;
  state.martStep = 0;
  state.stake = state.baseStake;
  state.paused = false;
  saveSettings();
  broadcast({ type: "BOT_STATUS", running: true });
  log("Bot started", "ok");
}

function stopBot() {
  state.botRunning = false;
  clearTimeout(state.pauseTimer);
  saveSettings();
  broadcast({ type: "BOT_STATUS", running: false });
  log("Bot stopped", "warn");
}

function emergencyStop() {
  const contractId = state.activeContractId; // save before clearing
  stopBot();
  state.activeTrade = null;
  state.activeContractId = null;
  state.pendingProposal = null;
  clearTimeout(state.tradeTimeout);
  state.tradeTimeout = null;

  if (contractId) {
    sendDeriv({ sell: contractId, price: 0, req_id: nextId() });
  }

  broadcast({ type: "EMERGENCY_STOP" });
  log("EMERGENCY STOP", "err");
}

// ─────────────────────────────────────────────
// BROWSER WEBSOCKET HANDLER
// ─────────────────────────────────────────────
wss.on("connection", (browserWs) => {
  state.browserClients.add(browserWs);
  console.log(`[+] Browser connected (${state.browserClients.size} total)`);

  // Send full state to new client
  browserWs.send(JSON.stringify({
    type: "FULL_STATE",
    state: {
      authorized: state.authorized,
      loginid: state.loginid,
      balance: state.balance,
      currency: state.currency,
      botRunning: state.botRunning,
      symbol: state.symbol,
      trades: state.trades.slice(0, 50),
      stats: getStats(),
      priceHistory: state.priceHistory,
      currentPrice: state.currentPrice,
      settings: {
        symbol: state.symbol,
        strategy: state.strategy,
        stake: state.baseStake,
        stopLossPct: state.stopLossPct,
        takeProfitPct: state.takeProfitPct,
        maxTradesPerDay: state.maxTradesPerDay,
        dailyLossLimit: state.dailyLossLimit,
        martingaleEnabled: state.martingaleEnabled,
        martingaleMultiplier: state.martingaleMultiplier,
        martingaleMaxSteps: state.martingaleMaxSteps,
        contractType: state.contractType,
        duration: state.duration,
        durationUnit: state.durationUnit,
        pauseOn3Losses: state.pauseOn3Losses,
      },
    },
  }));

  browserWs.on("message", (raw) => {
    let cmd;
    try { cmd = JSON.parse(raw.toString()); } catch (e) { return; }

    switch (cmd.type) {
      case "CONNECT":
        // Persist token so server can auto-connect on restart
        saveConfig({ token: cmd.token });
        // If already authorized with the same token and socket is open, just sync this client
        if (
          state.authorized &&
          state.token === cmd.token &&
          state.derivSocket?.readyState === WebSocket.OPEN
        ) {
          browserWs.send(JSON.stringify({
            type: "AUTHORIZED",
            loginid: state.loginid,
            balance: state.balance,
            currency: state.currency,
          }));
          if (state.botRunning) browserWs.send(JSON.stringify({ type: "BOT_STATUS", running: true }));
        } else {
          connectDeriv(cmd.token);
        }
        break;

      case "START_BOT":
        if (cmd.settings) applySettings(cmd.settings);
        startBot();
        break;

      case "STOP_BOT":
        stopBot();
        break;

      case "EMERGENCY_STOP":
        emergencyStop();
        break;

      case "CHANGE_SYMBOL":
        state.symbol = cmd.symbol;
        if (state.authorized) subscribeTicks(cmd.symbol);
        break;

      case "MANUAL_TRADE":
        if (!state.authorized) { log("Not connected — cannot trade", "err"); break; }
        if (state.activeTrade) { log("Trade already open", "warn"); break; }
        const dir = cmd.direction === "BUY" ? "CALL" : "PUT";
        placeTrade(dir, cmd.direction);
        break;

      case "UPDATE_SETTINGS":
        if (cmd.settings) applySettings(cmd.settings);
        break;

      default:
        console.log("Unknown command:", cmd.type);
    }
  });

  browserWs.on("close", () => {
    state.browserClients.delete(browserWs);
    console.log(`[-] Browser disconnected (${state.browserClients.size} total)`);
  });

  browserWs.on("error", () => {
    state.browserClients.delete(browserWs);
  });
});

// ─────────────────────────────────────────────
// REST API ENDPOINTS
// ─────────────────────────────────────────────
// Save token and connect (called from frontend or external tools)
app.post("/api/token", (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: "token required" });
  saveConfig({ token });
  if (!state.authorized || state.token !== token) connectDeriv(token);
  res.json({ ok: true });
});

// Check if a token is stored server-side
app.get("/api/token", (req, res) => {
  const cfg = loadConfig();
  res.json({ hasToken: !!cfg.token, loginid: state.loginid || null, authorized: state.authorized });
});

app.get("/api/status", (req, res) => {
  res.json({
    authorized: state.authorized,
    loginid: state.loginid,
    balance: state.balance,
    currency: state.currency,
    botRunning: state.botRunning,
    paused: state.paused,
    symbol: state.symbol,
    strategy: state.strategy,
    stake: state.stake,
    stats: getStats(),
  });
});

app.get("/api/trades", (req, res) => {
  res.json({ trades: state.trades.slice(0, 100) });
});

app.get("/api/signal", (req, res) => {
  res.json(analyzeSignal());
});

// Serve Angular app for all non-API routes
app.get("*", (req, res) => {
  const index = path.join(__dirname, "dist/derivbot-pro/browser/index.html");
  res.sendFile(index, (err) => {
    if (err) {
      // Angular not built yet — serve dev message
      res.send(`
        <html><body style="background:#07090f;color:#e8edf7;font-family:monospace;padding:40px;">
        <h2 style="color:#00e5b0">DerivBot Pro — Backend Running ✓</h2>
        <p style="color:#6b7899;margin-top:12px;">Backend WebSocket is live on port ${PORT}.</p>
        <p style="margin-top:10px;">To serve the Angular frontend, run:</p>
        <pre style="background:#161b26;padding:16px;border-radius:8px;margin-top:10px;color:#4f8ef7">
  cd frontend && npm install && npm run build
  # Then restart: node server.js
        </pre>
        <p style="margin-top:16px;">Or run Angular dev server separately:</p>
        <pre style="background:#161b26;padding:16px;border-radius:8px;margin-top:10px;color:#4f8ef7">
  cd frontend && npm install && npm start
  # Angular runs on http://localhost:4200
  # Backend WebSocket runs on ws://localhost:${PORT}
        </pre>
        <p style="margin-top:16px;color:#6b7899">API: <a href="/api/status" style="color:#00e5b0">/api/status</a> | <a href="/api/trades" style="color:#00e5b0">/api/trades</a> | <a href="/api/signal" style="color:#00e5b0">/api/signal</a></p>
        </body></html>
      `);
    }
  });
});

// ─────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────
server.listen(PORT, () => {
  console.log("\n╔══════════════════════════════════════╗");
  console.log("║     DerivBot Pro — Backend Server    ║");
  console.log("╠══════════════════════════════════════╣");
  console.log(`║  Dashboard  : http://localhost:${PORT}    ║`);
  console.log(`║  WebSocket  : ws://localhost:${PORT}      ║`);
  console.log(`║  API Status : /api/status            ║`);
  console.log(`║  API Trades : /api/trades            ║`);
  console.log(`║  API Signal : /api/signal            ║`);
  console.log("╚══════════════════════════════════════╝\n");

  // Auto-connect if token is saved; restore settings and bot state
  const cfg = loadConfig();
  if (cfg.token) {
    const settingsKeys = ["symbol","strategy","stopLossPct","takeProfitPct",
      "maxTradesPerDay","dailyLossLimit","martingaleEnabled","martingaleMultiplier",
      "martingaleMaxSteps","contractType","duration","durationUnit","pauseOn3Losses"];
    settingsKeys.forEach(k => { if (cfg[k] !== undefined) state[k] = cfg[k]; });
    if (cfg.stake !== undefined) { state.stake = cfg.stake; state.baseStake = cfg.stake; }
    if (cfg.botWasRunning === true) state.autoStartPending = true;

    console.log(`[INFO] Saved token found — auto-connecting (bot: ${cfg.botWasRunning ? "will auto-start" : "stopped"})`);
    connectDeriv(cfg.token);
  }

  // Self-ping every 10 min to prevent Render free tier from sleeping
  if (process.env.NODE_ENV === "production") {
    const selfUrl = `http://localhost:${PORT}/api/status`;
    setInterval(() => {
      http.get(selfUrl, (res) => {
        log(`Self-ping OK (${res.statusCode})`, "sys");
      }).on("error", (e) => {
        log(`Self-ping failed: ${e.message}`, "warn");
      });
    }, 10 * 60 * 1000);
    log("Self-ping enabled — server will stay alive on Render", "sys");
  }
});
