// ============================================================
// DerivBot Pro — UPDATED DERIV WS API (2026)
// Uses Official Deriv WebSocket API Flow
// Browser (Angular) ←→ Node Server ←→ Deriv WS API
// ============================================================

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { WebSocketServer } = WebSocket;
const path = require("path");
const fs = require("fs");

// ============================================================
// APP SETUP
// ============================================================

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

const DERIV_APP_ID =
  process.env.DERIV_APP_ID || "33lNATvPa34R1SQKLe1o9";

const DERIV_WS_URL = `wss://ws.derivws.com/websockets/v3?app_id=${DERIV_APP_ID}`;

const ENV_FILE = path.join(__dirname, ".env");
const CONFIG_FILE = path.join(__dirname, "config.json");

// ============================================================
// CORS
// ============================================================

const ALLOWED_ORIGINS = [
  "http://localhost:4200",
  "http://localhost:3000",
  "https://jaya-gy5o.onrender.com",
];

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (!origin || ALLOWED_ORIGINS.includes(origin)) {
    if (origin) {
      res.setHeader("Access-Control-Allow-Origin", origin);
    }

    res.setHeader(
      "Access-Control-Allow-Methods",
      "GET,POST,OPTIONS"
    );

    res.setHeader(
      "Access-Control-Allow-Headers",
      "Content-Type"
    );
  }

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});

app.use(express.json());

app.use(
  express.static(
    path.join(__dirname, "dist/derivbot-pro/browser")
  )
);

// ============================================================
// GLOBAL STATE
// ============================================================

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
};

// ============================================================
// HELPERS
// ============================================================

function nextId() {
  return ++state.reqId;
}

function log(msg, level = "info") {
  const ts = new Date().toTimeString().slice(0, 8);

  console.log(`[${ts}] ${msg}`);

  broadcast({
    type: "LOG",
    msg,
    level,
  });
}

function broadcast(data) {
  const raw = JSON.stringify(data);

  state.browserClients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      try {
        client.send(raw);
      } catch (e) {}
    }
  });
}

function sendDeriv(data) {
  if (
    state.derivSocket &&
    state.derivSocket.readyState === WebSocket.OPEN
  ) {
    state.derivSocket.send(JSON.stringify(data));
  }
}

function isValidToken(token) {
  return (
    typeof token === "string" &&
    token.length > 10
  );
}

// ============================================================
// CONFIG
// ============================================================

function loadConfig() {
  try {
    if (fs.existsSync(CONFIG_FILE)) {
      return JSON.parse(
        fs.readFileSync(CONFIG_FILE, "utf8")
      );
    }
  } catch (e) {}

  return {};
}

function saveConfig(data) {
  try {
    const existing = loadConfig();

    const merged = {
      ...existing,
      ...data,
    };

    fs.writeFileSync(
      CONFIG_FILE,
      JSON.stringify(merged, null, 2)
    );
  } catch (e) {}
}

// ============================================================
// CONNECT DERIV
// ============================================================

function connectDeriv(token) {
  if (!isValidToken(token)) {
    log("Invalid Deriv token", "err");

    broadcast({
      type: "CONN_STATUS",
      status: "error",
      label: "INVALID TOKEN",
    });

    return;
  }

  state.token = token;

  if (state.derivSocket) {
    try {
      state.derivSocket.close();
    } catch (e) {}
  }

  log("Connecting to Deriv...", "info");

  broadcast({
    type: "CONN_STATUS",
    status: "connecting",
    label: "CONNECTING",
  });

  const ws = new WebSocket(DERIV_WS_URL);

  state.derivSocket = ws;

  ws.on("open", () => {
    log("Connected to Deriv WS", "ok");

    sendDeriv({
      authorize: token,
      req_id: nextId(),
    });
  });

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    handleDerivMessage(data);
  });

  ws.on("close", () => {
    state.authorized = false;

    log("Deriv disconnected", "warn");

    broadcast({
      type: "CONN_STATUS",
      status: "error",
      label: "RECONNECTING",
    });

    setTimeout(() => {
      if (state.token) {
        connectDeriv(state.token);
      }
    }, 5000);
  });

  ws.on("error", (err) => {
    log(`WS Error: ${err.message}`, "err");
  });
}

// ============================================================
// HANDLE DERIV MESSAGE
// ============================================================

function handleDerivMessage(data) {
  if (data.error) {
    log(
      `Deriv Error: ${data.error.message}`,
      "err"
    );

    broadcast({
      type: "DERIV_ERROR",
      message: data.error.message,
    });

    return;
  }

  // =========================================================
  // AUTHORIZE
  // =========================================================

  if (data.msg_type === "authorize") {
    const auth = data.authorize;

    state.authorized = true;

    state.loginid = auth.loginid;

    state.balance = parseFloat(auth.balance);

    state.currency = auth.currency;

    log(
      `Authorized: ${state.loginid}`,
      "ok"
    );

    broadcast({
      type: "AUTHORIZED",
      loginid: state.loginid,
      balance: state.balance,
      currency: state.currency,
    });

    broadcast({
      type: "CONN_STATUS",
      status: "live",
      label: "LIVE",
    });

    // Subscribe balance

    sendDeriv({
      balance: 1,
      subscribe: 1,
    });

    // Subscribe ticks

    subscribeTicks(state.symbol);

    return;
  }

  // =========================================================
  // BALANCE
  // =========================================================

  if (data.msg_type === "balance") {
    const bal = data.balance;

    state.balance = parseFloat(
      bal.balance
    );

    state.currency = bal.currency;

    broadcast({
      type: "BALANCE_UPDATE",
      balance: state.balance,
      currency: state.currency,
    });

    return;
  }

  // =========================================================
  // TICKS
  // =========================================================

  if (data.msg_type === "tick") {
    const price = parseFloat(
      data.tick.quote
    );

    state.lastPrice =
      state.currentPrice || price;

    state.currentPrice = price;

    state.priceHistory.push(price);

    if (state.priceHistory.length > 200) {
      state.priceHistory.shift();
    }

    broadcast({
      type: "TICK",
      price,
      symbol: state.symbol,
    });

    if (
      state.botRunning &&
      !state.paused
    ) {
      runBotLogic();
    }

    return;
  }

  // =========================================================
  // PROPOSAL
  // =========================================================

  if (data.msg_type === "proposal") {
    const proposal = data.proposal;

    sendDeriv({
      buy: proposal.id,
      price: proposal.ask_price,
    });

    return;
  }

  // =========================================================
  // BUY
  // =========================================================

  if (data.msg_type === "buy") {
    const contract = data.buy;

    state.activeContractId =
      contract.contract_id;

    const trade = {
      openTime: Date.now(),
      direction:
        state.activeTrade?.direction ||
        "BUY",

      stake: state.stake,

      entryPrice: state.currentPrice,

      status: "open",
    };

    state.activeTrade = trade;

    state.todayTradeCount++;

    log(
      `Trade Opened ${trade.direction}`,
      "ok"
    );

    broadcast({
      type: "TRADE_OPENED",
      trade,
    });

    sendDeriv({
      proposal_open_contract: 1,
      contract_id:
        contract.contract_id,

      subscribe: 1,
    });

    return;
  }

  // =========================================================
  // CONTRACT RESULT
  // =========================================================

  if (
    data.msg_type ===
    "proposal_open_contract"
  ) {
    const poc =
      data.proposal_open_contract;

    if (!poc.is_sold) {
      return;
    }

    if (!state.activeTrade) {
      return;
    }

    const profit = parseFloat(
      poc.profit || 0
    );

    const trade = {
      ...state.activeTrade,

      exitPrice:
        poc.sell_price ||

        state.currentPrice,

      profit,

      status:
        profit > 0
          ? "win"
          : "loss",
    };

    handleContractClose(trade);

    return;
  }
}

// ============================================================
// SUBSCRIBE TICKS
// ============================================================

function subscribeTicks(symbol) {
  sendDeriv({
    ticks: symbol,
    subscribe: 1,
  });

  log(`Subscribed ${symbol}`, "ok");
}

// ============================================================
// SIMPLE STRATEGY
// ============================================================

function analyzeSignal() {
  const prices = state.priceHistory;

  if (prices.length < 20) {
    return {
      signal: "WAIT",
      strength: 0,
    };
  }

  const recent = prices.slice(-5);

  let up = 0;
  let down = 0;

  for (let i = 1; i < recent.length; i++) {
    if (recent[i] > recent[i - 1]) {
      up++;
    }

    if (recent[i] < recent[i - 1]) {
      down++;
    }
  }

  if (up >= 3) {
    return {
      signal: "BUY",
      strength: 75,
    };
  }

  if (down >= 3) {
    return {
      signal: "SELL",
      strength: 75,
    };
  }

  return {
    signal: "WAIT",
    strength: 20,
  };
}

// ============================================================
// BOT LOGIC
// ============================================================

function runBotLogic() {
  if (!state.authorized) return;

  if (!state.botRunning) return;

  if (state.activeTrade) return;

  const now = Date.now();

  if (
    now - state.lastSignalTime <
    3000
  ) {
    return;
  }

  state.lastSignalTime = now;

  const analysis = analyzeSignal();

  broadcast({
    type: "SIGNAL_UPDATE",
    analysis,
  });

  if (
    analysis.signal === "WAIT"
  ) {
    return;
  }

  let contractType = "CALL";

  if (
    analysis.signal === "SELL"
  ) {
    contractType = "PUT";
  }

  placeTrade(
    contractType,
    analysis.signal
  );
}

// ============================================================
// PLACE TRADE
// ============================================================

function placeTrade(
  contractType,
  direction
) {
  if (!state.authorized) {
    return;
  }

  state.activeTrade = {
    openTime: Date.now(),

    direction,

    stake: state.stake,

    entryPrice:
      state.currentPrice,

    status: "open",
  };

  log(
    `Proposal ${direction}`,
    "info"
  );

  sendDeriv({
    proposal: 1,

    amount: state.stake,

    basis: "stake",

    contract_type:
      contractType,

    currency: state.currency,

    duration: state.duration,

    duration_unit:
      state.durationUnit,

    symbol: state.symbol,
  });
}

// ============================================================
// CONTRACT CLOSE
// ============================================================

function handleContractClose(
  trade
) {
  const isWin =
    trade.status === "win";

  state.trades.unshift(trade);

  if (state.trades.length > 200) {
    state.trades.pop();
  }

  state.todayPnl += trade.profit;

  if (isWin) {
    state.todayWins++;

    state.currentStreak++;

    if (
      state.currentStreak >
      state.bestStreak
    ) {
      state.bestStreak =
        state.currentStreak;
    }

    state.consecutiveLosses = 0;

    state.martStep = 0;

    state.stake =
      state.baseStake;
  } else {
    state.todayLosses++;

    state.currentStreak = 0;

    state.consecutiveLosses++;

    if (
      state.martingaleEnabled
    ) {
      if (
        state.martStep <
        state.martingaleMaxSteps
      ) {
        state.martStep++;

        state.stake =
          parseFloat(
            (
              state.baseStake *
              Math.pow(
                state.martingaleMultiplier,
                state.martStep
              )
            ).toFixed(2)
          );
      }
    }
  }

  log(
    `Trade Closed ${trade.status.toUpperCase()} Profit ${trade.profit}`,
    isWin ? "ok" : "err"
  );

  broadcast({
    type: "TRADE_CLOSED",
    trade,
  });

  state.activeTrade = null;

  state.activeContractId = null;
}

// ============================================================
// BOT CONTROL
// ============================================================

function startBot() {
  state.botRunning = true;

  log("Bot started", "ok");

  broadcast({
    type: "BOT_STATUS",
    running: true,
  });
}

function stopBot() {
  state.botRunning = false;

  log("Bot stopped", "warn");

  broadcast({
    type: "BOT_STATUS",
    running: false,
  });
}

// ============================================================
// WEBSOCKET SERVER
// ============================================================

wss.on("connection", (browserWs) => {
  state.browserClients.add(browserWs);

  console.log(
    `[+] Browser Connected (${state.browserClients.size})`
  );

  browserWs.send(
    JSON.stringify({
      type: "FULL_STATE",

      state: {
        authorized:
          state.authorized,

        loginid:
          state.loginid,

        balance:
          state.balance,

        currency:
          state.currency,

        botRunning:
          state.botRunning,

        symbol:
          state.symbol,

        trades:
          state.trades,

        currentPrice:
          state.currentPrice,
      },
    })
  );

  browserWs.on(
    "message",
    (raw) => {
      let cmd;

      try {
        cmd = JSON.parse(
          raw.toString()
        );
      } catch {
        return;
      }

      switch (cmd.type) {
        case "CONNECT":
          saveConfig({
            token: cmd.token,
          });

          connectDeriv(
            cmd.token
          );

          break;

        case "START_BOT":
          startBot();
          break;

        case "STOP_BOT":
          stopBot();
          break;

        case "CHANGE_SYMBOL":
          state.symbol =
            cmd.symbol;

          subscribeTicks(
            cmd.symbol
          );

          break;
      }
    }
  );

  browserWs.on("close", () => {
    state.browserClients.delete(
      browserWs
    );
  });
});

// ============================================================
// REST API
// ============================================================

app.post("/api/token", (req, res) => {
  const { token } = req.body;

  if (!token) {
    return res.status(400).json({
      error: "token required",
    });
  }

  saveConfig({ token });

  connectDeriv(token);

  res.json({
    ok: true,
  });
});

app.get("/api/status", (req, res) => {
  res.json({
    authorized:
      state.authorized,

    loginid:
      state.loginid,

    balance:
      state.balance,

    currency:
      state.currency,

    botRunning:
      state.botRunning,
  });
});

// ============================================================
// ANGULAR FALLBACK
// ============================================================

app.get("*", (req, res) => {
  const index = path.join(
    __dirname,
    "dist/derivbot-pro/browser/index.html"
  );

  res.sendFile(index, (err) => {
    if (err) {
      res.send(`
        <html>
          <body style="background:#0b0f19;color:#fff;font-family:Arial;padding:40px;">
            <h2>DerivBot Pro Backend Running ✓</h2>
          </body>
        </html>
      `);
    }
  });
});

// ============================================================
// START SERVER
// ============================================================

server.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════╗
║   DerivBot Pro 2026 Running     ║
╚══════════════════════════════════╝
`);

  const cfg = loadConfig();

  if (cfg.token) {
    console.log(
      "[INFO] Auto connecting..."
    );

    connectDeriv(cfg.token);
  }
});