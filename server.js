const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";

const DERIV_WS = "wss://ws.binaryws.com/websockets/v3";
const TIMEFRAMES = { M5: 300, M15: 900, H1: 3600 };
const HISTORY_COUNT = 250;
const SCAN_MS = 15000;
const ALERT_COOLDOWN_MS = 15 * 60 * 1000;

const state = {
  online: false,
  lastUpdate: null,
  symbols: [],
  rows: {},
  alerts: [],
  error: null
};

let ws = null;
let reqId = 100;
let pending = new Map();
let reconnectTimer = null;

function rid() { return ++reqId; }

function send(payload) {
  return new Promise((resolve, reject) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return reject(new Error("Deriv WebSocket is not connected"));
    }
    const id = rid();
    payload.req_id = id;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify(payload));
    setTimeout(() => {
      if (pending.has(id)) {
        pending.delete(id);
        reject(new Error("Deriv request timeout"));
      }
    }, 12000);
  });
}

function connect() {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

  ws = new WebSocket(DERIV_WS);

  ws.on("open", async () => {
    state.online = true;
    state.error = null;
    try {
      const result = await send({
        active_symbols: "brief",
        product_type: "basic"
      });
      const all = result.active_symbols || [];
      state.symbols = all
        .filter(s => /volatility/i.test(s.display_name || "") || /HZ[0-9]+V/i.test(s.symbol || ""))
        .map(s => ({
          symbol: s.symbol,
          name: s.display_name,
          market: s.market
        }))
        .sort((a,b) => a.name.localeCompare(b.name));

      // Limit initial scan to symbols actually returned by Deriv.
      for (const s of state.symbols) {
        if (!state.rows[s.symbol]) {
          state.rows[s.symbol] = {
            symbol: s.symbol, name: s.name, price: null,
            h1: "WAIT", m15: "WAIT", m5: "WAIT",
            burst: "NONE", direction: "WAIT", score: 0,
            entry: null, sl: null, tp: null, updated: null,
            cooldownUntil: 0, lastAlertKey: ""
          };
        }
      }
      await scanAll();
    } catch (e) {
      state.error = e.message;
    }
  });

  ws.on("message", raw => {
    try {
      const data = JSON.parse(raw.toString());
      if (data.req_id && pending.has(data.req_id)) {
        const p = pending.get(data.req_id);
        pending.delete(data.req_id);
        if (data.error) p.reject(new Error(data.error.message || "Deriv API error"));
        else p.resolve(data);
      }
    } catch (_) {}
  });

  ws.on("close", () => {
    state.online = false;
    scheduleReconnect();
  });

  ws.on("error", err => {
    state.error = err.message;
  });
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, 4000);
}

async function getCandles(symbol, granularity) {
  const result = await send({
    ticks_history: symbol,
    end: "latest",
    count: HISTORY_COUNT,
    style: "candles",
    granularity,
    subscribe: 0
  });
  return (result.candles || []).map(c => ({
    time: Number(c.epoch),
    open: Number(c.open),
    high: Number(c.high),
    low: Number(c.low),
    close: Number(c.close)
  })).filter(c => [c.open,c.high,c.low,c.close].every(Number.isFinite));
}

function ema(values, len) {
  if (values.length < len) return null;
  const k = 2 / (len + 1);
  let e = values.slice(0, len).reduce((a,b) => a+b, 0) / len;
  for (let i=len; i<values.length; i++) e = values[i] * k + e * (1-k);
  return e;
}

function atr(c, len=14) {
  if (c.length < len + 1) return null;
  const tr = [];
  for (let i=1; i<c.length; i++) {
    tr.push(Math.max(
      c[i].high-c[i].low,
      Math.abs(c[i].high-c[i-1].close),
      Math.abs(c[i].low-c[i-1].close)
    ));
  }
  return tr.slice(-len).reduce((a,b)=>a+b,0)/len;
}

function rsi(closes, len=14) {
  if (closes.length < len+1) return 50;
  let gains=0, losses=0;
  for (let i=closes.length-len; i<closes.length; i++) {
    const d=closes[i]-closes[i-1];
    if (d>=0) gains += d; else losses -= d;
  }
  if (losses===0) return 100;
  const rs=(gains/len)/(losses/len);
  return 100-(100/(1+rs));
}

function structure(c) {
  if (c.length < 20) return "WAIT";
  const a=c[c.length-2], b=c[c.length-3], recent=c.slice(-8,-1);
  const hi=Math.max(...recent.map(x=>x.high));
  const lo=Math.min(...recent.map(x=>x.low));
  if (a.close > hi || a.high > hi && a.close > a.open) return "BULLISH BOS";
  if (a.close < lo || a.low < lo && a.close < a.open) return "BEARISH BOS";
  if (a.close > b.high) return "BULLISH CHOCH";
  if (a.close < b.low) return "BEARISH CHOCH";
  return a.close >= ema(c.map(x=>x.close),20) ? "BULLISH" : "BEARISH";
}

function burstFader(c) {
  if (c.length < 30) return { status:"NONE", peak:0 };
  const closes=c.map(x=>x.close);
  const basis=ema(closes,20);
  const a=atr(c,14);
  if (!basis || !a || a<=0) return {status:"NONE",peak:0};

  const x=c[c.length-2]; // completed candle
  const prev=c[c.length-3];
  const upExt=(x.high-(basis+2*a))/a;
  const dnExt=((basis-2*a)-x.low)/a;
  const prevUp=Math.max(0,(prev.high-(basis+2*a))/a);
  const prevDn=Math.max(0,((basis-2*a)-prev.low)/a);

  // A fader is strongest when the completed burst is larger than the next bar.
  if (upExt>0 && upExt>=prevUp) {
    const levels=Math.min(6,1+Math.floor(upExt/0.5));
    return {status:`UP BURST ${"◆".repeat(levels)}`, peak:levels, side:"UP", magnitude:upExt};
  }
  if (dnExt>0 && dnExt>=prevDn) {
    const levels=Math.min(6,1+Math.floor(dnExt/0.5));
    return {status:`DOWN BURST ${"◆".repeat(levels)}`, peak:levels, side:"DOWN", magnitude:dnExt};
  }
  return {status:"NONE",peak:0};
}

function analyze(symbol, h1, m15, m5) {
  const row=state.rows[symbol];
  const c5=m5, c15=m15, c1=h1;
  const close5=c5[c5.length-2]?.close;
  const e1=ema(c1.map(x=>x.close),20);
  const e15=ema(c15.map(x=>x.close),20);
  const e5=ema(c5.map(x=>x.close),20);
  const h1Dir = e1 == null ? "WAIT" : closeDir(c1[c1.length-2].close,e1);
  const m15Dir = e15 == null ? "WAIT" : closeDir(c15[c15.length-2].close,e15);
  const s15=structure(c15);
  const s5=structure(c5);
  const burst=burstFader(c5);
  let score=0;
  if (h1Dir==="BULLISH" || h1Dir==="BEARISH") score++;
  if (m15Dir===h1Dir) score++;
  if (s15.startsWith(h1Dir)) score++;
  if (s5.startsWith(h1Dir)) score++;
  if (burst.peak>=3) score++;

  let direction="WAIT";
  // Normal setups can work without a burst. A burst only strengthens/reverses a candidate.
  if (h1Dir==="BULLISH" && m15Dir==="BULLISH" && (s5.includes("BULLISH") || burst.side==="DOWN") && score>=3) direction="BUY";
  if (h1Dir==="BEARISH" && m15Dir==="BEARISH" && (s5.includes("BEARISH") || burst.side==="UP") && score>=3) direction="SELL";

  const a5=atr(c5,14) || Math.abs(c5[c5.length-2].high-c5[c5.length-2].low);
  const entry=close5;
  let sl=null,tp=null;
  if (direction==="BUY") { sl=Math.min(...c5.slice(-6,-1).map(x=>x.low))-a5*0.2; tp=entry+(entry-sl)*2; }
  if (direction==="SELL") { sl=Math.max(...c5.slice(-6,-1).map(x=>x.high))+a5*0.2; tp=entry-(sl-entry)*2; }

  const decimals=priceDecimals(entry);
  row.price=entry;
  row.h1=h1Dir; row.m15=m15Dir; row.m5=s5;
  row.burst=burst.status; row.direction=direction; row.score=score;
  row.entry=entry; row.sl=sl; row.tp=tp; row.updated=Date.now();

  if (direction!=="WAIT" && Date.now()>=row.cooldownUntil) {
    const key=`${direction}-${Math.round(entry*10**decimals)}`;
    if (row.lastAlertKey!==key) {
      row.lastAlertKey=key;
      row.cooldownUntil=Date.now()+ALERT_COOLDOWN_MS;
      const msg=`${direction==="BUY"?"🟢":"🔴"} ${direction} ${row.name}\n📍 Entry: ${fmt(entry,decimals)}\n🛑 SL: ${fmt(sl,decimals)}\n🎯 TP: ${fmt(tp,decimals)}\n⭐ Score: ${score}/5\n📊 1H: ${h1Dir}\n📊 15M: ${m15Dir}\n📊 5M: ${s5}\n💥 Burst: ${burst.status}\n🔎 SMC: ${s5}`;
      state.alerts.unshift({time:Date.now(),message:msg});
      state.alerts=state.alerts.slice(0,30);
      sendTelegram(msg).catch(()=>{});
    }
  }
}

function closeDir(price,e) { return price>=e ? "BULLISH" : "BEARISH"; }
function priceDecimals(v) {
  if (!Number.isFinite(v)) return 2;
  if (v >= 1000) return 2;
  if (v >= 100) return 3;
  return 4;
}
function fmt(v,d) { return Number.isFinite(v) ? v.toFixed(d) : "—"; }

async function sendTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  const url=`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  await fetch(url,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({chat_id:TELEGRAM_CHAT_ID,text})});
}

let scanning=false;
async function scanAll() {
  if (scanning || !state.online) return;
  scanning=true;
  try {
    // Sequential requests are intentional: they reduce bursts against the public API.
    for (const s of state.symbols) {
      try {
        const [h1,m15,m5]=await Promise.all([
          getCandles(s.symbol,TIMEFRAMES.H1),
          getCandles(s.symbol,TIMEFRAMES.M15),
          getCandles(s.symbol,TIMEFRAMES.M5)
        ]);
        if (h1.length && m15.length && m5.length) analyze(s.symbol,h1,m15,m5);
      } catch(e) {
        state.rows[s.symbol].error=e.message;
      }
    }
    state.lastUpdate=Date.now();
  } finally {
    scanning=false;
  }
}

app.get("/api/status",(req,res)=>{
  res.json({
    ok:true, online:state.online, lastUpdate:state.lastUpdate,
    symbols:state.symbols, rows:Object.values(state.rows),
    alerts:state.alerts, error:state.error,
    timeframes:{H1:"Direction",M15:"Structure",M5:"Entry + Burst Fader"}
  });
});

app.get("/health",(req,res)=>res.json({ok:true,online:state.online}));

app.listen(PORT,()=> {
  console.log(`Deriv Volatility Burst Fader running on port ${PORT}`);
  connect();
  setInterval(scanAll,SCAN_MS);
});
