import asyncio
import json
import logging
import os
import urllib.error
import urllib.request
import zoneinfo
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional

import numpy as np
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# =============================================================================
# ALPHAWAVE SIGNAL DASHBOARD & ZERO-DELAY TELEGRAM DISPATCHER
# Matches C:\Projects\trading\PA & SignalValidator Architecture
# Includes Automated Market Hours Live Scanner & Webhook Receiver
# =============================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("signal_center")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_FILE = DATA_DIR / "signals.json"
ENV_FILE = BASE_DIR / ".env"
PA_ENV_FILE = Path("C:/Projects/trading/PA/.env")

# --- Environment & Configuration Loading ---
def load_env_var(key: str, default: str = "") -> str:
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{key}="):
                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                if val:
                    return val

    val = os.getenv(key)
    if val:
        return val.strip()

    if PA_ENV_FILE.exists():
        for line in PA_ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{key}=") or line.startswith(f"PA_{key}="):
                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                if val:
                    return val

    return default

def save_env_var(key: str, value: str):
    lines = []
    if ENV_FILE.exists():
        lines = ENV_FILE.read_text(encoding="utf-8").splitlines()
    
    found = False
    new_lines = []
    for line in lines:
        if line.strip().startswith(f"{key}="):
            new_lines.append(f"{key}={value}")
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f"{key}={value}")
    
    ENV_FILE.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    os.environ[key] = value

# Initialize Settings
# Secrets live in .env (see .env.example) or can be set from the dashboard Settings panel.
TELEGRAM_BOT_TOKEN = load_env_var("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = load_env_var("TELEGRAM_CHAT_ID", "")
SCANNER_MODE = load_env_var("SCANNER_MODE", "MARKET_HOURS") # MARKET_HOURS, ALWAYS_ON, OFF
SIGNAL_MODE = load_env_var("SIGNAL_MODE", "ALL_CONFLUENCE") # ALL_CONFLUENCE, PULLBACK_ONLY, BREAKOUT_ONLY
DEFAULT_WATCHLIST = "AAPL,AMZN,COIN,GOOGL,HOOD,INTC,IWM,META,MRVL,MSFT,MU,NBIS,NFLX,NVDA,PLTR,QQQ,SNDK,SPCX,SPX,SPY,TSLA,UNH,WMT"
WATCHLIST_STR = load_env_var("WATCHLIST", DEFAULT_WATCHLIST)

# --- Telegram Dispatch Logic (Zero Delay Direct HTTPS Post) ---
def send_telegram_instant(text: str, token: Optional[str] = None, chat_id: Optional[str] = None) -> bool:
    """Zero-delay direct HTTPS POST to Telegram Bot API with no queue wait."""
    t = (token or load_env_var("TELEGRAM_BOT_TOKEN") or TELEGRAM_BOT_TOKEN or "").strip()
    c = (chat_id or load_env_var("TELEGRAM_CHAT_ID") or TELEGRAM_CHAT_ID or "").strip()

    if not t or not c:
        log.warning("Telegram alert skipped: TELEGRAM_BOT_TOKEN or CHAT_ID not configured.")
        return False

    url = f"https://api.telegram.org/bot{t}/sendMessage"
    payload = {
        "chat_id": c,
        "text": text,
        "disable_web_page_preview": True
    }
    
    try:
        raw = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=raw, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read().decode("utf-8") or "{}")
        ok = bool(body.get("ok"))
        if ok:
            log.info("Telegram message dispatched successfully: '%s'", text)
        else:
            log.warning("Telegram rejected message: %s", body.get("description") or body)
        return ok
    except Exception as exc:
        log.error("Telegram send failed: %s", exc)
        return False

# --- Signal Storage & State (Side-by-Side Active & Take Profit) ---
active_positions: List[Dict] = []
tp_positions: List[Dict] = []
sse_subscribers: List[asyncio.Queue] = []

CHICAGO_TZ = zoneinfo.ZoneInfo("America/Chicago")

def now_central() -> datetime:
    return datetime.now(CHICAGO_TZ)

def parse_to_central(ts: str) -> datetime:
    """Parse any ISO timestamp (UTC, naive, or offset) and convert to Central Time (America/Chicago)"""
    try:
        ts_clean = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=CHICAGO_TZ)
        else:
            dt = dt.astimezone(CHICAGO_TZ)
        return dt
    except Exception:
        return now_central()

def format_time_with_seconds(dt: Optional[datetime] = None) -> str:
    """Format time with seconds precision strictly in Central Time (America/Chicago), e.g. 9:55:43"""
    if dt is None:
        dt = now_central()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=CHICAGO_TZ)
    else:
        dt = dt.astimezone(CHICAGO_TZ)
    h = dt.hour % 12
    if h == 0:
        h = 12
    return f"{h}:{dt.minute:02d}:{dt.second:02d}"

def load_signals():
    global active_positions, tp_positions
    if SIGNALS_FILE.exists():
        try:
            data = json.loads(SIGNALS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                active_positions = data.get("active_positions", [])
                tp_positions = data.get("tp_positions", [])
            elif isinstance(data, list):
                active_positions = [s for s in data if s.get("status") == "OPEN"]
                tp_positions = [s for s in data if s.get("status") == "CLOSED"]

            # Ensure all historical timestamps are in Central Time and include seconds & date
            for item in active_positions:
                ts = item.get("timestamp")
                if ts:
                    try:
                        dt = parse_to_central(ts)
                        item["entry_time"] = format_time_with_seconds(dt)
                        item["date"] = dt.strftime("%Y-%m-%d")
                    except Exception:
                        pass
                if "date" not in item or not item["date"]:
                    item["date"] = now_central().strftime("%Y-%m-%d")

            for item in tp_positions:
                ts = item.get("timestamp")
                if ts:
                    try:
                        dt = parse_to_central(ts)
                        item["exit_time"] = format_time_with_seconds(dt)
                        item["date"] = dt.strftime("%Y-%m-%d")
                    except Exception:
                        pass
                if "date" not in item or not item["date"]:
                    item["date"] = now_central().strftime("%Y-%m-%d")
        except Exception:
            active_positions = []
            tp_positions = []
    else:
        active_positions = []
        tp_positions = []

def save_signals():
    payload = {
        "active_positions": active_positions,
        "tp_positions": tp_positions
    }
    SIGNALS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

load_signals()

async def broadcast_signal(data: dict):
    save_signals()
    data_str = f"data: {json.dumps(data)}\n\n"
    dead = []
    for q in sse_subscribers:
        try:
            await q.put(data_str)
        except Exception:
            dead.append(q)
    for d in dead:
        if d in sse_subscribers:
            sse_subscribers.remove(d)

# --- FastAPI App Setup ---
app = FastAPI(title="AlphaWave Indicator Signal Center", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConfigInput(BaseModel):
    telegram_bot_token: str
    telegram_chat_id: str
    scanner_mode: Optional[str] = "MARKET_HOURS"
    signal_mode: Optional[str] = "ALL_CONFLUENCE"
    watchlist: Optional[str] = "SPY,QQQ,TSLA,NVDA,AAPL"

def format_telegram_play(ticker: str, raw_play: str) -> tuple[str, str]:
    tk = ticker.strip().upper()
    p = (raw_play or "").strip()
    p_upper = p.upper()

    is_tp = ("TP" in p_upper) or ("TAKE PROFIT" in p_upper) or ("PROFIT" in p_upper) or ("EXIT" in p_upper)
    is_call = "CALL" in p_upper
    is_put = "PUT" in p_upper

    if is_tp and is_call:
        return f"Take Profit {tk} CALL", f"Take Profit {tk} CALL"
    elif is_tp and is_put:
        return f"Take Profit {tk} PUT", f"Take Profit {tk} PUT"
    elif is_call:
        return "CALL", f"CALL {tk}"
    elif is_put:
        return "PUT", f"PUT {tk}"
    
    return p, f"{p} {tk}"

def record_and_dispatch(ticker: str, raw_play: str, price: Optional[float] = None) -> dict:
    tk = ticker.strip().upper()
    dash_play, tg_message = format_telegram_play(tk, raw_play)

    # 1. Zero delay send to Telegram FIRST (non-blocking)
    send_telegram_instant(tg_message)

    # 2. Update Side-by-Side Tables
    p_upper = (raw_play or "").upper()
    is_exit = ("TP" in p_upper) or ("TAKE PROFIT" in p_upper) or ("PROFIT" in p_upper) or ("EXIT" in p_upper)
    is_call = "CALL" in p_upper
    is_put = "PUT" in p_upper
    play_dir = "CALL" if is_call else ("PUT" if is_put else "CALL")

    now_str = format_time_with_seconds()

    if is_exit:
        # Check active positions for this ticker to extract entry time and remove it
        entry_time = "--"
        active_match = None
        for i, a in enumerate(active_positions):
            if a.get("ticker") == tk:
                active_match = active_positions.pop(i)
                entry_time = active_match.get("entry_time", "--")
                break

        now_dt = now_central()
        now_str = format_time_with_seconds(now_dt)
        today_date_str = now_dt.strftime("%Y-%m-%d")

        tp_entry = {
            "id": len(tp_positions) + 1,
            "ticker": tk,
            "play": play_dir,
            "exit_time": now_str,
            "entry_time": entry_time,
            "price": price,
            "date": today_date_str,
            "timestamp": now_dt.isoformat()
        }
        tp_positions.insert(0, tp_entry)
        if len(tp_positions) > 300:
            tp_positions.pop()

        result_entry = tp_entry
    else:  # Active Entry (CALL or PUT)
        now_dt = now_central()
        now_str = format_time_with_seconds(now_dt)
        today_date_str = now_dt.strftime("%Y-%m-%d")

        # Check if already in active positions
        existing_idx = None
        for i, a in enumerate(active_positions):
            if a.get("ticker") == tk:
                existing_idx = i
                break

        if existing_idx is not None:
            existing = active_positions.pop(existing_idx)
            existing["play"] = play_dir
            existing["entry_time"] = now_str
            existing["price"] = price
            existing["date"] = today_date_str
            existing["timestamp"] = now_dt.isoformat()
            active_positions.insert(0, existing)
            result_entry = existing
        else:
            new_active = {
                "id": len(active_positions) + 1,
                "ticker": tk,
                "play": play_dir,
                "entry_time": now_str,
                "price": price,
                "date": today_date_str,
                "timestamp": now_dt.isoformat()
            }
            active_positions.insert(0, new_active)
            result_entry = new_active

        if len(active_positions) > 300:
            active_positions.pop()

    # Broadcast updated state to all connected SSE clients
    asyncio.create_task(broadcast_signal({
        "active_positions": active_positions,
        "tp_positions": tp_positions,
        "latest": result_entry,
        "today": datetime.now().strftime("%Y-%m-%d")
    }))

    return result_entry

# --- Market Hours Checker (Central Time: 8:30 AM - 3:00 PM CST/CDT) ---
def is_market_open() -> bool:
    now = now_central()
    # Mon-Fri
    if now.weekday() > 4:
        return False
    market_open = now.replace(hour=8, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close

# --- Automated Market Hours Scanner (1-to-1 Pine Script v5 Engine) ---
scanner_state: Dict[str, Dict] = {} # ticker -> { "last_evaluated_bar": None, "last_dispatched_sig": None }

def evaluate_pine_indicator(df: pd.DataFrame) -> Optional[Dict]:
    """
    1-to-1 exact Python replica of MultiConfluence_Signal_Indicator.pine (TradingView v5)
    Evaluates historical closed bars sequentially with full confluence & dynamic TP state machine.
    """
    if df is None or len(df) < 50:
        return None

    c = df['Close']
    o = df['Open']
    h = df['High']
    l = df['Low']
    v = df['Volume']

    ema9 = c.ewm(span=9, adjust=False).mean()
    ema21 = c.ewm(span=21, adjust=False).mean()
    ema50 = c.ewm(span=50, adjust=False).mean()

    # Wilder RSI 14
    delta = c.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
    rs = avg_gain / (avg_loss.replace(0, 0.0001))
    rsi = 100 - (100 / (1 + rs))

    # MACD (12, 26, 9)
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal

    vol_ma = v.rolling(20).mean()

    # Determine confirmed closed bar boundary
    # In 5m bars, if the latest bar timestamp is within 5m of now, it is currently forming/ticking.
    # The last confirmed closed bar is at len(df) - 2.
    now = datetime.now(timezone.utc)
    last_bar_time = df.index[-1]
    if hasattr(last_bar_time, 'tzinfo') and last_bar_time.tzinfo is not None:
        bar_utc = last_bar_time.astimezone(timezone.utc)
    else:
        bar_utc = last_bar_time.replace(tzinfo=timezone.utc)

    if (now - bar_utc).total_seconds() < 300:
        closed_end_idx = len(df) - 1
    else:
        closed_end_idx = len(df)

    if closed_end_idx < 30:
        return None

    signal_dir = 0 # 1 = CALL, -1 = PUT, 0 = FLAT / EXITED
    in_call = False
    in_put = False
    signals = []

    for i in range(1, closed_end_idx):
        t = df.index[i]
        curr_c = c.iloc[i]
        curr_o = o.iloc[i]
        curr_h = h.iloc[i]
        curr_l = l.iloc[i]
        curr_v = v.iloc[i]
        curr_vol_ma = vol_ma.iloc[i]

        curr_rsi = rsi.iloc[i]
        prev_rsi = rsi.iloc[i-1]

        curr_hist = hist.iloc[i]
        prev_hist = hist.iloc[i-1]

        curr_ema9 = ema9.iloc[i]
        prev_ema9 = ema9.iloc[i-1]
        curr_ema21 = ema21.iloc[i]
        prev_ema21 = ema21.iloc[i-1]
        curr_ema50 = ema50.iloc[i]

        # Engine A: Trend Pullback Setup (Value Zone)
        bull_trend = (curr_c > curr_ema50) and (curr_ema21 > curr_ema50)
        bear_trend = (curr_c < curr_ema50) and (curr_ema21 < curr_ema50)

        bull_dip = (curr_l <= curr_ema21 * 1.012) and (curr_c >= curr_ema50 * 0.975)
        bear_rally = (curr_h >= curr_ema21 * 0.988) and (curr_c <= curr_ema50 * 1.025)

        rsi_bull_turn = (curr_rsi > 45 and prev_rsi <= 45)
        macd_bull_turn = (curr_hist > 0 and prev_hist <= 0)
        bull_trigger = rsi_bull_turn or macd_bull_turn

        rsi_bear_turn = (curr_rsi < 55 and prev_rsi >= 55)
        macd_bear_turn = (curr_hist < 0 and prev_hist >= 0)
        bear_trigger = rsi_bear_turn or macd_bear_turn

        vol_confirmed_pb = (curr_v >= (curr_vol_ma * 0.80))
        pullback_call = bull_trend and bull_dip and bull_trigger and vol_confirmed_pb
        pullback_put = bear_trend and bear_rally and bear_trigger and vol_confirmed_pb

        # Engine B: Institutional Volume Breakout & V-Bottom Reversal
        # Genuine breakout requires price to emerge from below/crossing EMAs rather than an extended high candle
        was_below_or_crossing_call = (c.iloc[i-1] <= prev_ema9) or (c.iloc[i-1] <= prev_ema21) or (curr_o <= curr_ema9) or (curr_o <= curr_ema21)
        vol_confirmed_bo = (curr_v >= (curr_vol_ma * 1.10))
        breakout_call = was_below_or_crossing_call and (curr_c > curr_ema9) and (curr_c > curr_ema21) and (curr_c > curr_o) and vol_confirmed_bo and (curr_hist > 0) and (curr_rsi >= 46) and (curr_hist > prev_hist)

        was_above_or_crossing_put = (c.iloc[i-1] >= prev_ema9) or (c.iloc[i-1] >= prev_ema21) or (curr_o >= curr_ema9) or (curr_o >= curr_ema21)
        breakout_put = was_above_or_crossing_put and (curr_c < curr_ema9) and (curr_c < curr_ema21) and (curr_c < curr_o) and vol_confirmed_bo and (curr_hist < 0) and (curr_rsi <= 54) and (curr_hist < prev_hist)

        sig_mode = load_env_var("SIGNAL_MODE", SIGNAL_MODE).upper()
        if "PULLBACK" in sig_mode:
            raw_call = pullback_call
            raw_put = pullback_put
        elif "BREAKOUT" in sig_mode:
            raw_call = breakout_call
            raw_put = breakout_put
        else:
            raw_call = pullback_call or breakout_call
            raw_put = pullback_put or breakout_put

        call_sig = raw_call and (signal_dir != 1)
        put_sig = raw_put and (signal_dir != -1)

        # Dynamic Take Profit Signals
        tp_call = in_call and not call_sig and ((curr_c < curr_ema9 and c.iloc[i-1] >= prev_ema9) or (curr_hist < 0 and prev_hist >= 0))
        tp_put = in_put and not put_sig and ((curr_c > curr_ema9 and c.iloc[i-1] <= prev_ema9) or (curr_hist > 0 and prev_hist <= 0))

        if call_sig:
            signal_dir = 1
            in_call = True
            in_put = False
            signals.append({'bar_time': str(t), 'type': 'CALL', 'price': float(curr_c), 'bar_idx': i})
        elif put_sig:
            signal_dir = -1
            in_put = True
            in_call = False
            signals.append({'bar_time': str(t), 'type': 'PUT', 'price': float(curr_c), 'bar_idx': i})
        elif tp_call:
            in_call = False
            signal_dir = 0
            signals.append({'bar_time': str(t), 'type': 'TP_CALL', 'price': float(curr_c), 'bar_idx': i})
        elif tp_put:
            in_put = False
            signal_dir = 0
            signals.append({'bar_time': str(t), 'type': 'TP_PUT', 'price': float(curr_c), 'bar_idx': i})

    last_closed_bar_idx = closed_end_idx - 1
    last_closed_bar_time = str(df.index[last_closed_bar_idx])

    # Check if a new confirmed signal occurred on the most recent closed bar
    new_event = None
    if signals and signals[-1]['bar_idx'] == last_closed_bar_idx:
        new_event = signals[-1]

    return {
        'last_closed_bar_time': last_closed_bar_time,
        'position': 'CALL' if in_call else ('PUT' if in_put else 'FLAT'),
        'new_event': new_event,
        'latest_signal': signals[-1] if signals else None
    }

async def market_scanner_loop():
    global SCANNER_MODE, WATCHLIST_STR
    log.info("Market Scanner Background Worker (Pine v5 Confluence Engine) Started.")

    while True:
        try:
            mode = load_env_var("SCANNER_MODE", SCANNER_MODE).upper()
            should_scan = (mode == "ALWAYS_ON") or (mode == "MARKET_HOURS" and is_market_open())

            if should_scan:
                raw_wl = load_env_var("WATCHLIST", WATCHLIST_STR)
                wl = [t.strip().upper() for t in raw_wl.split(",") if t.strip()]
                yf_map = {tk: ("^SPX" if tk == "SPX" else tk) for tk in wl}
                tickers_str = " ".join(list(dict.fromkeys(yf_map.values())))

                # Batch download past 5 days of 5m candles (sufficient warmup for EMA50, EMA200, MACD, RSI)
                data = await asyncio.to_thread(yf.download, tickers=tickers_str, period="5d", interval="5m", progress=False)

                if data is not None and not data.empty:
                    for tk in wl:
                        try:
                            yf_tk = yf_map[tk]
                            if isinstance(data.columns, pd.MultiIndex):
                                df = data.xs(yf_tk, level=1, axis=1) if yf_tk in data.columns.levels[1] else None
                            else:
                                df = data if len(wl) == 1 else None

                            if df is None or df.empty or len(df) < 50:
                                continue

                            df = df.dropna().copy()
                            result = evaluate_pine_indicator(df)
                            if result is None:
                                continue

                            closed_bar_time = result['last_closed_bar_time']
                            state = scanner_state.setdefault(tk, {"last_evaluated_bar": None, "last_dispatched_sig": None})

                            if state.get("last_evaluated_bar") == closed_bar_time:
                                continue # This confirmed bar was already evaluated

                            state["last_evaluated_bar"] = closed_bar_time

                            # If a confirmed signal triggered on this closed bar
                            if result['new_event']:
                                sig = result['new_event']
                                sig_key = f"{sig['type']}_{sig['bar_time']}"
                                if state.get("last_dispatched_sig") != sig_key:
                                    state["last_dispatched_sig"] = sig_key
                                    log.info("Pine Indicator Signal Confirmed: %s %s at %s (Bar: %s)", sig['type'], tk, sig['price'], sig['bar_time'])
                                    record_and_dispatch(tk, sig['type'], sig['price'])

                        except Exception as e:
                            log.warning("Error evaluating ticker %s: %s", tk, e)

        except Exception as e:
            log.warning("Scanner iteration error: %s", e)

        sleep_sec = 5 if is_market_open() else 15
        await asyncio.sleep(sleep_sec)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(market_scanner_loop())

# --- Webhook & API Endpoints ---

@app.get("/health")
@app.get("/healthz")
async def health_check():
    return {"status": "ok", "service": "ChartSignalGenerator", "timestamp": now_central().isoformat()}

@app.get("/api/signals")
async def get_signals():
    return {
        "active_positions": active_positions,
        "tp_positions": tp_positions,
        "signals": active_positions,
        "today": now_central().strftime("%Y-%m-%d"),
        "timezone": "America/Chicago",
        "tz_label": "CST"
    }

@app.post("/api/signal")
@app.post("/webhook")
async def ingest_signal(req: Request):
    """
    Universal webhook receiver for TradingView alerts, Webull triggers, cURL, or local scripts.
    Accepts JSON or raw text with zero delay.
    """
    body_bytes = await req.body()
    body_text = body_bytes.decode("utf-8", errors="ignore").strip()

    ticker = "SPY"
    raw_play = "CALL"
    price = None

    try:
        data = json.loads(body_text)
        ticker = data.get("ticker") or data.get("symbol") or "SPY"
        raw_play = data.get("play") or data.get("action") or data.get("signal") or "CALL"
        price = data.get("price")
    except Exception:
        parts = body_text.split()
        if len(parts) >= 2:
            if parts[0].upper() in ["CALL", "PUT"]:
                raw_play = parts[0].upper()
                ticker = parts[1].upper()
            elif "TAKE" in body_text.upper() or "TP" in body_text.upper():
                raw_play = body_text
                for p in parts:
                    if p.isalpha() and p.isupper() and p not in ["TAKE", "PROFIT", "TP", "CALL", "PUT"]:
                        ticker = p
                        break

    entry = record_and_dispatch(ticker, raw_play, price)
    return {
        "status": "ok",
        "ticker": entry.get("ticker"),
        "play": entry.get("play"),
        "dispatched": True
    }

@app.post("/api/clear")
async def clear_signals():
    global active_positions, tp_positions
    active_positions = []
    tp_positions = []
    save_signals()
    return {"status": "cleared"}

@app.get("/api/config")
async def get_config():
    global TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SCANNER_MODE, SIGNAL_MODE, WATCHLIST_STR
    return {
        "telegram_bot_token": load_env_var("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
        "telegram_chat_id": load_env_var("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
        "scanner_mode": load_env_var("SCANNER_MODE", SCANNER_MODE),
        "signal_mode": load_env_var("SIGNAL_MODE", SIGNAL_MODE),
        "watchlist": load_env_var("WATCHLIST", WATCHLIST_STR),
        "market_open": is_market_open()
    }

@app.post("/api/config")
async def update_config(cfg: ConfigInput):
    global TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SCANNER_MODE, SIGNAL_MODE, WATCHLIST_STR
    if cfg.telegram_bot_token:
        TELEGRAM_BOT_TOKEN = cfg.telegram_bot_token.strip()
        save_env_var("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN)
    if cfg.telegram_chat_id:
        TELEGRAM_CHAT_ID = cfg.telegram_chat_id.strip()
        save_env_var("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)
    if cfg.scanner_mode:
        SCANNER_MODE = cfg.scanner_mode.strip()
        save_env_var("SCANNER_MODE", SCANNER_MODE)
    if cfg.signal_mode:
        SIGNAL_MODE = cfg.signal_mode.strip()
        save_env_var("SIGNAL_MODE", SIGNAL_MODE)
    if cfg.watchlist:
        WATCHLIST_STR = cfg.watchlist.strip()
        save_env_var("WATCHLIST", WATCHLIST_STR)
    
    test_ok = send_telegram_instant("Indicator Signals Bot Settings Updated!")
    return {"status": "updated", "test_message_sent": test_ok}

@app.get("/api/stream")
async def sse_stream():
    queue = asyncio.Queue()
    sse_subscribers.append(queue)

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            yield f"data: {json.dumps({'type': 'connected'})}\n\n"
            while True:
                data = await queue.get()
                yield data
        except asyncio.CancelledError:
            if queue in sse_subscribers:
                sse_subscribers.remove(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

# --- HTML Single Page Dashboard ---
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Indicator Signals | Live Positions Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #070b10;
      --panel: #0e141c;
      --card: #111a24;
      --border: #1c2733;
      --border-2: #243040;
      --text: #e7eef5;
      --muted: #7a8795;
      --accent: #2dd4bf;
      --up: #34d399;
      --down: #f43f5e;
      --warn: #fbbf24;
      --sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      --mono: 'JetBrains Mono', monospace;
      --radius: 12px;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: var(--bg);
      color: var(--text);
      font-family: var(--sans);
      min-height: 100vh;
      -webkit-font-smoothing: antialiased;
      overflow-x: hidden;
    }

    .glow {
      position: fixed;
      inset: 0;
      background:
        radial-gradient(60% 55% at 85% 90%, rgba(45, 212, 191, 0.08), transparent 60%),
        radial-gradient(50% 40% at 10% 0%, rgba(52, 211, 153, 0.05), transparent 55%);
      pointer-events: none;
      z-index: 0;
    }

    .wrap {
      position: relative;
      z-index: 1;
      width: 92%;
      max-width: 1300px;
      margin: 0 auto;
      padding: 30px 0 60px;
    }

    .header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--border);
      margin-bottom: 25px;
      flex-wrap: wrap;
      gap: 15px;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .logo-badge {
      background: linear-gradient(135deg, #2dd4bf, #0ea5e9);
      color: #070b10;
      font-family: var(--mono);
      font-weight: 800;
      font-size: 1.1rem;
      padding: 6px 12px;
      border-radius: 8px;
      letter-spacing: -0.05em;
    }

    .brand-title {
      font-size: 1.25rem;
      font-weight: 700;
      letter-spacing: -0.02em;
    }

    .brand-sub {
      color: var(--muted);
      font-size: 0.82rem;
      font-weight: 400;
      margin-left: 6px;
    }

    .nav-actions {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .pulse-dot {
      display: inline-block;
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--up);
      box-shadow: 0 0 10px var(--up);
      animation: pulse 2s infinite;
      margin-right: 6px;
    }

    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }

    .status-chip {
      background: rgba(52, 211, 153, 0.1);
      border: 1px solid rgba(52, 211, 153, 0.25);
      color: var(--up);
      font-size: 0.76rem;
      font-weight: 600;
      padding: 6px 12px;
      border-radius: 20px;
      display: flex;
      align-items: center;
    }

    .tz-chip {
      background: rgba(45, 212, 191, 0.08);
      border: 1px solid rgba(45, 212, 191, 0.22);
      color: var(--accent);
      font-family: var(--mono);
      font-size: 0.76rem;
      font-weight: 600;
      padding: 6px 12px;
      border-radius: 20px;
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .btn {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--border-2);
      color: var(--text);
      font-family: var(--sans);
      font-size: 0.82rem;
      font-weight: 600;
      padding: 8px 14px;
      border-radius: 8px;
      cursor: pointer;
      transition: all 0.15s ease;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }

    .btn:hover {
      background: rgba(255, 255, 255, 0.1);
      border-color: var(--accent);
      color: #fff;
    }

    .btn-primary {
      background: var(--accent);
      color: #070b10;
      border: none;
    }
    .btn-primary:hover {
      background: #5eead4;
      color: #070b10;
    }

    .time-cell {
      font-family: var(--mono);
      font-size: 0.92rem;
      font-weight: 600;
      color: #e2e8f0;
    }

    .time-open {
      color: var(--muted);
      opacity: 0.5;
      font-weight: 500;
    }

    .time-exit {
      color: var(--warn);
    }

    .tables-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px;
      align-items: start;
    }

    @media (max-width: 950px) {
      .tables-grid {
        grid-template-columns: 1fr;
      }
    }

    .section-header {
      display: flex;
      align-items: center;
      gap: 14px;
      margin-top: 32px;
      margin-bottom: 16px;
    }

    .section-header:first-of-type {
      margin-top: 0;
    }

    .section-badge {
      font-family: var(--mono);
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      padding: 6px 14px;
      border-radius: 8px;
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }

    .section-badge-live {
      background: rgba(45, 212, 191, 0.12);
      color: var(--accent);
      border: 1px solid rgba(45, 212, 191, 0.3);
      box-shadow: 0 0 15px rgba(45, 212, 191, 0.08);
    }

    .section-badge-history {
      background: rgba(148, 163, 184, 0.1);
      color: #94a3b8;
      border: 1px solid rgba(148, 163, 184, 0.22);
    }

    .section-line {
      flex: 1;
      height: 1px;
      background: linear-gradient(90deg, var(--border-2), transparent);
    }

    .section-date-indicator {
      font-family: var(--mono);
      font-size: 0.8rem;
      color: var(--muted);
      font-weight: 500;
    }

    .badge-today {
      font-size: 0.68rem;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 4px;
      background: rgba(45, 212, 191, 0.15);
      color: var(--accent);
      border: 1px solid rgba(45, 212, 191, 0.35);
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }

    .badge-today-tp {
      background: rgba(251, 191, 36, 0.15);
      color: var(--warn);
      border-color: rgba(251, 191, 36, 0.35);
    }

    .badge-prev {
      font-size: 0.68rem;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 4px;
      background: rgba(148, 163, 184, 0.12);
      color: #94a3b8;
      border: 1px solid rgba(148, 163, 184, 0.25);
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }

    .history-dot {
      display: inline-block;
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #64748b;
      margin-right: 6px;
    }

    .tp-history-dot {
      background: #d97706;
    }

    .date-cell {
      font-family: var(--mono);
      font-size: 0.86rem;
      color: #94a3b8;
      font-weight: 500;
      letter-spacing: 0.02em;
    }

    .table-wrap-scroll {
      max-height: 480px;
      overflow-y: auto;
    }

    .table-wrap-scroll::-webkit-scrollbar {
      width: 6px;
      height: 6px;
    }

    .table-wrap-scroll::-webkit-scrollbar-track {
      background: rgba(14, 20, 28, 0.5);
    }

    .table-wrap-scroll::-webkit-scrollbar-thumb {
      background: var(--border-2);
      border-radius: 4px;
    }

    .table-wrap-scroll::-webkit-scrollbar-thumb:hover {
      background: var(--muted);
    }

    .tp-dot {
      background: var(--warn);
      box-shadow: 0 0 10px var(--warn);
    }

    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.35);
      overflow: hidden;
    }

    .card-header {
      padding: 16px 20px;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .card-title {
      font-size: 0.95rem;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .card-sub {
      color: var(--muted);
      font-weight: 400;
      font-size: 0.8rem;
    }

    .table-wrap {
      width: 100%;
      overflow-x: auto;
    }

    table.pos-table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
    }

    table.pos-table th {
      padding: 12px 20px;
      font-size: 0.74rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      background: #141b24;
      border-bottom: 1px solid var(--border-2);
    }

    table.pos-table td {
      padding: 16px 20px;
      font-size: 0.92rem;
      border-bottom: 1px solid var(--border);
      vertical-align: middle;
    }

    table.pos-table tbody tr {
      transition: background 0.15s ease;
    }

    table.pos-table tbody tr:hover {
      background: rgba(45, 212, 191, 0.03);
    }

    .ticker-cell {
      font-family: var(--mono);
      font-weight: 700;
      font-size: 1.1rem;
      letter-spacing: 0.02em;
      color: #fff;
    }

    .ticker-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 14px;
      border-radius: 8px;
      font-size: 0.84rem;
      font-weight: 700;
      letter-spacing: 0.02em;
    }

    .badge-call {
      background: rgba(52, 211, 153, 0.15);
      color: var(--up);
      border: 1px solid rgba(52, 211, 153, 0.35);
    }

    .badge-put {
      background: rgba(244, 63, 94, 0.15);
      color: var(--down);
      border: 1px solid rgba(244, 63, 94, 0.35);
    }

    .badge-tp-call, .badge-tp-put {
      background: rgba(251, 191, 36, 0.15);
      color: var(--warn);
      border: 1px solid rgba(251, 191, 36, 0.35);
    }


    .empty-state {
      text-align: center;
      padding: 60px 20px;
      color: var(--muted);
      font-size: 0.95rem;
    }

    .modal {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.75);
      backdrop-filter: blur(4px);
      z-index: 100;
      align-items: center;
      justify-content: center;
    }

    .modal.open { display: flex; }

    .modal-box {
      background: #111a24;
      border: 1px solid var(--border-2);
      border-radius: 14px;
      width: 90%;
      max-width: 500px;
      padding: 24px;
      box-shadow: 0 20px 50px rgba(0, 0, 0, 0.6);
    }

    .modal-title {
      font-size: 1.1rem;
      font-weight: 700;
      margin-bottom: 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .modal-close {
      cursor: pointer;
      color: var(--muted);
      font-size: 1.3rem;
    }

    .form-group {
      margin-bottom: 14px;
    }

    .form-group label {
      display: block;
      font-size: 0.78rem;
      font-weight: 600;
      color: var(--muted);
      text-transform: uppercase;
      margin-bottom: 6px;
    }

    .form-input {
      width: 100%;
      background: #090e15;
      border: 1px solid var(--border-2);
      border-radius: 8px;
      color: var(--text);
      font-family: var(--mono);
      font-size: 0.88rem;
      padding: 10px 12px;
      outline: none;
    }
    .form-input:focus { border-color: var(--accent); }
    select.form-input { cursor: pointer; }
  </style>
</head>
<body>
  <div class="glow"></div>

  <div class="wrap">
    <header class="header">
      <div class="brand">
        <span class="logo-badge">ALPHA</span>
        <div>
          <span class="brand-title">Indicator Signals</span>
          <span class="brand-sub">Real-Time Chart Signals & Telegram Dispatcher</span>
        </div>
      </div>

      <div class="nav-actions">
        <div class="status-chip">
          <span class="pulse-dot"></span>
          <span id="conn-status">Zero-Delay Live</span>
        </div>
        <div class="tz-chip">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg>
          <span id="tz-display">Chicago (CST)</span>
        </div>
        <button class="btn" onclick="openConfigModal()">&#9881; Settings</button>
        <button class="btn" onclick="clearTable()">Clear</button>
      </div>
    </header>

    <!-- SECTION 1: TODAY'S POSITIONS (ALWAYS ON TOP) -->
    <div class="section-header">
      <div class="section-badge section-badge-live">
        <span class="pulse-dot"></span>
        <span>Today's Sessions</span>
      </div>
      <div class="section-line"></div>
      <div class="section-date-indicator" id="today-date-display">Today · Central Time (CST)</div>
    </div>

    <div class="tables-grid">
      <!-- 1. Today's Active Positions Table -->
      <div class="card">
        <div class="card-header">
          <div class="card-title">
            <span class="pulse-dot"></span>
            <span>Active Positions</span>
            <span class="badge-today">Today</span>
            <span class="card-sub" id="today-active-count">0 open</span>
          </div>
          <div class="card-sub" id="today-active-ping">Latest Entries</div>
        </div>

        <div class="table-wrap">
          <table class="pos-table">
            <thead>
              <tr>
                <th style="width: 35%;">Ticker</th>
                <th style="width: 30%;">Play</th>
                <th style="width: 35%; text-align: right;">Entry Time (CST)</th>
              </tr>
            </thead>
            <tbody id="today-active-rows">
            </tbody>
          </table>
        </div>
      </div>

      <!-- 2. Today's Take Profit Table -->
      <div class="card">
        <div class="card-header">
          <div class="card-title">
            <span class="pulse-dot tp-dot"></span>
            <span>Take Profit</span>
            <span class="badge-today badge-today-tp">Today</span>
            <span class="card-sub" id="today-tp-count">0 exited</span>
          </div>
          <div class="card-sub" id="today-tp-ping">Latest Exits</div>
        </div>

        <div class="table-wrap">
          <table class="pos-table">
            <thead>
              <tr>
                <th style="width: 35%;">Ticker</th>
                <th style="width: 30%;">Play</th>
                <th style="width: 35%; text-align: right;">Exit Time (CST)</th>
              </tr>
            </thead>
            <tbody id="today-tp-rows">
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- SECTION 2: PREVIOUS POSITIONS (WITH DATE COLUMN) -->
    <div class="section-header" style="margin-top: 40px;">
      <div class="section-badge section-badge-history">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg>
        <span>Previous Positions</span>
      </div>
      <div class="section-line"></div>
      <div class="section-date-indicator">Prior Sessions · CST</div>
    </div>

    <div class="tables-grid">
      <!-- 3. Previous Active Positions Table -->
      <div class="card">
        <div class="card-header">
          <div class="card-title">
            <span class="history-dot"></span>
            <span>Previous Positions</span>
            <span class="badge-prev">History</span>
            <span class="card-sub" id="prev-active-count">0 open</span>
          </div>
          <div class="card-sub">Past Sessions</div>
        </div>

        <div class="table-wrap table-wrap-scroll">
          <table class="pos-table">
            <thead>
              <tr>
                <th style="width: 25%;">Ticker</th>
                <th style="width: 22%;">Play</th>
                <th style="width: 26%;">Date</th>
                <th style="width: 27%; text-align: right;">Entry Time (CST)</th>
              </tr>
            </thead>
            <tbody id="prev-active-rows">
            </tbody>
          </table>
        </div>
      </div>

      <!-- 4. Previous Take Profit Table -->
      <div class="card">
        <div class="card-header">
          <div class="card-title">
            <span class="history-dot tp-history-dot"></span>
            <span>Previous Take Profit</span>
            <span class="badge-prev">History</span>
            <span class="card-sub" id="prev-tp-count">0 exited</span>
          </div>
          <div class="card-sub">Past Exits</div>
        </div>

        <div class="table-wrap table-wrap-scroll">
          <table class="pos-table">
            <thead>
              <tr>
                <th style="width: 25%;">Ticker</th>
                <th style="width: 22%;">Play</th>
                <th style="width: 26%;">Date</th>
                <th style="width: 27%; text-align: right;">Exit Time (CST)</th>
              </tr>
            </thead>
            <tbody id="prev-tp-rows">
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <div class="modal" id="config-modal">
    <div class="modal-box">
      <div class="modal-title">
        <span>Settings</span>
        <span class="modal-close" onclick="closeConfigModal()">&times;</span>
      </div>
      <div class="form-group">
        <label>Telegram Bot Token ("Indicator Signals")</label>
        <input type="password" id="cfg-token" class="form-input" placeholder="Paste bot token from BotFather">
      </div>
      <div class="form-group">
        <label>Telegram Chat ID</label>
        <input type="text" id="cfg-chat" class="form-input" placeholder="e.g. 123456789">
      </div>
      <div class="form-group">
        <label>Auto Market Scanner</label>
        <select id="cfg-scanner" class="form-input">
          <option value="MARKET_HOURS">Auto (Market Hours 9:30 AM - 4:00 PM EST)</option>
          <option value="ALWAYS_ON">Always ON (24/7)</option>
          <option value="OFF">OFF (Webhook Only)</option>
        </select>
      </div>
      <div class="form-group">
        <label>Signal Mode (TradingView Alignment)</label>
        <select id="cfg-signal-mode" class="form-input">
          <option value="ALL_CONFLUENCE">All Confluence (Pullback + Volume Reversal)</option>
          <option value="PULLBACK_ONLY">Value Zone Pullback Only</option>
          <option value="BREAKOUT_ONLY">Volume Breakout Reversal Only</option>
        </select>
      </div>
      <div class="form-group">
        <label>Scanner Watchlist Tickers</label>
        <input type="text" id="cfg-watchlist" class="form-input" placeholder="AAPL,AMZN,COIN,GOOGL,HOOD,INTC,IWM,META,MRVL,MSFT,MU,NBIS,NFLX,NVDA,PLTR,QQQ,SNDK,SPCX,SPX,SPY,TSLA,UNH,WMT">
      </div>
      <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 18px;">
        <button class="btn" onclick="closeConfigModal()">Cancel</button>
        <button class="btn btn-primary" onclick="saveConfig()">Save & Test</button>
      </div>
    </div>
  </div>

  <script>
    let activePositions = [];
    let tpPositions = [];
    let serverToday = '';

    function renderBadge(play) {
      const p = (play || '').toUpperCase();
      if (p.includes('PUT')) {
        return `<span class="badge badge-put">PUT</span>`;
      }
      return `<span class="badge badge-call">CALL</span>`;
    }

    function getTodayString() {
      if (serverToday) return serverToday;
      try {
        const parts = new Intl.DateTimeFormat('en-US', {
          timeZone: 'America/Chicago',
          year: 'numeric',
          month: '2-digit',
          day: '2-digit'
        }).formatToParts(new Date());
        const y = parts.find(p => p.type === 'year').value;
        const m = parts.find(p => p.type === 'month').value;
        const d = parts.find(p => p.type === 'day').value;
        return `${y}-${m}-${d}`;
      } catch (e) {
        const now = new Date();
        const y = now.getFullYear();
        const m = String(now.getMonth() + 1).padStart(2, '0');
        const d = String(now.getDate()).padStart(2, '0');
        return `${y}-${m}-${d}`;
      }
    }

    function getItemDate(item) {
      if (item.date) return item.date;
      const ts = item.timestamp;
      if (!ts) return getTodayString();
      if (ts.includes('T')) return ts.split('T')[0];
      if (ts.includes(' ')) return ts.split(' ')[0];
      return ts.slice(0, 10);
    }

    function isTodayItem(item) {
      const d = getItemDate(item);
      return d === getTodayString();
    }

    function updateDateDisplay(todayStr) {
      const el = document.getElementById('today-date-display');
      if (!el) return;
      try {
        const parts = (todayStr || getTodayString()).split('-');
        if (parts.length === 3) {
          const d = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
          const dateText = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });
          el.textContent = `${dateText} · Central Time (CST)`;
          return;
        }
      } catch (e) {}
      el.textContent = `${todayStr || 'Today'} · Central Time (CST)`;
    }

    function renderTables() {
      const todayStr = getTodayString();
      updateDateDisplay(todayStr);

      const todayActive = activePositions.filter(isTodayItem);
      const prevActive = activePositions.filter(item => !isTodayItem(item));
      const todayTP = tpPositions.filter(isTodayItem);
      const prevTP = tpPositions.filter(item => !isTodayItem(item));

      // 1. Today's Active Positions Table
      const todayActiveBody = document.getElementById('today-active-rows');
      const todayActiveCount = document.getElementById('today-active-count');
      if (todayActiveCount) todayActiveCount.textContent = `${todayActive.length} open`;

      if (todayActive.length === 0) {
        todayActiveBody.innerHTML = `<tr><td colspan="3" class="empty-state">No active positions open today. Signals will appear here automatically.</td></tr>`;
      } else {
        todayActiveBody.innerHTML = todayActive.map(s => `
          <tr>
            <td class="ticker-cell">
              <div class="ticker-pill"><span>${s.ticker}</span></div>
            </td>
            <td>${renderBadge(s.play)}</td>
            <td class="time-cell" style="text-align: right;">${s.entry_time || '--'}</td>
          </tr>
        `).join('');
      }

      // 2. Today's Take Profit Table
      const todayTPBody = document.getElementById('today-tp-rows');
      const todayTPCount = document.getElementById('today-tp-count');
      if (todayTPCount) todayTPCount.textContent = `${todayTP.length} exited`;

      if (todayTP.length === 0) {
        todayTPBody.innerHTML = `<tr><td colspan="3" class="empty-state">No take profits recorded today. Exits will appear here automatically.</td></tr>`;
      } else {
        todayTPBody.innerHTML = todayTP.map(s => `
          <tr>
            <td class="ticker-cell">
              <div class="ticker-pill"><span>${s.ticker}</span></div>
            </td>
            <td>${renderBadge(s.play)}</td>
            <td class="time-cell time-exit" style="text-align: right;">${s.exit_time || '--'}</td>
          </tr>
        `).join('');
      }

      // 3. Previous Active Positions Table (with Date column)
      const prevActiveBody = document.getElementById('prev-active-rows');
      const prevActiveCount = document.getElementById('prev-active-count');
      if (prevActiveCount) prevActiveCount.textContent = `${prevActive.length} open`;

      if (prevActive.length === 0) {
        prevActiveBody.innerHTML = `<tr><td colspan="4" class="empty-state">No previous active positions recorded.</td></tr>`;
      } else {
        prevActiveBody.innerHTML = prevActive.map(s => `
          <tr>
            <td class="ticker-cell">
              <div class="ticker-pill"><span>${s.ticker}</span></div>
            </td>
            <td>${renderBadge(s.play)}</td>
            <td class="date-cell">${getItemDate(s)}</td>
            <td class="time-cell" style="text-align: right;">${s.entry_time || '--'}</td>
          </tr>
        `).join('');
      }

      // 4. Previous Take Profit Table (with Date column)
      const prevTPBody = document.getElementById('prev-tp-rows');
      const prevTPCount = document.getElementById('prev-tp-count');
      if (prevTPCount) prevTPCount.textContent = `${prevTP.length} exited`;

      if (prevTP.length === 0) {
        prevTPBody.innerHTML = `<tr><td colspan="4" class="empty-state">No previous take profits recorded.</td></tr>`;
      } else {
        prevTPBody.innerHTML = prevTP.map(s => `
          <tr>
            <td class="ticker-cell">
              <div class="ticker-pill"><span>${s.ticker}</span></div>
            </td>
            <td>${renderBadge(s.play)}</td>
            <td class="date-cell">${getItemDate(s)}</td>
            <td class="time-cell time-exit" style="text-align: right;">${s.exit_time || '--'}</td>
          </tr>
        `).join('');
      }
    }

    async function loadInitial() {
      try {
        const res = await fetch('/api/signals');
        const data = await res.json();
        if (data.today) serverToday = data.today;
        activePositions = data.active_positions || [];
        tpPositions = data.tp_positions || [];
        renderTables();
      } catch (e) {
        console.error("Failed to load positions:", e);
      }
    }

    function connectSSE() {
      const es = new EventSource('/api/stream');
      const connStatus = document.getElementById('conn-status');

      es.onopen = () => {
        connStatus.textContent = 'Zero-Delay Live';
      };

      es.onmessage = (event) => {
        try {
          const item = JSON.parse(event.data);
          if (item) {
            if (item.today) serverToday = item.today;
            if (item.active_positions !== undefined) activePositions = item.active_positions;
            if (item.tp_positions !== undefined) tpPositions = item.tp_positions;
            renderTables();
            if (item.latest) {
              const isExit = item.latest.exit_time && item.latest.exit_time !== '--';
              const isItemToday = isTodayItem(item.latest);
              if (isItemToday) {
                const pingId = isExit ? 'today-tp-ping' : 'today-active-ping';
                const el = document.getElementById(pingId);
                if (el) el.textContent = `${item.latest.ticker} ${item.latest.play}`;
              }
            }
          }
        } catch (e) {
          console.error("SSE parse error", e);
        }
      };

      es.onerror = () => {
        connStatus.textContent = 'Reconnecting...';
        es.close();
        setTimeout(connectSSE, 2000);
      };
    }

    async function clearTable() {
      if (!confirm("Clear active positions and take profit history?")) return;
      await fetch('/api/clear', { method: 'POST' });
      activePositions = [];
      tpPositions = [];
      renderTables();
    }

    function openConfigModal() {
      fetch('/api/config')
        .then(r => r.json())
        .then(d => {
          document.getElementById('cfg-token').value = d.telegram_bot_token || '';
          document.getElementById('cfg-chat').value = d.telegram_chat_id || '';
          document.getElementById('cfg-scanner').value = d.scanner_mode || 'MARKET_HOURS';
          document.getElementById('cfg-signal-mode').value = d.signal_mode || 'ALL_CONFLUENCE';
          document.getElementById('cfg-watchlist').value = d.watchlist || 'AAPL,AMZN,COIN,GOOGL,HOOD,INTC,IWM,META,MRVL,MSFT,MU,NBIS,NFLX,NVDA,PLTR,QQQ,SNDK,SPCX,SPX,SPY,TSLA,UNH,WMT';
          document.getElementById('config-modal').classList.add('open');
        });
    }

    function closeConfigModal() {
      document.getElementById('config-modal').classList.remove('open');
    }

    async function saveConfig() {
      const token = document.getElementById('cfg-token').value.trim();
      const chat = document.getElementById('cfg-chat').value.trim();
      const scanner = document.getElementById('cfg-scanner').value.trim();
      const signalMode = document.getElementById('cfg-signal-mode').value.trim();
      const watchlist = document.getElementById('cfg-watchlist').value.trim();

      const res = await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          telegram_bot_token: token,
          telegram_chat_id: chat,
          scanner_mode: scanner,
          signal_mode: signalMode,
          watchlist: watchlist
        })
      });
      const data = await res.json();
      if (data.test_message_sent) {
        alert("Settings saved! A test message was sent to Telegram.");
      } else {
        alert("Settings saved!");
      }
      closeConfigModal();
    }

    loadInitial();
    connectSSE();
  </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return DASHBOARD_HTML

if __name__ == "__main__":
    import uvicorn
    port = int(load_env_var("PORT", "8800"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)
