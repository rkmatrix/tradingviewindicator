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
    market_close = now.replace(hour=15, minute=6, second=0, microsecond=0)   # a few minutes past the close so the 14:55 bar is still evaluated
    return market_open <= now <= market_close

# --- Automated Market Hours Scanner (1-to-1 Pine Script v5 Engine) ---
scanner_state: Dict[str, Dict] = {} # ticker -> { "last_evaluated_bar": None, "last_dispatched_sig": None }

# -----------------------------------------------------------------------------
# These settings MUST mirror the inputs of MultiConfluence_Signal_Indicator.pine.
# Defaults below equal the indicator defaults. If you change an input on the
# TradingView chart, mirror it in data/pine_settings.json (same keys) so the
# Telegram alerts keep firing exactly when the chart prints a flag.
# -----------------------------------------------------------------------------
PINE_DEFAULTS: Dict = {
    # Signal Engine
    "cooldown_bars": 0,
    # Trade Lifecycle & Exits
    "use_dyn_tp": True, "exit_on_sl": True, "exit_on_tp2": True, "be_after_tp1": True, "max_hold_bars": 0,
    # 1. Trend & Baseline
    "ema_fast": 9, "ema_med": 21, "ema_slow": 50, "ema_base": 200,
    "filter_200": "FLEXIBLE",          # FLEXIBLE | STRICT | DISABLED
    # 1b. Higher-Timeframe Bias
    "use_htf": False, "htf_minutes": 60, "htf_ema_len": 50,
    # 2. Momentum
    "use_rsi": True, "rsi_len": 14, "rsi_bull_buy": 45, "rsi_bear_sell": 55, "bo_rsi_call": 46, "bo_rsi_put": 54,
    "use_macd": True, "macd_fast": 12, "macd_slow": 26, "macd_signal": 9,
    "use_resume": True, "resume_bars": 3,
    # 2b. ADX Regime
    "use_adx": False, "adx_len": 14, "adx_smooth": 14, "adx_min": 20.0,
    # 3. Volatility, Value Zone & Targets
    "atr_len": 14, "sl_atr": 1.2, "tp1_atr": 1.5, "tp2_atr": 2.5,
    "zone_mode": "ATR",                # ATR | PERCENT
    "zone_tol_pct": 1.2, "zone_depth_pct": 2.5, "zone_tol_atr": 0.5, "zone_depth_atr": 1.0,
    "fast_zone": True, "fast_touch_atr": 0.15,
    # 4. Volume
    "use_vol_filter": True, "vol_baseline": "MEDIAN",   # MEDIAN | SMA
    "vol_len": 20, "vol_mult": 0.80, "vol_break_mult": 1.10,
    # 4b. Session (exchange time, HHMM-HHMM)
    "use_session": False, "session": "0930-1600",
    # Exchange time zone used for the session filter and HTF bar alignment (TradingView uses the symbol's exchange tz)
    "exchange_tz": "America/New_York",
}
PINE_SETTINGS_FILE = DATA_DIR / "pine_settings.json"
_pine_settings_cache: Dict = {"mtime": None, "settings": dict(PINE_DEFAULTS)}

def load_pine_settings() -> Dict:
    """Indicator defaults, optionally overridden by data/pine_settings.json (hot-reloaded)."""
    try:
        mtime = PINE_SETTINGS_FILE.stat().st_mtime if PINE_SETTINGS_FILE.exists() else None
    except OSError:
        mtime = None
    if mtime != _pine_settings_cache["mtime"]:
        s = dict(PINE_DEFAULTS)
        if mtime is not None:
            try:
                user = json.loads(PINE_SETTINGS_FILE.read_text(encoding="utf-8"))
                unknown = [k for k in user if k not in PINE_DEFAULTS]
                if unknown:
                    log.warning("pine_settings.json: ignoring unknown keys %s", unknown)
                s.update({k: v for k, v in user.items() if k in PINE_DEFAULTS})
                log.info("Loaded Pine settings overrides from %s", PINE_SETTINGS_FILE)
            except Exception as e:
                log.warning("Could not read %s (%s) - using indicator defaults", PINE_SETTINGS_FILE, e)
        _pine_settings_cache["mtime"] = mtime
        _pine_settings_cache["settings"] = s
    return _pine_settings_cache["settings"]

# ---- Pine-exact indicator math ----------------------------------------------
def _pine_recursive(src: pd.Series, length: int, alpha: float) -> pd.Series:
    """Pine ta.ema / ta.rma: seeded with the SMA of the first `length` valid values, then recursive."""
    v = src.to_numpy(dtype=float)
    n = len(v)
    out = np.full(n, np.nan)
    start = None
    for i in range(length - 1, n):
        w = v[i - length + 1:i + 1]
        if not np.isnan(w).any():
            out[i] = w.mean()
            start = i
            break
    if start is None:
        return pd.Series(out, index=src.index)
    prev = out[start]
    for i in range(start + 1, n):
        x = v[i]
        if not np.isnan(x):
            prev = alpha * x + (1.0 - alpha) * prev
        out[i] = prev
    return pd.Series(out, index=src.index)

def pine_ema(src: pd.Series, length: int) -> pd.Series:
    return _pine_recursive(src, length, 2.0 / (length + 1))

def pine_rma(src: pd.Series, length: int) -> pd.Series:
    return _pine_recursive(src, length, 1.0 / length)

def pine_rsi(src: pd.Series, length: int) -> pd.Series:
    chg = src.diff()
    up = pine_rma(chg.clip(lower=0), length).to_numpy()
    dn = pine_rma((-chg).clip(lower=0), length).to_numpy()
    out = np.full(len(up), np.nan)
    for i in range(len(up)):
        u, d = up[i], dn[i]
        if np.isnan(u) or np.isnan(d):
            continue
        out[i] = 100.0 if d == 0 else (0.0 if u == 0 else 100.0 - 100.0 / (1.0 + u / d))
    return pd.Series(out, index=src.index)

def pine_tr(h: pd.Series, l: pd.Series, c: pd.Series) -> pd.Series:
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    tr.iloc[0] = h.iloc[0] - l.iloc[0]   # ta.tr(true) on the first bar
    return tr

def pine_dmi(h: pd.Series, l: pd.Series, c: pd.Series, di_len: int, adx_smooth: int):
    up = h.diff()
    dn = -l.diff()
    plus_dm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=h.index).where(up.notna())
    minus_dm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=h.index).where(dn.notna())
    trur = pine_rma(pine_tr(h, l, c), di_len)
    plus = (100 * pine_rma(plus_dm, di_len) / trur).ffill()
    minus = (100 * pine_rma(minus_dm, di_len) / trur).ffill()
    s = plus + minus
    adx = 100 * pine_rma((plus - minus).abs() / s.where(s != 0, 1.0), adx_smooth)
    return plus, minus, adx

def _crossover(a_now, a_prev, b_now, b_prev) -> bool:
    return (not np.isnan(a_prev)) and (not np.isnan(b_prev)) and a_now > b_now and a_prev <= b_prev

def _crossunder(a_now, a_prev, b_now, b_prev) -> bool:
    return (not np.isnan(a_prev)) and (not np.isnan(b_prev)) and a_now < b_now and a_prev >= b_prev

def _parse_session(sess: str):
    try:
        a, b = sess.split("-")
        return (int(a[:2]) * 60 + int(a[2:4]), int(b[:2]) * 60 + int(b[2:4]))
    except Exception:
        return (9 * 60 + 30, 16 * 60)

def _htf_series(df: pd.DataFrame, minutes: int, ema_len: int):
    """
    request.security(tf, ema(close, len)[1] / close[1], lookahead_on): for each chart bar, the values of the
    LAST COMPLETED higher-timeframe bar. HTF buckets are anchored to 09:30 exchange time like TradingView.
    """
    rule = f"{int(minutes)}min"
    htf = df["Close"].resample(rule, label="left", closed="left", origin="start_day", offset="9h30min").last().dropna()
    htf_ema = pine_ema(htf, ema_len).shift(1)
    htf_close = htf.shift(1)
    bucket = (df.index - pd.Timedelta("9h30min")).floor(rule) + pd.Timedelta("9h30min")
    return htf_ema.reindex(bucket).to_numpy(), htf_close.reindex(bucket).to_numpy()

def evaluate_pine_indicator(df: pd.DataFrame, settings: Optional[Dict] = None) -> Optional[Dict]:
    """
    Line-by-line Python port of MultiConfluence_Signal_Indicator.pine (TradingView v5).
    Replays the full engine (dual-engine confluence + trade lifecycle state machine) over closed bars
    and reports the events (TP_CALL / TP_PUT / CALL / PUT) that printed on the most recent closed bar.
    """
    S = settings or load_pine_settings()
    if df is None or len(df) < max(S["ema_base"], 60):
        return None
    if getattr(df.index, "tz", None) is not None:
        df = df.tz_convert(S["exchange_tz"])   # yfinance may return UTC; Pine sessions / HTF bars are exchange-local

    c = df["Close"].astype(float)
    o = df["Open"].astype(float)
    h = df["High"].astype(float)
    l = df["Low"].astype(float)
    v = df["Volume"].astype(float) if "Volume" in df.columns else pd.Series(np.nan, index=df.index)

    # --- 2. INDICATOR CORE CALCULATIONS ---
    ema_fast = pine_ema(c, S["ema_fast"]).to_numpy()
    ema_med = pine_ema(c, S["ema_med"]).to_numpy()
    ema_slow = pine_ema(c, S["ema_slow"]).to_numpy()
    ema_base = pine_ema(c, S["ema_base"]).to_numpy()
    rsi = pine_rsi(c, S["rsi_len"]).to_numpy()
    macd_line = pine_ema(c, S["macd_fast"]) - pine_ema(c, S["macd_slow"])
    hist = (macd_line - pine_ema(macd_line, S["macd_signal"])).to_numpy()
    atr = pine_rma(pine_tr(h, l, c), S["atr_len"]).to_numpy()
    if S["use_adx"]:
        _, _, adx_s = pine_dmi(h, l, c, S["adx_len"], S["adx_smooth"])
        adx = adx_s.to_numpy()
    else:
        adx = np.full(len(c), np.nan)

    vol_sma = v.rolling(S["vol_len"]).mean()
    vol_med = v.rolling(S["vol_len"]).median()
    vol_base = (vol_sma if str(S["vol_baseline"]).upper().startswith("SMA") else vol_med).to_numpy()
    vol = v.to_numpy()

    if S["use_htf"]:
        htf_ema, htf_close = _htf_series(df, S["htf_minutes"], S["htf_ema_len"])
    else:
        htf_ema = htf_close = None

    sess_start, sess_end = _parse_session(S["session"])
    idx = df.index
    tod = (idx.hour * 60 + idx.minute) if S["use_session"] else None

    cn, on, hn, ln = c.to_numpy(), o.to_numpy(), h.to_numpy(), l.to_numpy()

    # --- Closed-bar boundary: the latest yfinance row is the bar that is still forming ---
    now = datetime.now(timezone.utc)
    last_bar_time = idx[-1]
    bar_utc = last_bar_time.astimezone(timezone.utc) if last_bar_time.tzinfo is not None else last_bar_time.replace(tzinfo=timezone.utc)
    closed_end_idx = len(df) - 1 if (now - bar_utc).total_seconds() < 310 else len(df)
    if closed_end_idx < max(S["ema_base"], 60):
        return None

    sig_mode = load_env_var("SIGNAL_MODE", SIGNAL_MODE).upper()
    strict200 = str(S["filter_200"]).upper().startswith("STRICT")
    use_rsi, use_macd = bool(S["use_rsi"]), bool(S["use_macd"])
    no_mom_filter = not use_rsi and not use_macd
    zone_atr = str(S["zone_mode"]).upper() == "ATR"

    # --- 4. TRADE LIFECYCLE STATE (var) ---
    pos_dir = 0
    entry_px = sl_level = tp1_level = tp2_level = np.nan
    tp1_done = False
    entry_bar = None
    last_exit_bar = None
    engine_used = ""
    last_bull_touch = None   # for ta.barssince(bullTouch)
    last_bear_touch = None

    all_signals: List[Dict] = []

    for i in range(1, closed_end_idx):
        close, open_, high, low = cn[i], on[i], hn[i], ln[i]
        ef, em, es, eb = ema_fast[i], ema_med[i], ema_slow[i], ema_base[i]
        r, hl, a = rsi[i], hist[i], atr[i]
        if any(np.isnan(x) for x in (ef, em, es, r, hl, a)) or (strict200 and np.isnan(eb)):
            continue
        hl_prev, r_prev = hist[i - 1], rsi[i - 1]

        # Volume (gracefully degrades on symbols with no volume feed)
        vb = vol_base[i]
        has_volume = (not np.isnan(vol[i])) and (not np.isnan(vb)) and vb > 0
        vol_ok_pull = (not S["use_vol_filter"]) or (not has_volume) or vol[i] >= vb * S["vol_mult"]
        vol_ok_break = (not S["use_vol_filter"]) or (not has_volume) or vol[i] >= vb * S["vol_break_mult"]

        # Higher-timeframe bias
        if htf_ema is not None:
            he, hc = htf_ema[i], htf_close[i]
            htf_ok_call = (not np.isnan(he)) and (not np.isnan(hc)) and hc > he
            htf_ok_put = (not np.isnan(he)) and (not np.isnan(hc)) and hc < he
        else:
            htf_ok_call = htf_ok_put = True

        # Session / regime gates (bar-close gate is implicit: only closed bars are replayed)
        in_session = (not S["use_session"]) or (sess_start <= tod[i] < sess_end)
        adx_ok = (not S["use_adx"]) or ((not np.isnan(adx[i])) and adx[i] >= S["adx_min"])
        gate_ok = in_session and adx_ok

        # Cross events
        rsi_bull_turn = _crossover(r, r_prev, S["rsi_bull_buy"], S["rsi_bull_buy"])
        rsi_bear_turn = _crossunder(r, r_prev, S["rsi_bear_sell"], S["rsi_bear_sell"])
        macd_bull_turn = _crossover(hl, hl_prev, 0.0, 0.0)
        macd_bear_turn = _crossunder(hl, hl_prev, 0.0, 0.0)
        close_over_fast = _crossover(close, cn[i - 1], ef, ema_fast[i - 1])
        close_under_fast = _crossunder(close, cn[i - 1], ef, ema_fast[i - 1])

        # --- 3. DUAL-ENGINE CONFLUENCE EVALUATION ---
        pass200_call = (not strict200) or close > eb
        pass200_put = (not strict200) or close < eb

        bull_trend = (close > eb if strict200 else close > es) and em > es
        bear_trend = (close < eb if strict200 else close < es) and em < es

        zone_reach = a * S["zone_tol_atr"] if zone_atr else em * S["zone_tol_pct"] / 100
        zone_depth = a * S["zone_depth_atr"] if zone_atr else es * S["zone_depth_pct"] / 100
        fast_touch = a * S["fast_touch_atr"]

        bull_touch = low <= em + zone_reach or (S["fast_zone"] and low <= ef + fast_touch)
        bear_touch = high >= em - zone_reach or (S["fast_zone"] and high >= ef - fast_touch)
        if bull_touch:
            last_bull_touch = i
        if bear_touch:
            last_bear_touch = i
        bull_depth_ok = close >= es - zone_depth
        bear_depth_ok = close <= es + zone_depth
        bull_dip = bull_touch and bull_depth_ok
        bear_rally = bear_touch and bear_depth_ok

        bull_turn = no_mom_filter or (use_rsi and rsi_bull_turn) or (use_macd and macd_bull_turn)
        bear_turn = no_mom_filter or (use_rsi and rsi_bear_turn) or (use_macd and macd_bear_turn)

        bull_touched_recently = last_bull_touch is not None and (i - last_bull_touch) < S["resume_bars"]
        bear_touched_recently = last_bear_touch is not None and (i - last_bear_touch) < S["resume_bars"]
        bull_resume = S["use_resume"] and bull_touched_recently and bull_depth_ok and close > ef and close > open_ and (not use_rsi or r > 50) and (not use_macd or hl > 0)
        bear_resume = S["use_resume"] and bear_touched_recently and bear_depth_ok and close < ef and close < open_ and (not use_rsi or r < 50) and (not use_macd or hl < 0)

        pullback_call = bull_trend and pass200_call and vol_ok_pull and ((bull_dip and bull_turn) or bull_resume)
        pullback_put = bear_trend and pass200_put and vol_ok_pull and ((bear_rally and bear_turn) or bear_resume)

        # Engine B: Volume Breakout / V-Reversal through the 9 & 21 EMA
        was_below_call = cn[i - 1] <= ema_fast[i - 1] or cn[i - 1] <= ema_med[i - 1] or open_ <= ef or open_ <= em
        was_above_put = cn[i - 1] >= ema_fast[i - 1] or cn[i - 1] >= ema_med[i - 1] or open_ >= ef or open_ >= em
        bo_macd_call = (not use_macd) or (hl > 0 and hl > hl_prev)
        bo_macd_put = (not use_macd) or (hl < 0 and hl < hl_prev)
        bo_rsi_ok_call = (not use_rsi) or r >= S["bo_rsi_call"]
        bo_rsi_ok_put = (not use_rsi) or r <= S["bo_rsi_put"]

        breakout_call = was_below_call and close > ef and close > em and close > open_ and vol_ok_break and bo_macd_call and bo_rsi_ok_call and pass200_call
        breakout_put = was_above_put and close < ef and close < em and close < open_ and vol_ok_break and bo_macd_put and bo_rsi_ok_put and pass200_put

        if "PULLBACK" in sig_mode:
            mode_call, mode_put = pullback_call, pullback_put
        elif "BREAKOUT" in sig_mode:
            mode_call, mode_put = breakout_call, breakout_put
        else:
            mode_call, mode_put = (pullback_call or breakout_call), (pullback_put or breakout_put)

        raw_call_pre = gate_ok and htf_ok_call and mode_call
        raw_put_pre = gate_ok and htf_ok_put and mode_put
        conflict = raw_call_pre and raw_put_pre
        raw_call = raw_call_pre and not conflict
        raw_put = raw_put_pre and not conflict

        # --- 4. TRADE LIFECYCLE STATE MACHINE ---
        in_call = pos_dir == 1
        in_put = pos_dir == -1
        after_entry = entry_bar is not None and i > entry_bar

        call_sl_hit = in_call and after_entry and S["exit_on_sl"] and low <= sl_level
        call_tp1_hit = in_call and after_entry and not tp1_done and high >= tp1_level
        call_tp2_hit = in_call and after_entry and S["exit_on_tp2"] and high >= tp2_level
        call_dyn_tp = in_call and after_entry and S["use_dyn_tp"] and (close_under_fast or macd_bear_turn)
        call_time_up = in_call and after_entry and S["max_hold_bars"] > 0 and i - entry_bar >= S["max_hold_bars"]

        put_sl_hit = in_put and after_entry and S["exit_on_sl"] and high >= sl_level
        put_tp1_hit = in_put and after_entry and not tp1_done and low <= tp1_level
        put_tp2_hit = in_put and after_entry and S["exit_on_tp2"] and low <= tp2_level
        put_dyn_tp = in_put and after_entry and S["use_dyn_tp"] and (close_over_fast or macd_bull_turn)
        put_time_up = in_put and after_entry and S["max_hold_bars"] > 0 and i - entry_bar >= S["max_hold_bars"]

        call_exit_code = (6 if tp1_done else 3) if call_sl_hit else 2 if call_tp2_hit else 1 if call_dyn_tp else 4 if call_time_up else 0
        put_exit_code = (6 if tp1_done else 3) if put_sl_hit else 2 if put_tp2_hit else 1 if put_dyn_tp else 4 if put_time_up else 0

        cooldown_ok = S["cooldown_bars"] == 0 or last_exit_bar is None or i - last_exit_bar >= S["cooldown_bars"]
        call_signal = raw_call and not in_call and cooldown_ok
        put_signal = raw_put and not in_put and cooldown_ok

        exit_call = in_call and (call_exit_code > 0 or put_signal)
        exit_put = in_put and (put_exit_code > 0 or call_signal)
        exit_code = (call_exit_code if call_exit_code > 0 else 5) if exit_call else (put_exit_code if put_exit_code > 0 else 5) if exit_put else 0

        exit_px = np.nan
        stop_exit = exit_code in (3, 6)
        if exit_call:
            exit_px = min(open_, sl_level) if stop_exit else max(open_, tp2_level) if exit_code == 2 else close
        if exit_put:
            exit_px = max(open_, sl_level) if stop_exit else min(open_, tp2_level) if exit_code == 2 else close

        exit_pnl = np.nan
        if exit_call:
            exit_pnl = (exit_px - entry_px) / entry_px * 100
        if exit_put:
            exit_pnl = (entry_px - exit_px) / entry_px * 100

        if call_tp1_hit or put_tp1_hit:
            tp1_done = True
            if S["be_after_tp1"] and not exit_call and not exit_put:
                sl_level = entry_px

        t = str(idx[i])
        bar_events: List[Dict] = []
        # Exits before entries so a reversal arrives as TP_x followed by BUY_y (same as the Pine alert() path)
        if exit_call or exit_put:
            bar_events.append({"bar_time": t, "type": "TP_CALL" if exit_call else "TP_PUT", "price": float(exit_px),
                               "exit_code": exit_code, "pnl_pct": None if np.isnan(exit_pnl) else round(float(exit_pnl), 2),
                               "engine": engine_used, "bar_idx": i})
            pos_dir = 0
            last_exit_bar = i
            entry_bar = None

        if call_signal or put_signal:
            pos_dir = 1 if call_signal else -1
            entry_px = close
            entry_bar = i
            tp1_done = False
            engine_used = "PULLBACK" if (pullback_call if call_signal else pullback_put) else "BREAKOUT"
            sgn = 1 if call_signal else -1
            sl_level = close - sgn * a * S["sl_atr"]
            tp1_level = close + sgn * a * S["tp1_atr"]
            tp2_level = close + sgn * a * S["tp2_atr"]
            bar_events.append({"bar_time": t, "type": "CALL" if call_signal else "PUT", "price": float(close),
                               "sl": float(sl_level), "tp1": float(tp1_level), "tp2": float(tp2_level),
                               "engine": engine_used, "bar_idx": i})

        all_signals.extend(bar_events)

    last_closed_bar_idx = closed_end_idx - 1
    last_closed_bar_time = str(idx[last_closed_bar_idx])
    events = [s for s in all_signals if s["bar_idx"] == last_closed_bar_idx]

    return {
        "last_closed_bar_time": last_closed_bar_time,
        "position": "CALL" if pos_dir == 1 else ("PUT" if pos_dir == -1 else "FLAT"),
        "entry_price": None if np.isnan(entry_px) or pos_dir == 0 else float(entry_px),
        "new_events": events,
        "new_event": events[-1] if events else None,
        "latest_signal": all_signals[-1] if all_signals else None,
        "signals": all_signals,
    }

minute_cache: Dict = {"tickers": None, "fetched": float("-inf"), "data": None}
state_fixed_log: Dict[str, str] = {}

def _extract_ticker(data: Optional[pd.DataFrame], yf_tk: str, single: bool) -> Optional[pd.DataFrame]:
    if data is None or data.empty:
        return None
    if isinstance(data.columns, pd.MultiIndex):
        return data.xs(yf_tk, level=1, axis=1) if yf_tk in data.columns.levels[1] else None
    return data if single else None

PHANTOM_VOL_MULT = 30.0     # a 1m print this many times its neighbourhood median is a Yahoo artefact
PHANTOM_VOL_WINDOW = 31     # centred 1m window used for that median

def strip_phantom_volume(df5: pd.DataFrame, df1: Optional[pd.DataFrame]) -> tuple[pd.DataFrame, List[str]]:
    """
    Yahoo's intraday feed injects isolated 1-minute volume prints of 100-500x normal (e.g. SPY 7.5M shares in a
    minute surrounded by ~30k) that do not exist on TradingView. They fake a volume surge and make the scanner fire
    signals the chart never prints. Subtract each phantom print's excess over its local median from its 5m bar.
    The first and last 5 minutes of the session are left alone because genuine auction volume lives there.
    """
    if df1 is None or df1.empty or "Volume" not in df1.columns:
        return df5, []
    v1 = df1["Volume"].astype(float).dropna()
    if v1.empty:
        return df5, []
    med = v1.rolling(PHANTOM_VOL_WINDOW, center=True, min_periods=5).median()
    local = v1.index.tz_convert(load_pine_settings()["exchange_tz"]) if v1.index.tz is not None else v1.index
    mins = local.hour * 60 + local.minute
    edge = (mins < 9 * 60 + 35) | (mins >= 15 * 60 + 55)
    spike = (med > 0) & (v1 > med * PHANTOM_VOL_MULT) & ~edge
    if not spike.any():
        return df5, []
    excess = (v1 - med).where(spike, 0.0)
    excess5 = excess.groupby(excess.index.floor("5min")).sum()
    excess5 = excess5[excess5 > 0]
    hit = excess5.reindex(df5.index).fillna(0.0)
    if not (hit > 0).any():
        return df5, []
    out = df5.copy()
    out["Volume"] = (out["Volume"].astype(float) - hit).clip(lower=0.0)
    return out, [str(t) for t in hit[hit > 0].index]

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
                # Yahoo's ^SPX feed is 15 minutes delayed; ^GSPC is the same index in real time
                yf_map = {tk: ("^GSPC" if tk in ("SPX", "^SPX") else tk) for tk in wl}
                tickers_str = " ".join(list(dict.fromkeys(yf_map.values())))

                # 30 days of 5m candles (~2,300 bars): lets the 200 EMA, RMA-based RSI / ATR and the trade state
                # machine fully converge to what the TradingView chart shows (5 days was not enough warm-up).
                data = await asyncio.to_thread(yf.download, tickers=tickers_str, period="30d", interval="5m", progress=False, auto_adjust=False)

                # 1m data (Yahoo keeps ~7 days) is only used to scrub phantom volume prints; refreshed at most once a minute
                now_ts = asyncio.get_running_loop().time()
                if minute_cache["tickers"] != tickers_str or now_ts - minute_cache["fetched"] >= 60:
                    try:
                        minute_cache["data"] = await asyncio.to_thread(yf.download, tickers=tickers_str, period="7d", interval="1m", progress=False, auto_adjust=False)
                        minute_cache["tickers"] = tickers_str
                        minute_cache["fetched"] = now_ts
                    except Exception as e:
                        log.warning("1m volume download failed (%s) - using raw 5m volume", e)

                if data is not None and not data.empty:
                    single = len(dict.fromkeys(yf_map.values())) == 1
                    for tk in wl:
                        try:
                            yf_tk = yf_map[tk]
                            df = _extract_ticker(data, yf_tk, single)
                            if df is None or df.empty:
                                continue

                            df = df.dropna(subset=["Open", "High", "Low", "Close"]).copy()
                            df, fixed = strip_phantom_volume(df, _extract_ticker(minute_cache["data"], yf_tk, single))
                            if fixed and state_fixed_log.get(tk) != fixed[-1]:
                                state_fixed_log[tk] = fixed[-1]
                                log.info("Scrubbed phantom Yahoo volume on %s bars: %s", tk, ", ".join(f[5:16] for f in fixed[-3:]))
                            result = evaluate_pine_indicator(df)
                            if result is None:
                                continue

                            closed_bar_time = result['last_closed_bar_time']
                            state = scanner_state.setdefault(tk, {"last_evaluated_bar": None, "last_dispatched_sig": None})

                            if state.get("last_evaluated_bar") == closed_bar_time:
                                continue # This confirmed bar was already evaluated

                            state["last_evaluated_bar"] = closed_bar_time

                            # Every flag the chart printed on this closed bar, in chart order
                            # (a reversal is TP_CALL/TP_PUT followed by the new CALL/PUT, like the Pine alert() path)
                            for sig in result['new_events']:
                                sig_key = f"{sig['type']}_{sig['bar_time']}"
                                if state.get("last_dispatched_sig") == sig_key:
                                    continue
                                state["last_dispatched_sig"] = sig_key
                                log.info("Pine Indicator Signal Confirmed: %s %s at %.2f (Bar: %s, engine=%s%s)",
                                         sig['type'], tk, sig['price'], sig['bar_time'], sig.get('engine'),
                                         f", exit_code={sig['exit_code']}" if 'exit_code' in sig else "")
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
