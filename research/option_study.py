"""
Option-premium study of the chart trades: same entries as the chart, priced on real option prints.

    python research/option_study.py

Needs Alpaca keys (APCA_API_KEY_ID / APCA_API_SECRET_KEY) in the environment or in the SignalValidator .env
(ALPACA_ENV_FILE, default C:/Projects/trading/SignalValidator/.env). Free keys get historical 1m option trade bars
(not same-day; no historical quotes), so fills are pessimistic proxies:
    buy  = HIGH of the first 1m option bar at/after the decision minute
    sell = LOW  of the first 1m option bar at/after the exit minute
A trade with no option print within 3 minutes of the decision is dropped from that variant.
"""
import os
import pickle
import sys
import time
import warnings
from datetime import date, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "research"))
import numpy as np
import pandas as pd
import requests
from exit_study import engine_trades
from exit_study_1m import load_1m
from tune import load_data

CACHE = ROOT / "research" / "cache_opt_bars.pkl"
# "hl": buy at the bar high, sell at the bar low (pessimistic).
# "vw": bar VWAP +/- HALF_SPREAD (live paper fills on 2026-10-20 showed 1-3% full spreads).
FILL = (sys.argv[1] if len(sys.argv) > 1 else "hl").lower()
HALF_SPREAD = 0.01


def buy_px(b):
    return float(b["h"]) if FILL == "hl" else float(b["vw"]) * (1 + HALF_SPREAD)


def sell_px(b):
    return float(b["l"]) if FILL == "hl" else float(b["vw"]) * (1 - HALF_SPREAD)
FLAT_MIN = 15 * 60 + 50
NY = "America/New_York"


def _keys():
    k, s = os.getenv("APCA_API_KEY_ID"), os.getenv("APCA_API_SECRET_KEY")
    if not (k and s):
        from dotenv import dotenv_values
        env = dotenv_values(os.getenv("ALPACA_ENV_FILE", "C:/Projects/trading/SignalValidator/.env"))
        k, s = env.get("APCA_API_KEY_ID"), env.get("APCA_API_SECRET_KEY")
    return {"APCA-API-KEY-ID": k or "", "APCA-API-SECRET-KEY": s or ""}


def occ(tk, exp: date, cp, strike):
    return f"{tk}{exp:%y%m%d}{cp}{int(round(strike * 1000)):08d}"


def target_strike(px, side, itm_steps):
    """ATM, or ITM by 0.5% of the price per step."""
    return px * (1 - 0.005 * itm_steps) if side == "C" else px * (1 + 0.005 * itm_steps)


def strikes_near(px, side, itm_steps):
    want = target_strike(px, side, itm_steps)
    out = set()
    for step in (0.5, 1.0, 2.5, 5.0):
        base = round(want / step) * step
        out.update(round(base + step * k, 2) for k in (-1, 0, 1))
    return sorted(out)


class Bars:
    def __init__(self):
        self.h = _keys()
        self.cache = pickle.loads(CACHE.read_bytes()) if CACHE.exists() else {}
        self.dirty = 0

    def get(self, symbols, day: date):
        need = [s for s in symbols if (s, day) not in self.cache]
        for i in range(0, len(need), 100):
            chunk = need[i:i + 100]
            got = {s: [] for s in chunk}
            token = None
            while True:
                p = {"symbols": ",".join(chunk), "timeframe": "1Min", "limit": 10000,
                     "start": f"{day}T13:30:00Z", "end": f"{day}T20:00:00Z"}
                if token:
                    p["page_token"] = token
                r = None
                for attempt in range(6):
                    try:
                        r = requests.get("https://data.alpaca.markets/v1beta1/options/bars", params=p,
                                         headers=self.h, timeout=30)
                    except requests.RequestException:
                        time.sleep(2 + attempt * 3)
                        continue
                    if r.status_code == 429:
                        time.sleep(3)
                        continue
                    break
                j = r.json() if r is not None and r.ok else {}
                for s, rows in (j.get("bars") or {}).items():
                    got[s].extend(rows)
                token = j.get("next_page_token")
                if not token:
                    break
            for s, rows in got.items():
                df = pd.DataFrame(rows)
                if not df.empty:
                    df.index = pd.to_datetime(df["t"], utc=True)
                self.cache[(s, day)] = df
            self.dirty += 1
            if self.dirty % 20 == 0:
                self.save()
        return {s: self.cache[(s, day)] for s in symbols}

    def save(self):
        CACHE.write_bytes(pickle.dumps(self.cache))


def pick_contract(bars, tk, side, px, day, dte_rule, itm_steps):
    exps = [day + timedelta(days=k) for k in range(0, 10)]
    exps = [e for e in exps if e.weekday() < 5]
    if dte_rule == "0dte":
        exps = exps[:1]
    elif dte_rule == "1+":
        exps = exps[1:3]
    elif dte_rule == "2-5":
        exps = [e for e in exps if 2 <= (e - day).days <= 7]
    syms = [occ(tk, e, side, k) for e in exps for k in strikes_near(px, side, itm_steps)]
    got = bars.get(syms, day)
    best = None
    for e in exps:
        cands = [(s, df) for s, df in got.items() if s[len(tk):len(tk) + 6] == f"{e:%y%m%d}" and not df.empty]
        if not cands:
            continue
        want = target_strike(px, side, itm_steps)
        cands.sort(key=lambda c: (abs(int(c[0][-8:]) / 1000 - want), -c[1]["v"].sum()))
        best = cands[0]
        break
    return best


def underlying_exit(m1, en, ex, rule):
    """Exit timestamp + reason on 1m underlying for the bot's exit logic."""
    sgn = 1 if en["type"] == "CALL" else -1
    e, risk = en["price"], abs(en["price"] - en["sl"])
    t0 = pd.Timestamp(en["bar_time"]) + pd.Timedelta(minutes=5)
    t1 = pd.Timestamp(ex["bar_time"]) + pd.Timedelta(minutes=5)
    w = m1[(m1.index >= t0)]
    peak, stop = 0.0, (-1.0 if rule.get("hard", True) else -99)
    for ts, row in w.iterrows():
        if rule.get("chart", True) and ts >= t1:
            return ts, "chart"
        loc = ts.tz_convert(NY)
        if loc.hour * 60 + loc.minute >= FLAT_MIN:
            return ts, "eod"
        adv = sgn * ((row["Low"] if sgn > 0 else row["High"]) - e) / risk
        fav = sgn * ((row["High"] if sgn > 0 else row["Low"]) - e) / risk
        if adv <= stop:
            return ts, "stop"
        peak = max(peak, fav)
        if rule.get("trail") is not None and peak >= rule["arm"]:
            stop = max(stop, peak - rule["trail"])
    return (w.index[-1] if len(w) else t1), "end"


def first_bar(df, t, within=3):
    w = df[(df.index >= t) & (df.index <= t + pd.Timedelta(minutes=within))]
    return w.iloc[0] if len(w) else None


def premium_exit(df, t_in, entry, t_cap, rule):
    """Walk option bars from entry to t_cap; return (exit_price, ts, why) under a premium rule, or None."""
    w = df[(df.index > t_in) & (df.index <= t_cap)]
    peak = entry
    for ts, b in w.iterrows():
        if rule.get("stop") is not None and b["l"] <= entry * (1 - rule["stop"]):
            return b["l"], ts, "prem_stop"
        if rule.get("take") is not None and b["h"] >= entry * (1 + rule["take"]):
            return entry * (1 + rule["take"]) * 0.98, ts, "prem_take"
        peak = max(peak, b["h"])
        if rule.get("ptrail") is not None and peak >= entry * (1 + rule["parm"]):
            lvl = peak * (1 - rule["ptrail"])
            if b["l"] <= lvl:
                return lvl * 0.98, ts, "prem_trail"
    return None


def main():
    os.environ["SIGNAL_MODE"] = "PULLBACK_ONLY"
    import app
    app.load_env_var = lambda k, d="": os.environ.get(k, d) if k == "SIGNAL_MODE" else d
    S = dict(app.PINE_DEFAULTS)
    data = load_data()
    m1 = load_1m(list(data))
    start = min(df.index.min() for df in m1.values())
    today = pd.Timestamp.now(tz=NY).date()
    trades = []
    for tk, df in data.items():
        if tk not in m1 or tk in ("SPX", "^SPX"):
            continue
        for en, ex in engine_trades(df, S):
            t = pd.Timestamp(en["bar_time"])
            if t >= start and t.tz_convert(NY).date() < today:
                trades.append((tk, en, ex))
    print(f"{len(trades)} chart trades {min(pd.Timestamp(e['bar_time']) for _, e, _ in trades).date()}.."
          f"{max(pd.Timestamp(e['bar_time']) for _, e, _ in trades).date()}")

    bars = Bars()
    contracts = {"0DTE ATM": ("0dte", 0), "1-2 DTE ATM": ("1+", 0), "2-7 DTE ATM": ("2-5", 0), "1-2 DTE 1 ITM": ("1+", 1),
                 "2-7 DTE 2 ITM": ("2-5", 2)}
    exits = {
        "chart exits": {"chart": True, "hard": False},
        "chart + live stop + trail .3/.3 (current bot)": {"chart": True, "arm": 0.3, "trail": 0.3},
    }
    prem_rules = {
        "current + premium stop -15%": {"stop": 0.15},
        "current + premium trail 10% after +10%": {"parm": 0.10, "ptrail": 0.10},
        "current + take +20%": {"take": 0.20},
        "current + stop -15% + trail 10% after +10%": {"stop": 0.15, "parm": 0.10, "ptrail": 0.10},
    }
    res = []
    import atexit
    atexit.register(bars.save)
    for n, (tk, en, ex) in enumerate(trades):
        if n % 50 == 0:
            print(f"  {n}/{len(trades)}", flush=True)
        t0 = pd.Timestamp(en["bar_time"]) + pd.Timedelta(minutes=5)
        day = t0.tz_convert(NY).date()
        side = "C" if en["type"] == "CALL" else "P"
        loc = t0.tz_convert(NY)
        for cname, (dte, itm) in contracts.items():
            pick = pick_contract(bars, tk, side, en["price"], day, dte, itm)
            if pick is None:
                continue
            sym, odf = pick
            b0 = first_bar(odf, t0)
            if b0 is None:
                continue
            buy = buy_px(b0)
            for ename, rule in exits.items():
                t_x, why = underlying_exit(m1[tk], en, ex, rule)
                bx = first_bar(odf, t_x, within=5)
                if bx is None:
                    prior = odf[odf.index <= t_x]
                    if prior.empty:
                        continue
                    bx = prior.iloc[-1]
                sell = sell_px(bx)
                res.append(dict(tk=tk, day=day, mins=loc.hour * 60 + loc.minute, contract=cname, exit=ename,
                                pct=(sell / buy - 1) * 100, usd=(sell - buy) * 100, prem=buy))
                if ename.startswith("chart + live"):
                    for pname, pr in prem_rules.items():
                        hit = premium_exit(odf, b0.name, buy, t_x, pr)
                        s2 = hit[0] if hit else sell
                        res.append(dict(tk=tk, day=day, mins=loc.hour * 60 + loc.minute, contract=cname, exit=pname,
                                        pct=(s2 / buy - 1) * 100, usd=(s2 - buy) * 100, prem=buy))
    bars.save()
    df = pd.DataFrame(res)
    days = sorted(df["day"].unique())
    half = days[len(days) // 2]

    def line(g):
        a, b = g[g["day"] < half], g[g["day"] >= half]
        f = lambda x: f"{len(x):4d} {(x['pct'] > 0).mean() * 100:5.1f} {x['pct'].mean():7.1f} {x['usd'].sum():8.0f}" if len(x) else " " * 27
        return f"{f(a)} | {f(b)}"

    print(f"\nSessions {days[0]}..{days[-1]} split at {half}. Columns per half: n, win%, avg option %, total $ (1 contract)")
    print(f"{'contract':14s} {'exit':46s} | {'1st half':^27s} | {'2nd half':^27s}")
    for (c, e), g in df.groupby(["contract", "exit"], sort=False):
        print(f"{c:14s} {e:46s} | {line(g)}")
    cur = df[df["exit"].str.startswith("chart + live")]
    print("\nCurrent bot exits by entry time (all contracts):")
    for lo, hi, lab in ((570, 600, "09:30-10:00"), (600, 690, "10:00-11:30"), (690, 840, "11:30-14:00"), (840, 960, "14:00-16:00")):
        g = cur[(cur["mins"] >= lo) & (cur["mins"] < hi)]
        print(f"  {lab}: {line(g)}")


if __name__ == "__main__":
    main()
