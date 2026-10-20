"""
1-minute check of the exit rules in exit_study.py (Yahoo keeps ~30 days of 1m bars).

    python research/exit_study_1m.py

Entries and chart exits come from the 5m engine; the exit rules are then walked on 1m bars starting at the close of
the entry bar, with ``SLIP_R`` charged against every rule-triggered exit.
"""
import pickle
import sys
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "research"))
import pandas as pd
from exit_study import engine_trades
from tune import load_data, score

CACHE_1M = ROOT / "research" / "cache_1m_30d.pkl"
SLIP_R = 0.05
FLAT_MIN = 15 * 60 + 50


def load_1m(symbols):
    import app
    import yfinance as yf
    if CACHE_1M.exists():
        return pickle.loads(CACHE_1M.read_bytes())
    out = {tk: [] for tk in symbols}
    yf_map = {tk: ("^GSPC" if tk in ("SPX", "^SPX") else tk) for tk in symbols}
    end = datetime.now(timezone.utc)
    for k in range(4):
        e = end - timedelta(days=7 * k)
        s = e - timedelta(days=7)
        raw = yf.download(" ".join(dict.fromkeys(yf_map.values())), start=s, end=e, interval="1m",
                          progress=False, auto_adjust=False)
        for tk, ytk in yf_map.items():
            df = app._extract_ticker(raw, ytk, False)
            if df is not None and not df.empty:
                out[tk].append(df.dropna(subset=["High", "Low", "Close"]))
    res = {tk: pd.concat(v).sort_index().loc[lambda d: ~d.index.duplicated()] for tk, v in out.items() if v}
    CACHE_1M.write_bytes(pickle.dumps(res))
    return res


def walk(m1, entry, exit_, rule):
    sgn = 1 if entry["type"] == "CALL" else -1
    e, risk = entry["price"], abs(entry["price"] - entry["sl"])
    if risk <= 0:
        return None
    tp1R = abs(entry["tp1"] - e) / risk
    t0 = pd.Timestamp(entry["bar_time"]) + pd.Timedelta(minutes=5)
    t1 = pd.Timestamp(exit_["bar_time"]) + pd.Timedelta(minutes=5)
    ignore_fade = rule.get("ignore_codes") and exit_.get("exit_code") in rule["ignore_codes"]
    if ignore_fade:
        t1 = t0.tz_convert("America/New_York").normalize().tz_convert("UTC") + pd.Timedelta(hours=24)
    w = m1[(m1.index >= t0) & (m1.index < t1)]
    if w.empty:
        return None
    base_R = sgn * (w["Close"].iloc[-1] - e) / risk if ignore_fade else sgn * (exit_["price"] - e) / risk
    peak, stop = 0.0, (-1.0 if ignore_fade else -99.0)
    for ts, row in w.iterrows():
        fav = sgn * ((row["High"] if sgn > 0 else row["Low"]) - e) / risk
        adv = sgn * ((row["Low"] if sgn > 0 else row["High"]) - e) / risk
        if stop > -99 and adv <= stop:
            return stop - SLIP_R
        tgt = rule.get("target")
        if tgt is not None:
            t = tp1R if tgt == "tp1" else float(tgt)
            if fav >= t:
                return t - SLIP_R
        loc = ts.tz_convert("America/New_York")
        if loc.hour * 60 + loc.minute >= FLAT_MIN:
            return sgn * (row["Close"] - e) / risk - SLIP_R
        peak = max(peak, fav)
        if rule.get("lock") is not None and peak >= rule["lock"]:
            stop = max(stop, rule.get("lock_to", 0.0))
        if rule.get("trail") is not None and peak >= rule.get("arm", 0.0):
            stop = max(stop, peak - rule["trail"])
    return base_R


RULES = {
    "chart exit only (current)": {},
    "full exit at TP1": {"target": "tp1"},
    "+0.1R lock once +0.5R": {"lock": 0.5, "lock_to": 0.1},
    "trail 0.5R after +0.5R": {"arm": 0.5, "trail": 0.5},
    "trail 0.4R after +0.4R": {"arm": 0.4, "trail": 0.4},
    "trail 0.3R after +0.3R": {"arm": 0.3, "trail": 0.3},
    "trail 0.3R after +0.5R": {"arm": 0.5, "trail": 0.3},
    "trail 0.25R after +0.4R": {"arm": 0.4, "trail": 0.25},
    "trail 0.2R after +0.3R": {"arm": 0.3, "trail": 0.2},
    "trail .3/.3, ignore momentum-fade exits": {"arm": 0.3, "trail": 0.3, "ignore_codes": (1,)},
    "trail .3/.3, ignore fade + time exits": {"arm": 0.3, "trail": 0.3, "ignore_codes": (1, 4)},
}


def main():
    import os
    import app
    os.environ["SIGNAL_MODE"] = "PULLBACK_ONLY"
    app.load_env_var = lambda k, d="": os.environ.get(k, d) if k == "SIGNAL_MODE" else d
    S = dict(app.PINE_DEFAULTS)
    data = load_data()
    m1 = load_1m(list(data))
    start = min(df.index.min() for df in m1.values())
    trades = [(tk, en, ex) for tk, df in data.items() if tk in m1
              for en, ex in engine_trades(df, S) if pd.Timestamp(en["bar_time"]) >= start]
    days = sorted({pd.Timestamp(en["bar_time"]).tz_convert("America/New_York").date() for _, en, _ in trades})
    half = days[len(days) // 2]
    print(f"{len(trades)} trades on 1m data, {len(days)} sessions {days[0]}..{days[-1]}; slippage {SLIP_R}R per rule exit")
    print(f"{'rule':30s} | {'1st n':>5} {'win%':>5} {'avgR':>6} {'PF':>5} | {'2nd n':>5} {'win%':>5} {'avgR':>6} {'PF':>5}")
    for name, rule in RULES.items():
        a, b = [], []
        for tk, en, ex in trades:
            r = walk(m1[tk], en, ex, rule)
            if r is None:
                continue
            d = pd.Timestamp(en["bar_time"]).tz_convert("America/New_York").date()
            (a if d < half else b).append({"R": r})
        x, y = score(a), score(b)
        print(f"{name:30s} | {x['n']:5d} {x['win']:5.1f} {x['avgR']:6.3f} {x['pf']:5.2f} | {y['n']:5d} {y['win']:5.1f} {y['avgR']:6.3f} {y['pf']:5.2f}")


if __name__ == "__main__":
    main()
