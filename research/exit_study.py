"""
Exit-rule study for the shipped setup (5m, Value Zone Pullback Only, No-Trade Filter).

    python research/exit_study.py

Same entries as the chart; only the exit changes. Each rule walks the 5m bars after the entry bar and can only
exit EARLIER than the chart's own exit (the paper bot still honours every chart TP flag) or at the 15:50 ET flatten.
Bars where the stop and a profit level are both touched are scored as the stop (pessimistic). Results in R
(P&L / initial stop distance); IS = first 2/3 of sessions, OOS = last 1/3.
"""
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "research"))
import numpy as np
import pandas as pd
from tune import load_data, score

FLAT_MIN = 15 * 60 + 50


def engine_trades(df, S):
    import app
    res = app.evaluate_pine_indicator(df, S)
    out, open_ = [], None
    for s in (res or {}).get("signals", []):
        if s["type"] in ("CALL", "PUT"):
            open_ = s
        elif open_ is not None:
            out.append((open_, s))
            open_ = None
    return out


def simulate(df, entry, exit_, rule):
    """Return exit R for one trade under ``rule`` (dict)."""
    sgn = 1 if entry["type"] == "CALL" else -1
    e, sl = entry["price"], entry["sl"]
    risk = abs(e - sl)
    if risk <= 0:
        return None
    tp1R = abs(entry["tp1"] - e) / risk
    base_R = sgn * (exit_["price"] - e) / risk
    i0, i1 = entry["bar_idx"], exit_["bar_idx"]
    hi, lo, cl = df["High"].values, df["Low"].values, df["Close"].values
    loc = df.index.tz_convert("America/New_York")
    peak = 0.0
    stop = -1.0
    half_done = None
    for i in range(i0 + 1, i1 + 1):
        fav = sgn * ((hi[i] if sgn > 0 else lo[i]) - e) / risk
        adv = sgn * ((lo[i] if sgn > 0 else hi[i]) - e) / risk
        # 1) stop from the previous bar's state
        if rule.get("trail") is not None or rule.get("lock") is not None:
            if adv <= stop and stop > -1.0:
                r = stop
                return r if half_done is None else 0.5 * half_done + 0.5 * r
        # 2) fixed profit target
        tgt = rule.get("target")
        if tgt is not None:
            t = tp1R if tgt == "tp1" else float(tgt)
            if fav >= t and adv > -1.0:
                if rule.get("half"):
                    if half_done is None:
                        half_done = t
                        stop = max(stop, 0.0)
                else:
                    return t
        if i == i1:
            break
        m = loc[i].hour * 60 + loc[i].minute
        if m >= FLAT_MIN:
            r = sgn * (cl[i] - e) / risk
            return r if half_done is None else 0.5 * half_done + 0.5 * r
        # 3) update the trailing state with this bar's extreme for the next bar
        peak = max(peak, fav)
        lock = rule.get("lock")
        if lock is not None and peak >= lock:
            stop = max(stop, rule.get("lock_to", 0.0))
        trail = rule.get("trail")
        if trail is not None and peak >= rule.get("arm", 0.0):
            stop = max(stop, peak - trail)
    return base_R if half_done is None else 0.5 * half_done + 0.5 * base_R


RULES = {
    "chart exit only (current)": {},
    "full exit at TP1": {"target": "tp1"},
    "full exit at +0.5R": {"target": 0.5},
    "full exit at +0.75R": {"target": 0.75},
    "half at TP1, rest BE": {"target": "tp1", "half": True},
    "BE once +0.5R": {"lock": 0.5, "lock_to": 0.0},
    "+0.1R lock once +0.5R": {"lock": 0.5, "lock_to": 0.1},
    "trail 0.5R after +0.5R": {"arm": 0.5, "trail": 0.5},
    "trail 0.4R after +0.4R": {"arm": 0.4, "trail": 0.4},
    "trail 0.3R after +0.3R": {"arm": 0.3, "trail": 0.3},
    "trail 0.5R after +0.5R, TP1": {"arm": 0.5, "trail": 0.5, "target": "tp1"},
    "BE+0.5R lock, TP1 full": {"lock": 0.5, "lock_to": 0.0, "target": "tp1"},
}


def main():
    import os
    import app
    os.environ["SIGNAL_MODE"] = "PULLBACK_ONLY"
    app.load_env_var = lambda k, d="": os.environ.get(k, d) if k == "SIGNAL_MODE" else d
    S = dict(app.PINE_DEFAULTS)
    data = load_data()
    days = sorted({d for df in data.values() for d in df.index.tz_convert("America/New_York").normalize().unique()})
    split = days[int(len(days) * 2 / 3)]
    print(f"{len(data)} symbols, {len(days)} sessions {days[0].date()}..{days[-1].date()}, OOS from {split.date()}")
    trades = []
    for tk, df in data.items():
        for en, ex in engine_trades(df, S):
            trades.append((tk, df, en, ex))
    print(f"{len(trades)} trades")
    print(f"{'rule':32s} | {'IS n':>5} {'win%':>5} {'avgR':>6} {'PF':>5} {'totR':>6} | {'OOS n':>5} {'win%':>5} {'avgR':>6} {'PF':>5} {'totR':>6}")
    for name, rule in RULES.items():
        is_, oos = [], []
        for tk, df, en, ex in trades:
            r = simulate(df, en, ex, rule)
            if r is None:
                continue
            day = pd.Timestamp(en["bar_time"]).tz_convert("America/New_York").normalize()
            (is_ if day < split else oos).append({"R": r})
        a, b = score(is_), score(oos)
        print(f"{name:32s} | {a['n']:5d} {a['win']:5.1f} {a['avgR']:6.3f} {a['pf']:5.2f} {a['totR']:6.1f} | {b['n']:5d} {b['win']:5.1f} {b['avgR']:6.3f} {b['pf']:5.2f} {b['totR']:6.1f}")


if __name__ == "__main__":
    main()
