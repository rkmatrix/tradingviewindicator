"""
Early-trigger intraday setups, scored in R with a cost haircut, walk-forward (IS = first 2/3 of sessions, OOS = rest).

    python research/strategies.py

ORB   : Opening Range Breakout - first 5m close beyond the N-minute opening range; stop at the other side of the range
        (or its midpoint); target k x R; flat at 15:55 ET. One trade per symbol per day.
VWAPR : VWAP reclaim / rejection after the open - price crosses back through session VWAP in the direction of the
        opening-range bias; stop beyond the last swing (3 bars); target k x R; flat at 15:55.
"""
import sys, warnings, itertools
from pathlib import Path
warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parent))
import numpy as np, pandas as pd
from tune import load_data

COST_R = 0.05   # spread / slippage haircut per trade, in R


def prep(df):
    d = df.tz_convert("America/New_York").copy()
    d["day"] = d.index.normalize()
    d["mins"] = d.index.hour * 60 + d.index.minute
    tp = (d.High + d.Low + d.Close) / 3
    d["vwap"] = (tp * d.Volume).groupby(d.day).cumsum() / d.Volume.groupby(d.day).cumsum().replace(0, np.nan)
    tr = pd.concat([d.High - d.Low, (d.High - d.Close.shift()).abs(), (d.Low - d.Close.shift()).abs()], axis=1).max(axis=1)
    d["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean()
    d["vmed"] = d.Volume.rolling(20).median()
    return d


def simulate(bars, i0, direction, entry, stop, target):
    """Walk forward from bar i0+1; stop wins ties; flat at the last bar's close (15:55)."""
    risk = abs(entry - stop)
    for j in range(i0 + 1, len(bars)):
        h, l = bars.High.iat[j], bars.Low.iat[j]
        o = bars.Open.iat[j]
        if direction == 1:
            if l <= stop:
                return (min(o, stop) - entry) / risk, j - i0
            if h >= target:
                return (max(o, target) - entry) / risk, j - i0
        else:
            if h >= stop:
                return (entry - max(o, stop)) / risk, j - i0
            if l <= target:
                return (entry - min(o, target)) / risk, j - i0
    last = bars.Close.iat[-1]
    return direction * (last - entry) / risk, len(bars) - 1 - i0


def orb(d, or_min=15, stop_mode="opposite", k=2.0, vol_mult=0.0, vwap_filter=False, max_risk_atr=0.0, last_entry=12 * 60):
    trades = []
    for day, g in d.groupby("day"):
        g = g[(g.mins >= 570) & (g.mins < 960)]
        if len(g) < 20:
            continue
        orng = g[g.mins < 570 + or_min]
        hi, lo = orng.High.max(), orng.Low.min()
        mid = (hi + lo) / 2
        rest = g[g.mins >= 570 + or_min]
        for idx in range(len(rest)):
            row = rest.iloc[idx]
            if row.mins >= last_entry:
                break
            direction = 1 if row.Close > hi else -1 if row.Close < lo else 0
            if direction == 0:
                continue
            if vol_mult > 0 and not (row.Volume >= vol_mult * row.vmed):
                break
            if vwap_filter and ((direction == 1 and row.Close < row.vwap) or (direction == -1 and row.Close > row.vwap)):
                break
            entry = row.Close
            stop = (lo if direction == 1 else hi) if stop_mode == "opposite" else mid
            risk = abs(entry - stop)
            if risk <= 0 or (max_risk_atr > 0 and risk > max_risk_atr * row.atr):
                break
            target = entry + direction * k * risk
            r, held = simulate(rest, idx, direction, entry, stop, target)
            trades.append({"day": day, "R": r - COST_R, "held": held, "dir": direction})
            break
    return trades


def vwap_reclaim(d, k=2.0, bias_min=30, start=10 * 60, last_entry=14 * 60, vol_mult=0.0):
    trades = []
    for day, g in d.groupby("day"):
        g = g[(g.mins >= 570) & (g.mins < 960)].reset_index()
        if len(g) < 30:
            continue
        orng = g[g.mins < 570 + bias_min]
        bias = 1 if orng.Close.iat[-1] > orng.Open.iat[0] else -1
        for i in range(3, len(g)):
            if g.mins.iat[i] < start:
                continue
            if g.mins.iat[i] >= last_entry:
                break
            c, pc, vw, pvw = g.Close.iat[i], g.Close.iat[i - 1], g.vwap.iat[i], g.vwap.iat[i - 1]
            crossed = (bias == 1 and pc <= pvw and c > vw) or (bias == -1 and pc >= pvw and c < vw)
            if not crossed:
                continue
            if vol_mult > 0 and not (g.Volume.iat[i] >= vol_mult * g.vmed.iat[i]):
                continue
            stop = g.Low.iloc[i - 3:i + 1].min() if bias == 1 else g.High.iloc[i - 3:i + 1].max()
            risk = abs(c - stop)
            if risk <= 0.1 * g.atr.iat[i]:
                continue
            r, held = simulate(g.set_index("Datetime" if "Datetime" in g else g.columns[0]), i, bias, c, stop, c + bias * k * risk)
            trades.append({"day": day, "R": r - COST_R, "held": held, "dir": bias})
            break
    return trades


def score(rows):
    if not rows:
        return "    0     -      -     -      -"
    r = np.array([x["R"] for x in rows])
    pf = r[r > 0].sum() / -r[r < 0].sum() if (r < 0).any() else 99
    return f"{len(r):5d} {(r > 0).mean() * 100:5.1f} {r.mean():6.3f} {pf:5.2f} {r.sum():6.1f}"


def main():
    data = {k: prep(v) for k, v in load_data().items() if not v.empty}
    days = sorted({d for df in data.values() for d in df.day.unique()})
    split = days[int(len(days) * 2 / 3)]
    print(f"{len(data)} symbols, {len(days)} sessions, OOS from {pd.Timestamp(split).date()}, cost {COST_R}R/trade")
    configs = {}
    for or_min, stop_mode, k in itertools.product([5, 15, 30], ["opposite", "mid"], [1.0, 2.0, 3.0]):
        configs[f"ORB{or_min} {stop_mode} {k}R"] = (orb, dict(or_min=or_min, stop_mode=stop_mode, k=k))
    configs["ORB15 mid 2R vol1.5"] = (orb, dict(or_min=15, stop_mode="mid", k=2.0, vol_mult=1.5))
    configs["ORB15 mid 2R vwap"] = (orb, dict(or_min=15, stop_mode="mid", k=2.0, vwap_filter=True))
    configs["ORB15 mid 2R risk<1.5atr"] = (orb, dict(or_min=15, stop_mode="mid", k=2.0, max_risk_atr=1.5))
    configs["ORB30 mid 2R vol1.5"] = (orb, dict(or_min=30, stop_mode="mid", k=2.0, vol_mult=1.5))
    for k in (1.5, 2.0, 3.0):
        configs[f"VWAP reclaim {k}R"] = (vwap_reclaim, dict(k=k))
    configs["VWAP reclaim 2R vol1.2"] = (vwap_reclaim, dict(k=2.0, vol_mult=1.2))

    print(f"{'setup':28s} | {'IS n':>5} {'win%':>5} {'avgR':>6} {'PF':>5} {'totR':>6} | {'OOS n':>5} {'win%':>5} {'avgR':>6} {'PF':>5} {'totR':>6}")
    for name, (fn, kw) in configs.items():
        rows = []
        for tk, d in data.items():
            for t in fn(d, **kw):
                t["tk"] = tk
                rows.append(t)
        is_ = [x for x in rows if x["day"] < split]
        oos = [x for x in rows if x["day"] >= split]
        print(f"{name:28s} | {score(is_)} | {score(oos)}")


if __name__ == "__main__":
    main()
