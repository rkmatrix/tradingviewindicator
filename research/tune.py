"""
Walk-forward filter research for the AlphaWave engine (the exact engine the scanner runs).

    python research/tune.py              # downloads 60d of 5m bars for the dashboard watchlist (cached)

Each config is replayed per symbol; trades are scored in R (P&L / initial stop distance). The first ~2/3 of the
sessions are in-sample (IS) for choosing filters, the remaining sessions are out-of-sample (OOS) for checking them.
"""
import sys, json, pickle, warnings, itertools
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import numpy as np, pandas as pd

CACHE = ROOT / "research" / "cache_5m_60d.pkl"


def load_data():
    import app, yfinance as yf
    if CACHE.exists():
        return pickle.loads(CACHE.read_bytes())
    wl = [t.strip().upper() for t in app.load_env_var("WATCHLIST", app.WATCHLIST_STR).split(",") if t.strip()]
    yf_map = {tk: ("^GSPC" if tk in ("SPX", "^SPX") else tk) for tk in wl}
    raw = yf.download(" ".join(dict.fromkeys(yf_map.values())), period="60d", interval="5m", progress=False, auto_adjust=False)
    out = {}
    for tk, ytk in yf_map.items():
        df = app._extract_ticker(raw, ytk, False)
        if df is None or df.empty:
            continue
        df = df.dropna(subset=["Open", "High", "Low", "Close"]).copy()
        # 1m history is not available this far back: scrub phantom prints at the 5m level instead
        v = df["Volume"].astype(float)
        med = v.rolling(21, center=True, min_periods=5).median()
        loc = df.index.tz_convert("America/New_York")
        mins = loc.hour * 60 + loc.minute
        edge = (mins < 9 * 60 + 40) | (mins >= 15 * 60 + 50)
        bad = (v > med * 12) & ~edge
        df["Volume"] = v.where(~bad, med)
        out[tk] = df
    CACHE.write_bytes(pickle.dumps(out))
    return out


def trades_from(signals):
    trades, open_ = [], None
    for s in signals:
        if s["type"] in ("CALL", "PUT"):
            open_ = s
        elif open_ is not None:
            risk = abs(open_["price"] - open_["sl"])
            sgn = 1 if open_["type"] == "CALL" else -1
            r = sgn * (s["price"] - open_["price"]) / risk if risk > 0 else 0.0
            trades.append({"t": open_["bar_time"], "dir": open_["type"], "engine": open_["engine"], "R": r,
                           "pnl": s["pnl_pct"] or 0.0, "bars": s["bar_idx"] - open_["bar_idx"], "code": s["exit_code"]})
            open_ = None
    return trades


def run_one(args):
    import os, app
    name, overrides, data = args
    overrides = dict(overrides)
    os.environ["SIGNAL_MODE"] = overrides.pop("__mode", "ALL_CONFLUENCE")
    tf = overrides.pop("__tf", 5)
    if tf != 5:
        agg = {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
        res_data = {}
        for tk, df in data.items():
            if df.empty:
                continue
            loc = df[list(agg)].tz_convert("America/New_York")
            r = loc.resample(f"{tf}min", origin="start_day", offset="9h30min", label="left", closed="left").agg(agg)
            res_data[tk] = r.dropna(subset=["Close"])
        data = res_data
    app.load_env_var = lambda k, d="": os.environ.get(k, d) if k == "SIGNAL_MODE" else d
    S = dict(app.PINE_DEFAULTS)
    S.update(overrides)
    rows = []
    for tk, df in data.items():
        res = app.evaluate_pine_indicator(df, S)
        if res:
            for t in trades_from(res["signals"]):
                t["tk"] = tk
                rows.append(t)
    return name, rows


def score(rows):
    if not rows:
        return dict(n=0, win=0, avgR=0, pf=0, totR=0)
    r = np.array([x["R"] for x in rows])
    gain, loss = r[r > 0].sum(), -r[r < 0].sum()
    return dict(n=len(r), win=round((r > 0).mean() * 100, 1), avgR=round(r.mean(), 3),
                pf=round(gain / loss, 2) if loss else 99, totR=round(r.sum(), 1))


CONFIGS = {
    "baseline": {},
    "sep0.2": {"min_trend_sep_atr": 0.2}, "sep0.4": {"min_trend_sep_atr": 0.4}, "sep0.6": {"min_trend_sep_atr": 0.6},
    "ext0.6": {"max_ext_atr": 0.6}, "ext0.8": {"max_ext_atr": 0.8}, "ext1.0": {"max_ext_atr": 1.0},
    "vwap": {"use_vwap": True},
    "open15": {"skip_open_min": 15}, "open30": {"skip_open_min": 30},
    "loss2/day": {"max_losses_per_day": 2}, "loss3/day": {"max_losses_per_day": 3},
    "losscd6": {"loss_cooldown_bars": 6},
    "adx20": {"use_adx": True, "adx_min": 20}, "adx25": {"use_adx": True, "adx_min": 25},
    "htf60": {"use_htf": True},
    "cooldown3": {"cooldown_bars": 3},
    "pullbackOnly": {"__mode": "PULLBACK"},
    "noResume": {"use_resume": False},
    "noFastZone": {"fast_zone": False},
    "tp2_2.0": {"tp2_atr": 2.0}, "sl1.5": {"sl_atr": 1.5},
}


def main():
    data = load_data()
    days = sorted({d for df in data.values() for d in df.index.tz_convert("America/New_York").normalize().unique()})
    split = days[int(len(days) * 2 / 3)]
    print(f"{len(data)} symbols, {len(days)} sessions, OOS from {split.date()}")
    cfgs = dict(CONFIGS)
    if len(sys.argv) > 1:
        cfgs = json.loads(Path(sys.argv[1]).read_text())
    jobs = [(name, ov, data) for name, ov in cfgs.items()]
    with ProcessPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(run_one, jobs))
    print(f"{'config':28s} | {'IS n':>5} {'win%':>5} {'avgR':>6} {'PF':>5} {'totR':>6} | {'OOS n':>5} {'win%':>5} {'avgR':>6} {'PF':>5} {'totR':>6}")
    for name, rows in results:
        is_ = [x for x in rows if pd.Timestamp(x["t"]).tz_convert("America/New_York").normalize() < split]
        oos = [x for x in rows if pd.Timestamp(x["t"]).tz_convert("America/New_York").normalize() >= split]
        a, b = score(is_), score(oos)
        print(f"{name:28s} | {a['n']:5d} {a['win']:5.1f} {a['avgR']:6.3f} {a['pf']:5.2f} {a['totR']:6.1f} | {b['n']:5d} {b['win']:5.1f} {b['avgR']:6.3f} {b['pf']:5.2f} {b['totR']:6.1f}")


if __name__ == "__main__":
    main()
