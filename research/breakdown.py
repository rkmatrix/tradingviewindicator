"""Where does the engine win / lose?  python research/breakdown.py '{"use_vwap": true}'"""
import sys, json, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parent))
import pandas as pd
from tune import load_data, run_one

ov = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
_, rows = run_one(("x", ov, load_data()))
df = pd.DataFrame(rows)
t = pd.to_datetime(df.t, utc=True).dt.tz_convert("America/New_York")
df["hour"] = t.dt.hour + (t.dt.minute >= 30) * 0.5
codes = {1: "momentum fade", 2: "TP2", 3: "stop loss", 4: "time", 5: "reversal", 6: "breakeven"}
df["exit"] = df.code.map(codes)

def agg(g):
    return pd.Series({"n": len(g), "win%": (g.R > 0).mean() * 100, "avgR": g.R.mean(), "totR": g.R.sum(), "bars": g.bars.mean()})

pd.set_option("display.width", 200)
for col in ["exit", "engine", "dir", "hour"]:
    print(df.groupby(col).apply(agg).round(2).to_string(), "\n")
print("winners avgR %.2f   losers avgR %.2f" % (df.R[df.R > 0].mean(), df.R[df.R <= 0].mean()))
print("trades/day/symbol %.2f" % (len(df) / df.tk.nunique() / t.dt.date.nunique()))
