"""Show one symbol's flags for one day under a settings override: python research/day_check.py COIN 2026-10-19 '{...}' [tf]"""
import sys, json, os, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app, yfinance as yf, pandas as pd

tk, day = sys.argv[1], sys.argv[2]
ov = json.loads(Path(sys.argv[3]).read_text()) if len(sys.argv) > 3 and sys.argv[3].endswith(".json") else {}
mode = ov.pop("__mode", None)
if mode:
    os.environ["SIGNAL_MODE"] = mode
    app.load_env_var = lambda k, d="": mode if k == "SIGNAL_MODE" else d
tf = ov.pop("__tf", 5)
d5 = yf.download(tk, period="60d", interval="5m", progress=False, auto_adjust=False)
d1 = yf.download(tk, period="7d", interval="1m", progress=False, auto_adjust=False)
df = app._extract_ticker(d5, tk, True).dropna(subset=["Open", "High", "Low", "Close"])
df, _ = app.strip_phantom_volume(df, app._extract_ticker(d1, tk, True))
df = app.resample_bars(df, tf)
S = dict(app.PINE_DEFAULTS); S.update(ov); S["timeframe_min"] = tf
res = app.evaluate_pine_indicator(df, S)
for s in res["signals"]:
    t = pd.Timestamp(s["bar_time"]).tz_convert("America/Chicago")
    if str(t.date()) == day:
        print(t.strftime("%H:%M"), s["type"], round(s["price"], 2), s.get("exit_code", ""), s.get("pnl_pct", ""))
