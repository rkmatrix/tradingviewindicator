"""Per-bar PUT gate states: python research/why_no_signal.py AAPL 2026-10-19 09:20 11:30"""
import sys, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app, yfinance as yf, pandas as pd

tk, day, t0, t1 = sys.argv[1:5]
d5 = yf.download(tk, period="60d", interval="5m", progress=False, auto_adjust=False)
d1 = yf.download(tk, period="7d", interval="1m", progress=False, auto_adjust=False)
df = app._extract_ticker(d5, tk, True).dropna(subset=["Open", "High", "Low", "Close"])
df, _ = app.strip_phantom_volume(df, app._extract_ticker(d1, tk, True))
S = dict(app.PINE_DEFAULTS)
S["_diag"] = []
res = app.evaluate_pine_indicator(df, S)
d = pd.DataFrame(S["_diag"]).set_index("t")
d.index = pd.DatetimeIndex(d.index).tz_convert("America/Chicago")
d["atr/avg"] = d.atr / d.atrAvg
d = d.loc[f"{day} {t0}":f"{day} {t1}"]
pd.set_option("display.width", 250)
print(d[["c", "pos", "bearTrend", "bearTouch", "bearTurn", "bearResume", "volOk", "pbPut", "boPut", "atr/avg", "moveOk", "rsi", "hist", "setupPut", "rawPut"]].round(2).to_string())
