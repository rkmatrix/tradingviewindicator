"""Dump the bar-level context of a scanner signal: python research/inspect_signal.py AMZN 2026-10-08 10:40"""
import sys, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app, yfinance as yf, pandas as pd

tk, day, hhmm = sys.argv[1], sys.argv[2], sys.argv[3]
d5 = yf.download(tk, period="30d", interval="5m", progress=False, auto_adjust=False)
d1 = yf.download(tk, period="7d", interval="1m", progress=False, auto_adjust=False)
df = app._extract_ticker(d5, tk, True).dropna(subset=["Open", "High", "Low", "Close"])
df, _ = app.strip_phantom_volume(df, app._extract_ticker(d1, tk, True))
res = app.evaluate_pine_indicator(df)
for s in res["signals"]:
    t = pd.Timestamp(s["bar_time"]).tz_convert("America/Chicago")
    if str(t.date()) == day:
        print(t.strftime("%H:%M"), s["type"], round(s["price"], 2), s["engine"], s.get("exit_code", ""), s.get("pnl_pct", ""))

d = df.tz_convert("America/Chicago")
c = d["Close"]
macd = app.pine_ema(c, 12) - app.pine_ema(c, 26)
atr = app.pine_rma(app.pine_tr(d["High"], d["Low"], c), 14)
tp = (d["High"] + d["Low"] + c) / 3
vwap = (tp * d["Volume"]).groupby(d.index.date).cumsum() / d["Volume"].groupby(d.index.date).cumsum()
out = pd.DataFrame({"o": d.Open, "h": d.High, "l": d.Low, "c": c,
                    "e9": app.pine_ema(c, 9), "e21": app.pine_ema(c, 21), "e50": app.pine_ema(c, 50),
                    "vwap": vwap, "rsi": app.pine_rsi(c, 14), "hist": macd - app.pine_ema(macd, 9), "atr": atr,
                    "vol%": d.Volume / d.Volume.rolling(20).median() * 100})
out["ext9_atr"] = (c - out.e9) / atr
pd.set_option("display.width", 250)
t0 = pd.Timestamp(f"{day} {hhmm}", tz="America/Chicago")
print(out.loc[t0 - pd.Timedelta(minutes=50): t0 + pd.Timedelta(minutes=10)].round(2).to_string())
