"""Run the scanner's exact live pipeline (download -> scrub -> resample -> engine) once and print today's flags."""
import sys, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app, yfinance as yf, pandas as pd

tks = sys.argv[1:] or ["SPY", "AMZN", "QQQ"]
d5 = yf.download(" ".join(tks), period="60d", interval="5m", progress=False, auto_adjust=False)
d1 = yf.download(" ".join(tks), period="7d", interval="1m", progress=False, auto_adjust=False)
S = app.load_pine_settings()
print("timeframe", S["timeframe_min"], "m | SIGNAL_MODE", app.load_env_var("SIGNAL_MODE", app.SIGNAL_MODE))
for tk in tks:
    df = app._extract_ticker(d5, tk, len(tks) == 1).dropna(subset=["Open", "High", "Low", "Close"])
    df, _ = app.strip_phantom_volume(df, app._extract_ticker(d1, tk, len(tks) == 1))
    df = app.resample_bars(df, S["timeframe_min"])
    res = app.evaluate_pine_indicator(df)
    day = df.index[-1].date()
    flags = [(pd.Timestamp(s["bar_time"]).tz_convert("America/Chicago").strftime("%H:%M"), s["type"], round(s["price"], 2))
             for s in res["signals"] if pd.Timestamp(s["bar_time"]).date() == day]
    print(f"{tk}: {len(df)} bars, last closed {res['last_closed_bar_time']}, position {res['position']}, today (CST bar open): {flags}")
