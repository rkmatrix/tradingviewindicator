# AlphaWave Multi-Confluence Pro Signal Engine

An institutional-grade trading indicator and backtested strategy combining **Trend Alignment**, **Dynamic Value Zone Pullbacks**, **Momentum Divergence**, **Volume Confirmation**, and **Volatility-based Targets (ATR)**.

---

## 📁 Repository Structure

| File | Description |
| :--- | :--- |
| **`MultiConfluence_Signal_Indicator.pine`** | **Pine Script v5 Indicator** for TradingView chart overlay, Buy/Sell visual labels, dynamic TP/SL target lines, bar coloring, and real-time alerts. |
| **`MultiConfluence_Signal_Strategy.pine`** | **Pine Script v5 Strategy** for 1-click backtesting inside TradingView's built-in **Strategy Tester** tab (Net Profit, Win Rate %, Profit Factor, Max Drawdown). |
| **`Webull_Custom_Indicator.txt`** | Ready-to-paste script for **Webull Desktop's** Custom Indicator Script Editor. |
| **`backtester.py`** | Standalone Python backtester powered by `yfinance` & `plotly` with interactive HTML visualizer. |
| **`backtest_chart.html`** | Generated interactive dark-mode chart showing candlesticks, EMA ribbon, Buy/Sell markers, MACD/RSI, and account equity curve. |

---

## 🧠 Indicator Architecture: Confluence Engine

A single indicator produces whipsaws in ranging markets. The **AlphaWave Confluence Engine** runs two entry engines and a full trade-lifecycle state machine:

**Engine A - Value Zone Pullback ("Buy the Dip / Sell the Rally")**
1. **Trend Regime**: Price > 50 EMA and 21 EMA > 50 EMA for CALLs (mirror for PUTs). Optional *Strict* mode also requires the 200 EMA side.
2. **Value Zone Touch**: The bar's low reaches the 21 EMA band while the close holds the 50 EMA. Band width is `%`-based (legacy) or ATR-adaptive.
3. **Momentum Trigger**: RSI (14) crossing 45 up / 55 down, or the MACD (12, 26, 9) histogram crossing zero.
4. **Volume**: Bar volume >= 80% of the 20-period volume MA.

**Engine B - Volume Breakout / V-Reversal**
- Price emerges from below (above) the 9 and 21 EMA on a bullish (bearish) candle with volume >= 110% of the volume MA, MACD histogram positive and rising (negative and falling), RSI >= 46 (<= 54).

**Trade Lifecycle (identical in indicator and strategy)**
- Entry at the signal bar close; SL = 1.2x ATR, TP1 = 1.5x ATR (trim), TP2 = 2.5x ATR (runner).
- After TP1 is touched the stop moves to breakeven (toggle).
- Exits: stop loss, TP2, momentum fade (close crosses the 9 EMA or MACD histogram rolls over), optional max-hold-bars theta exit, or an opposite signal (reversal). Each exit carries a reason code: `1` momentum fade, `2` TP2, `3` stop loss, `4` time, `5` reversal, `6` breakeven stop.

**Optional Filters**: higher-timeframe EMA bias (non-repainting), ADX trend-strength (skip chop), intraday session window, cooldown bars after an exit, and bar-close-only evaluation (on by default, so labels never repaint intrabar).

---

## 🚀 How to Add to TradingView

### Step 1: Open TradingView
1. Open any chart on [TradingView](https://www.tradingview.com/chart/).
2. At the bottom toolbar, click on the **Pine Editor** tab.

### Step 2: Add Visual Indicator
1. Open **[`MultiConfluence_Signal_Indicator.pine`](file:///c:/Projects/trading/ChartSignalGenerator/MultiConfluence_Signal_Indicator.pine)**.
2. Copy the entire script content.
3. In TradingView's Pine Editor, clear any default code and paste the script.
4. Click **"Save"**, then click **"Add to chart"**.
5. You will see:
   - Green **"CALL"** labels below confirmed candles, red **"PUT"** labels above.
   - Gold **"TP CALL / TP PUT"** exit labels, orange **"SL"**, amber **"BE"** (breakeven stop) and grey **"EXIT"** (time) labels, plus small **"TP1"** trim markers.
   - Active Entry / TP1 / TP2 / Stop Loss lines while a position is open.
   - EMA Trend Ribbon (9, 21, 50, 200) and a status dashboard (regime, RSI, MACD, ADX, volume, position, open P&L).
   - Candles colored by trend state.

### Step 3: Run TradingView Strategy Tester Backtest
1. Open **[`MultiConfluence_Signal_Strategy.pine`](file:///c:/Projects/trading/ChartSignalGenerator/MultiConfluence_Signal_Strategy.pine)**.
2. Copy the entire script content.
3. In TradingView's Pine Editor, click **Open** -> **New Strategy**, paste the script, and click **"Add to chart"**.
4. Click on the **Strategy Tester** tab at the bottom to inspect:
   - **Net Profit**
   - **Percent Profitable (Win Rate %)**
   - **Profit Factor**
   - **Max Drawdown %**
   - **List of Trades** with exact entry and exit prices!
5. The on-chart performance table summarises the same metrics plus expectancy and average win / loss.
6. Notes: the strategy uses `process_orders_on_close=true` so entries fill at the signal bar close (the same price the indicator reports). Optional **Risk % of Equity** sizing uses the ATR stop distance. Set **TP1 Trim Size** to `0` for a single-target exit.

### Step 4: Configure Real-Time Alerts
Two alert styles are available; use one or the other to avoid duplicate webhooks.

**Option A - one dynamic alert (recommended)**
1. Click the **Alert** button (clock icon) or press `Alt + A`.
2. Condition: **AlphaWave Options** -> **Any alert() function call**.
3. Enter your **Webhook URL** (e.g. `http://<host>:8000/webhook`). The script emits JSON for every event:
   `BUY_CALL`, `BUY_PUT`, `TP_CALL`, `TP_PUT` with `engine`, `reason`, `price`, `sl`, `tp1`, `tp2`, `pnl_pct`, `atr`, `rsi`, `time`.

**Option B - classic per-condition alerts**
1. Condition: **AlphaWave Options** -> `AlphaWave CALL Alert`, `AlphaWave PUT Alert`, `CALL Exit Alert`, or `PUT Exit Alert`.
2. Choose **Once Per Bar Close**.
3. Payloads reference plots by title (`{{plot("Stop Loss Level")}}` etc.), so SL / TP values are always correct regardless of plot ordering. Turn off **Send Dynamic JSON via alert()** in the indicator settings when using this option.

The dashboard (`app.py`) treats any `action` containing `TP` or `EXIT` as a position close, so every exit type is reported as `TP_CALL` / `TP_PUT` with the specific `reason` in the payload.

---

## 💻 How to Add to Webull Desktop

1. Open Webull Desktop and navigate to any stock or ETF chart.
2. Right-click the chart and select **Indicator Settings** (or click the Settings gear -> Indicators).
3. Click **Script Management** (or **Custom Indicators**) -> **Add New Indicator**.
4. Name the indicator `AlphaWave`.
5. Open **[`Webull_Custom_Indicator.txt`](file:///c:/Projects/trading/ChartSignalGenerator/Webull_Custom_Indicator.txt)**, copy the script, paste it into the editor, and click **Compile & Save**.
6. Check the box to display it on your main chart.

---

## 📊 Running Local Python Backtests & Generating Charts

You can backtest any stock, ETF, or cryptocurrency over any timeframe directly from your terminal:

```bash
# Backtest SPY over 3 years on Daily chart
python backtester.py --ticker SPY --period 3y --interval 1d

# Backtest Apple (AAPL)
python backtester.py --ticker AAPL --period 3y --interval 1d

# Backtest Bitcoin (BTC-USD)
python backtester.py --ticker BTC-USD --period 2y --interval 1d
```

### Interactive HTML Visualizer
Each run generates **`backtest_chart.html`**. Double-click this file or open it in your browser to inspect the interactive Plotly dark-mode chart with zoomable candlesticks, Buy/Sell markers, MACD histogram, and account equity curve.
