"""
AlphaWave Options Confluence Engine: CALL & PUT Signal Backtester & Visualizer
Runs quantitative validation across stocks, ETFs, and crypto with interactive chart export.
"""

import os
import sys
import argparse
import numpy as np
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def run_backtest(ticker="SPY", period="2y", interval="1d", capital=10000.0, risk_pct=2.0, generate_chart=True):
    print(f"\n" + "="*75)
    print(f" FETCHING DATA & RUNNING ALPHAWAVE OPTIONS CONFLUENCE (CALL/PUT) FOR {ticker}")
    print(f" Period: {period} | Interval: {interval} | Initial Capital: ${capital:,.2f}")
    print("="*75, flush=True)
    
    data = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
    if data.empty:
        print(f"Error: Unable to fetch data for ticker '{ticker}'.", flush=True)
        return None
        
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [c[0] for c in data.columns]
        
    df = data.copy()
    close = df['Close']
    high = df['High']
    low = df['Low']
    vol = df['Volume']
    
    # 1. EMAs
    df['EMA9'] = close.ewm(span=9, adjust=False).mean()
    df['EMA21'] = close.ewm(span=21, adjust=False).mean()
    df['EMA50'] = close.ewm(span=50, adjust=False).mean()
    df['EMA200'] = close.ewm(span=200, adjust=False).mean()
    
    # 2. RSI (14)
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # 3. MACD (12, 26, 9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = macd - signal
    
    # 4. ATR (14)
    tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
    df['ATR'] = tr.rolling(14).mean()
    
    # 5. Volume Filter
    df['Vol_MA'] = vol.rolling(20).mean()
    df['Vol_Good'] = vol >= (df['Vol_MA'] * 0.85)
    
    # Options Regimes
    df['Bull_Regime'] = (close > df['EMA200']) & (df['EMA9'] > df['EMA21'])
    df['Bear_Regime'] = (close < df['EMA200']) & (df['EMA9'] < df['EMA21'])
    
    # Value Zone Retest
    df['Bull_Dip'] = (low <= df['EMA21'] * 1.012) & (close >= df['EMA50'] * 0.975)
    df['Bear_Rally'] = (high >= df['EMA21'] * 0.988) & (close <= df['EMA50'] * 1.025)
    
    # Reversal Triggers
    call_trig = ((df['RSI'] > 42) & (df['RSI'].shift(1) <= 42)) | ((df['MACD_Hist'] > 0) & (df['MACD_Hist'].shift(1) <= 0))
    put_trig  = ((df['RSI'] < 58) & (df['RSI'].shift(1) >= 58)) | ((df['MACD_Hist'] < 0) & (df['MACD_Hist'].shift(1) >= 0))
    
    df['CALL_Signal'] = df['Bull_Regime'] & df['Bull_Dip'] & call_trig & df['Vol_Good']
    df['PUT_Signal']  = df['Bear_Regime'] & df['Bear_Rally'] & put_trig & df['Vol_Good']
    
    # Execution
    current_cash = capital
    position = 0 # 1: CALL, -1: PUT
    entry_price = 0.0
    entry_idx = 0
    stop_loss = 0.0
    take_profit = 0.0
    shares = 0.0
    trades = []
    equity_curve = []
    timestamps = []
    
    start_idx = 200 if len(df) > 220 else 50
    
    for i in range(start_idx, len(df)):
        dt = df.index[i]
        c = df['Close'].iloc[i]
        h = df['High'].iloc[i]
        l = df['Low'].iloc[i]
        atr = df['ATR'].iloc[i]
        
        # Check active CALL
        if position == 1:
            if h >= take_profit:
                pnl = (take_profit - entry_price) * shares
                current_cash += pnl
                trades.append({
                    'Type': 'CALL', 'Entry_Date': entry_date, 'Exit_Date': dt,
                    'Entry': entry_price, 'Exit': take_profit,
                    'PnL': pnl, 'Pct': (take_profit - entry_price)/entry_price*100,
                    'Win': True, 'Reason': 'Take Profit'
                })
                position = 0
            elif l <= stop_loss:
                pnl = (stop_loss - entry_price) * shares
                current_cash += pnl
                trades.append({
                    'Type': 'CALL', 'Entry_Date': entry_date, 'Exit_Date': dt,
                    'Entry': entry_price, 'Exit': stop_loss,
                    'PnL': pnl, 'Pct': (stop_loss - entry_price)/entry_price*100,
                    'Win': False, 'Reason': 'Stop Loss'
                })
                position = 0
            elif df['PUT_Signal'].iloc[i] or (i - entry_idx >= 15):
                pnl = (c - entry_price) * shares
                current_cash += pnl
                trades.append({
                    'Type': 'CALL', 'Entry_Date': entry_date, 'Exit_Date': dt,
                    'Entry': entry_price, 'Exit': c,
                    'PnL': pnl, 'Pct': (c - entry_price)/entry_price*100,
                    'Win': pnl > 0, 'Reason': 'Time/Reversal Exit'
                })
                position = 0
                
        # Check active PUT
        elif position == -1:
            if l <= take_profit:
                pnl = (entry_price - take_profit) * shares
                current_cash += pnl
                trades.append({
                    'Type': 'PUT', 'Entry_Date': entry_date, 'Exit_Date': dt,
                    'Entry': entry_price, 'Exit': take_profit,
                    'PnL': pnl, 'Pct': (entry_price - take_profit)/entry_price*100,
                    'Win': True, 'Reason': 'Take Profit'
                })
                position = 0
            elif h >= stop_loss:
                pnl = (entry_price - stop_loss) * shares
                current_cash += pnl
                trades.append({
                    'Type': 'PUT', 'Entry_Date': entry_date, 'Exit_Date': dt,
                    'Entry': entry_price, 'Exit': stop_loss,
                    'PnL': pnl, 'Pct': (entry_price - stop_loss)/entry_price*100,
                    'Win': False, 'Reason': 'Stop Loss'
                })
                position = 0
            elif df['CALL_Signal'].iloc[i] or (i - entry_idx >= 15):
                pnl = (entry_price - c) * shares
                current_cash += pnl
                trades.append({
                    'Type': 'PUT', 'Entry_Date': entry_date, 'Exit_Date': dt,
                    'Entry': entry_price, 'Exit': c,
                    'PnL': pnl, 'Pct': (entry_price - c)/entry_price*100,
                    'Win': pnl > 0, 'Reason': 'Time/Reversal Exit'
                })
                position = 0
                
        # New Entry
        if position == 0:
            risk_budget = current_cash * (risk_pct / 100.0)
            if df['CALL_Signal'].iloc[i]:
                position = 1
                entry_price = c
                entry_date = dt
                entry_idx = i
                stop_loss = entry_price - (1.2 * atr)
                take_profit = entry_price + (1.8 * atr)
                per_share_risk = entry_price - stop_loss
                shares = (risk_budget / per_share_risk) if per_share_risk > 0 else (current_cash / entry_price)
            elif df['PUT_Signal'].iloc[i]:
                position = -1
                entry_price = c
                entry_date = dt
                entry_idx = i
                stop_loss = entry_price + (1.2 * atr)
                take_profit = entry_price - (1.8 * atr)
                per_share_risk = stop_loss - entry_price
                shares = (risk_budget / per_share_risk) if per_share_risk > 0 else (current_cash / entry_price)
                
        equity_curve.append(current_cash)
        timestamps.append(dt)
        
    trades_df = pd.DataFrame(trades)
    net_profit = current_cash - capital
    total_return_pct = (net_profit / capital) * 100
    
    if len(trades_df) > 0:
        win_trades = trades_df[trades_df['Win'] == True]
        loss_trades = trades_df[trades_df['Win'] == False]
        pass_pct = (len(win_trades) / len(trades_df)) * 100
        gross_profit = win_trades['PnL'].sum() if len(win_trades) > 0 else 0.0
        gross_loss = abs(loss_trades['PnL'].sum()) if len(loss_trades) > 0 else 1.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.nan
        calls = trades_df[trades_df['Type'] == 'CALL']
        puts = trades_df[trades_df['Type'] == 'PUT']
        call_pass = (calls['Win'].sum() / len(calls) * 100) if len(calls) > 0 else 0.0
        put_pass = (puts['Win'].sum() / len(puts) * 100) if len(puts) > 0 else 0.0
    else:
        pass_pct, profit_factor, call_pass, put_pass = 0, 0, 0, 0
        calls, puts = pd.DataFrame(), pd.DataFrame()
        
    eq_series = pd.Series(equity_curve)
    peak = eq_series.cummax()
    drawdown = (eq_series - peak) / peak * 100
    max_drawdown = drawdown.min()
    
    print("\n" + "="*55)
    print(f" ALPHAWAVE OPTIONS PERFORMANCE REPORT: {ticker}")
    print("="*55)
    print(f" Initial Capital : ${capital:,.2f}")
    print(f" Ending Capital  : ${current_cash:,.2f}")
    print(f" Net Profit      : ${net_profit:,.2f} ({total_return_pct:+.2f}%)")
    print(f" Total Trades    : {len(trades_df)}")
    print(f" Overall Pass %  : {pass_pct:.2f}% (Win Rate)")
    print(f" CALL Trades     : {len(calls)} (Pass Rate: {call_pass:.1f}%)")
    print(f" PUT Trades      : {len(puts)} (Pass Rate: {put_pass:.1f}%)")
    print(f" Profit Factor   : {profit_factor:.2f}")
    print(f" Max Drawdown    : {max_drawdown:.2f}%")
    print("="*55 + "\n", flush=True)
    
    if generate_chart and len(df) > 0:
        create_interactive_chart(df, trades_df, timestamps, equity_curve, ticker)
        
    return {
        'ticker': ticker, 'net_profit': net_profit, 'return_pct': total_return_pct,
        'trades': len(trades_df), 'pass_pct': pass_pct, 'profit_factor': profit_factor,
        'max_drawdown': max_drawdown, 'trades_df': trades_df
    }

def create_interactive_chart(df, trades_df, timestamps, equity_curve, ticker):
    fig = make_subplots(
        rows=3, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.04, 
        row_heights=[0.6, 0.2, 0.2],
        subplot_titles=(f"AlphaWave Options Signals (CALL & PUT): {ticker}", "MACD & RSI Momentum", "Account Equity Curve ($)")
    )
    
    fig.add_trace(go.Candlestick(
        x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
        name="Price", increasing_line_color='#00E676', decreasing_line_color='#FF5252'
    ), row=1, col=1)
    
    fig.add_trace(go.Scatter(x=df.index, y=df['EMA9'], name="EMA 9 (Fast)", line=dict(color='#00E676', width=1.2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['EMA21'], name="EMA 21 (Med)", line=dict(color='#FFD600', width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['EMA50'], name="EMA 50 (Slow)", line=dict(color='#FF9100', width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['EMA200'], name="EMA 200 (Macro)", line=dict(color='#2979FF', width=2.5)), row=1, col=1)
    
    call_signals = df[df['CALL_Signal'] == True]
    put_signals = df[df['PUT_Signal'] == True]
    
    fig.add_trace(go.Scatter(
        x=call_signals.index, y=call_signals['Low'] * 0.99,
        mode='markers+text',
        marker=dict(symbol='triangle-up', size=14, color='#00E676', line=dict(width=1, color='white')),
        text="CALL", textposition="bottom center", textfont=dict(color='#00E676', size=11, family='Arial Black'),
        name="CALL Signal"
    ), row=1, col=1)
    
    fig.add_trace(go.Scatter(
        x=put_signals.index, y=put_signals['High'] * 1.01,
        mode='markers+text',
        marker=dict(symbol='triangle-down', size=14, color='#FF1744', line=dict(width=1, color='white')),
        text="PUT", textposition="top center", textfont=dict(color='#FF1744', size=11, family='Arial Black'),
        name="PUT Signal"
    ), row=1, col=1)
    
    hist_colors = ['#00E676' if val >= 0 else '#FF5252' for val in df['MACD_Hist']]
    fig.add_trace(go.Bar(
        x=df.index, y=df['MACD_Hist'], name="MACD Hist", marker_color=hist_colors
    ), row=2, col=1)
    
    fig.add_trace(go.Scatter(
        x=timestamps, y=equity_curve, name="Portfolio Equity", line=dict(color='#00B0FF', width=2.5),
        fill='tozeroy', fillcolor='rgba(0, 176, 255, 0.1)'
    ), row=3, col=1)
    
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#111318",
        plot_bgcolor="#181B22",
        xaxis_rangeslider_visible=False,
        height=900,
        margin=dict(l=40, r=40, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    output_html = "backtest_chart.html"
    fig.write_html(output_html)
    print(f"--> Interactive Backtest Chart saved to: {os.path.abspath(output_html)}", flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AlphaWave Options Backtester (CALL & PUT)")
    parser.add_argument("--ticker", type=str, default="QQQ", help="Ticker symbol (e.g. QQQ, SPY, AAPL, NVDA)")
    parser.add_argument("--period", type=str, default="2y", help="Historical period (e.g. 1y, 2y, 3y)")
    parser.add_argument("--interval", type=str, default="1d", help="Bar interval (e.g. 1d, 1h, 15m)")
    parser.add_argument("--capital", type=float, default=10000.0, help="Initial capital in USD")
    args = parser.parse_args()
    
    run_backtest(ticker=args.ticker, period=args.period, interval=args.interval, capital=args.capital)
