# Start the AlphaWave Indicator Signals Dashboard & Telegram Dispatcher
Write-Host "Starting Indicator Signals Dashboard on http://127.0.0.1:8800..." -ForegroundColor Cyan
python -m uvicorn app:app --host 127.0.0.1 --port 8800 --reload
