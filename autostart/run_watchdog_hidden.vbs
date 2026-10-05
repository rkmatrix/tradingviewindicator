Option Explicit
Dim WshShell
Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""C:\Projects\trading\ChartSignalGenerator\autostart\health_watchdog.ps1""", 0, False
