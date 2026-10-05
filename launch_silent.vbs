' =============================================================================
' Indicator Signals Auto-Launcher (Completely Silent, Zero Console Popups)
' 1. Launches FastAPI server on 127.0.0.1:8800 via Watchdog if not running
' 2. Opens the live dashboard in default web browser
' =============================================================================
Option Explicit
Dim WshShell, fso, scriptDir, watchdogVbs, http, isRunning, i

Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
watchdogVbs = scriptDir & "\autostart\run_watchdog_hidden.vbs"

Function IsServerHealthy()
    Dim testHttp
    On Error Resume Next
    Set testHttp = CreateObject("MSXML2.ServerXMLHTTP.6.0")
    testHttp.Open "GET", "http://127.0.0.1:8800/healthz", False
    testHttp.setTimeouts 1000, 1000, 1000, 1000
    testHttp.Send
    If Err.Number = 0 And testHttp.Status = 200 Then
        IsServerHealthy = True
    Else
        IsServerHealthy = False
    End If
    On Error GoTo 0
End Function

If Not IsServerHealthy() Then
    If fso.FileExists(watchdogVbs) Then
        WshShell.Run "wscript.exe """ & watchdogVbs & """", 0, False
    Else
        WshShell.Run "wscript.exe """ & scriptDir & "\run_hidden.vbs""", 0, False
    End If

    ' Wait up to 15 seconds for health
    For i = 1 To 15
        WScript.Sleep 1000
        If IsServerHealthy() Then Exit For
    Next
End If

' Launch dashboard in default browser
WshShell.Run "http://127.0.0.1:8800/"
