' Launches the Indicator Signals background server without any console window
Option Explicit
Dim WshShell, fso, scriptDir, cmd, pythonExe
Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)

pythonExe = "C:\Users\rkmat\AppData\Local\Programs\Python\Python311\python.exe"
If Not fso.FileExists(pythonExe) Then
    pythonExe = "python.exe"
End If

cmd = """" & pythonExe & """ -m uvicorn app:app --host 127.0.0.1 --port 8800"
WshShell.CurrentDirectory = scriptDir
WshShell.Run cmd, 0, False
