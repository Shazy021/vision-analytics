@echo off
REM ============================================================
REM Vision Analytics Platform - Windows starter (no make needed).
REM
REM Why .cmd instead of .ps1? PowerShell blocks unsigned .ps1
REM scripts by default (ExecutionPolicy error). cmd.exe is not
REM subject to that policy and launches PowerShell with
REM -ExecutionPolicy Bypass, so this works out of the box.
REM
REM   scripts\up.cmd                 start the full stack
REM   scripts\up.cmd down            stop
REM   scripts\up.cmd logs            follow logs
REM   scripts\up.cmd ps              service status
REM
REM GPU is auto-detected via nvidia-smi (see up.ps1).
REM ============================================================
setlocal
set "SCRIPT_DIR=%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%up.ps1" %*
endlocal
