@echo off
REM ============================================================
REM Vision Analytics Platform - vision-bench launcher (no make needed).
REM
REM WHY ASCII ONLY HERE: cmd.exe parses .cmd in the OEM codepage,
REM so Cyrillic comments in a UTF-8 file turn into bogus commands
REM (same reason up.cmd keeps English comments).
REM
REM PYTHONIOENCODING=utf-8 is REQUIRED: bench prints Cyrillic and
REM check/cross marks; on the default cp1251 console it crashes.
REM
REM   scripts\bench.cmd selftest
REM   scripts\bench.cmd preflight --video uploads\Praha_air.mp4
REM   scripts\bench.cmd run --profile quick ^
REM       --video uploads\Praha_air.mp4 --label manual
REM   scripts\bench.cmd compare bench\results\<run-A> bench\results\<run-B>
REM   scripts\bench.cmd cleanup
REM
REM Needs: venv (.venv) and the stack up (scripts\up.cmd).
REM Reports: bench\results\<run_id>\ (result.json, report.md, load_curve.png)
REM Methodology: bench\README.md
REM ============================================================
setlocal
set "ROOT=%~dp0.."
pushd "%ROOT%"
set "PYTHONIOENCODING=utf-8"
".venv\Scripts\python.exe" -m bench %*
set "RC=%ERRORLEVEL%"
popd
REM Popd/endlocal reset ERRORLEVEL, so the child's code must be propagated
REM explicitly: callers (CI gates, scripts) rely on a non-zero exit on failure.
REM Single line ON PURPOSE: %RC% expands before endlocal rolls the var back.
endlocal & exit /b %RC%
