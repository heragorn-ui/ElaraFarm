@echo off
title ElaraFarm Worker (debug)
setlocal
cd /d %~dp0

if not exist ".venv\Scripts\python.exe" (
  py -3 -m venv .venv
)

call .venv\Scripts\activate
python -m pip install -U pip >nul 2>&1
pip show requests >nul 2>&1 || pip install requests

set ELARA_SERVER=http://127.0.0.1:8000
set ELARA_JOIN_SECRET=MY_JOIN_SECRET
set ELARA_WORKER_NAME=%COMPUTERNAME%

echo.
echo === PYTHON ===
python -V
python -c "import requests; print('Requests OK')" || goto :ERR

echo.
echo Starting worker...
python worker.py
echo.
echo ===== Worker stopped =====
pause
goto :EOF

:ERR
echo.
echo ***** WORKER START FAILED *****
pause
