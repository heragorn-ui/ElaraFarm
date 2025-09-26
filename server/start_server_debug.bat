@echo off
title ElaraFarm Server (debug)
setlocal
cd /d %~dp0

:: --- venv yoksa oluştur ---
if not exist ".venv\Scripts\python.exe" (
  py -3 -m venv .venv
)

call .venv\Scripts\activate
python -m pip install -U pip >nul 2>&1

:: --- gerekli paketler ---
pip show fastapi >nul 2>&1 || pip install fastapi
pip show uvicorn >nul 2>&1 || pip install "uvicorn[standard]"

:: --- ortam değişkenleri ---
set ELARA_USER_API_KEY=MY_USER_KEY
set ELARA_JOIN_SECRET=MY_JOIN_SECRET
set ELARA_LOG_DIR=C:\ElaraFarm\worker\logs

echo.
echo === PYTHON ===
python -V
echo Import test...
python -c "import fastapi,uvicorn,starlette; print('Imports OK')" || goto :ERR

echo.
echo Starting server with uvicorn...
echo (Window stays open; errors will be shown here)
uvicorn server:app --host 127.0.0.1 --port 8000 --reload
echo.
echo ===== Server stopped =====
pause
goto :EOF

:ERR
echo.
echo ***** STARTUP FAILED *****
echo Check that "server.py" is in this folder.
echo Also ensure no local files named fastapi.py / uvicorn.py shadow the packages.
pause
