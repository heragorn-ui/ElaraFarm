@echo off
cd /d %~dp0
call .venv\Scripts\activate
set ELARA_SERVER=http://127.0.0.1:8000
set ELARA_JOIN_SECRET=JOIN123
set ELARA_WORKER_NAME=%COMPUTERNAME%
python worker.py
pause



