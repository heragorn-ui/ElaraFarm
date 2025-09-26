@echo off
cd /d %~dp0
call .venv\Scripts\activate
set ELARA_USER_API_KEY=ELARA123
set ELARA_JOIN_SECRET=JOIN123
set ELARA_LOG_DIR=C:\ElaraFarm\worker\logs
set ELARA_USER_API_KEY=ELARA123
uvicorn server:app --host 127.0.0.1 --port 8000 --reload
pause

