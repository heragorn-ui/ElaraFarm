cd C:\ElaraFarm\worker
.\.venv\Scripts\activate
set ELARA_SERVER=http://127.0.0.1:8000
set ELARA_JOIN_SECRET=MY_JOIN_SECRET
set ELARA_WORKER_NAME=DESKTOP-ABC
python worker.py
