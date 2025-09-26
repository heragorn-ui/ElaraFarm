cd C:\ElaraFarm\server
py -3 -m venv .venv
.\.venv\Scripts\activate
pip install fastapi "uvicorn[standard]"
