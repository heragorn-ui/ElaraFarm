cd C:\ElaraFarm\server
py -3 -m venv .venv
.\.venv\Scripts\activate
python -m pip install -U pip
pip install fastapi "uvicorn[standard]"
