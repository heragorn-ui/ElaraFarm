# -*- coding: utf-8 -*-
"""
ElaraFarm - Worker (stable)
- Register: supports {"worker_id":..., "api_key":...}
- next_job: GET with id/worker_id (+ api_key) to avoid 405/422
- job_update: sends api_key
- Pause (immediate), NIMBY (finish frame), Resume from first missing
- D1 done-detection (min size + quiet time)
- Debug logs when ELARA_DEBUG=1
"""

import os
import re
import time
import subprocess
from pathlib import Path
from typing import Set, List, Optional

import requests

# ---------- Env / globals ----------
SERVER = os.environ.get("ELARA_SERVER", "http://127.0.0.1:8000").rstrip("/")
JOIN_SECRET = os.environ.get("ELARA_JOIN_SECRET", "JOIN123")
WORKER_NAME = os.environ.get("ELARA_WORKER_NAME", os.environ.get("COMPUTERNAME", "worker"))
LOG_DIR = Path(os.environ.get("ELARA_LOG_DIR", str(Path.cwd() / "logs")))

DEBUG = os.environ.get("ELARA_DEBUG", "0") not in ("", "0", "false", "False")

def dlog(*a):
    if DEBUG:
        print(*a)

# Prefer server task env (same value as server's ELARA_USER_API_KEY).
# Fall back to whatever register_worker returns (if any).

API_KEY: str = os.environ.get("ELARA_USER_API_KEY", "")  # primary
REG_API: str = ""                                       # from register, optional

def auth_params() -> dict:
    d = {}
    if API_KEY:
        # mevcutlar:
        d["api_key"] = API_KEY
        d["user_api_key"] = API_KEY
        # EKLER (server farklı isim bekliyorsa)
        d["USER_API_KEY"] = API_KEY
        d["userApiKey"] = API_KEY
    if REG_API:
        d.setdefault("api_key", REG_API)
    return d


def auth_headers() -> dict:
    h = {}
    if API_KEY:
        # mevcutlar:
        h["X-API-KEY"] = API_KEY
        h["Authorization"] = f"Bearer {API_KEY}"
        # EKLER (bazı backend'ler bunu bekler)
        h["X-USER-API-KEY"] = API_KEY
    return h



IMAGE_EXTS = (".exr", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".hdr", ".tx", ".tga")
# Kare numarası regex (esnek)
FRAME_RE = re.compile(r"(?:^|[^\d])(\d{1,6})(?:[^\d]|$)")

# D1 thresholds
MIN_SIZE_KB = int(os.environ.get("ELARA_MIN_FRAME_SIZE_KB", "128"))
QUIET_SEC   = float(os.environ.get("ELARA_FRAME_QUIET_SEC", "2.5"))
NIMBY_TIMEOUT_SEC = float(os.environ.get("ELARA_NIMBY_TIMEOUT_SEC", "600"))

EXT_MIN_KB = {
    ".png": 32, ".jpg": 16, ".jpeg": 16, ".bmp": 32,
    ".tif": 64, ".tiff": 64,
    ".exr": MIN_SIZE_KB
}

def find_render_exe() -> str:
    for c in (
        r"C:\Program Files\Autodesk\Maya2025\bin\Render.exe",
        r"C:\Program Files\Autodesk\Maya2024\bin\Render.exe",
        r"C:\Program Files\Autodesk\Maya2023\bin\Render.exe",
    ):
        if Path(c).exists():
            return c
    return "Render.exe"

RENDER_EXE = find_render_exe()
session = requests.Session()
LOG_DIR.mkdir(parents=True, exist_ok=True)

print("=== Elara Worker ===")
print("SERVER:", SERVER)
print("RENDER_EXE:", RENDER_EXE)

# ---------- helpers ----------

def list_done_frames(output_dir: str, start: int, end: int) -> Set[int]:
    """Scan output dir and collect frames that look fully written (D1 filter)."""
    root = Path(output_dir or "")
    s: Set[int] = set()
    if not root.exists():
        return s

    now_ts = time.time()
    try:
        for f in root.rglob("*"):
            if not f.is_file():
                continue
            ext = f.suffix.lower()
            if ext not in IMAGE_EXTS:
                continue
            try:
                st = f.stat()
            except Exception:
                continue
            min_kb = EXT_MIN_KB.get(ext, MIN_SIZE_KB)
            if st.st_size < min_kb * 1024:
                continue
            if (now_ts - st.st_mtime) < QUIET_SEC:
                continue

            m = FRAME_RE.search(f.stem)
            if not m:
                continue
            try:
                fr = int(m.group(1))
                if start <= fr <= end:
                    s.add(fr)
            except Exception:
                pass
    except Exception as e:
        print("[worker] rglob error:", e)

    return s


def align_done_to_step(done: Set[int], start: int, end: int, step: int) -> List[int]:
    if step <= 0:
        step = 1
    seq: List[int] = []
    fr = start
    while fr <= end:
        if fr in done:
            seq.append(fr)
        else:
            break
        fr += step
    return seq


def first_missing(start: int, end: int, step: int, aligned_done: List[int]) -> int:
    if step <= 0:
        step = 1
    fr = start
    for d in aligned_done:
        if d == fr:
            fr += step
        else:
            break
    return fr


def build_render_cmd(job: dict, start_frame: int) -> List[str]:
    scene = job["scene"]
    proj  = job.get("project_root") or ""
    out   = job.get("output_dir") or ""
    width = int(job.get("width") or 1920)
    height= int(job.get("height") or 1080)
    rend  = job.get("renderer") or "arnold"
    cam   = job.get("camera") or ""
    layer = job.get("layer") or ""
    end_frame = int(job.get("end") or start_frame)
    step = int(job.get("step") or 1)

    args = [RENDER_EXE, "-r", rend, "-s", str(start_frame), "-e", str(end_frame), "-b", str(step)]
    if proj:
        args += ["-proj", proj]
    if out:
        args += ["-rd", out]
    args += ["-x", str(width), "-y", str(height)]
    if cam:
        args += ["-cam", cam]
    if layer:
        args += ["-rl", layer]
    args += [scene]
    return args

# ---------- server comms ----------

def register() -> int:
    """Register worker and store api_key (server returns {"worker_id":..,"api_key":..})."""
    global API_KEY
    try:
        r = session.post(f"{SERVER}/register_worker", json={
            "join_secret": JOIN_SECRET,
            "name": WORKER_NAME
        }, timeout=10)
        raw = r.text
        r.raise_for_status()
        data = r.json()
        wid = int(data.get("id") or data.get("worker_id") or 0)
        api = data.get("api_key") or ""
        global REG_API
        REG_API = api
        
        if wid <= 0:
            print("[worker] register response:", raw)
        else:
            print(f"[worker] registered id={wid}")
        return wid
    except Exception as e:
        print("[worker] register error:", e)
        time.sleep(3)
        return 0


def poll_next_job(worker_id: int) -> Optional[dict]:
    """Ask server for next job (GET; include id/worker_id and api key in both params and headers)."""
    params = {"id": worker_id, "worker_id": worker_id}
    params.update(auth_params())
    headers = auth_headers()
    try:
        r = session.get(f"{SERVER}/next_job", params=params, headers=headers, timeout=15)
        if r.status_code == 204:
            return None
        r.raise_for_status()
        data = r.json()
        # Bazı serverlar hata JSON'u dönebilir; koruma
        if not isinstance(data, dict) or ("id" not in data and "job" not in data):
            return None
        # data "job" içinde gelebilir
        if "job" in data and isinstance(data["job"], dict):
            return data["job"]
        return data
    except Exception:
        return None



def post_update(job_id: int, payload: dict) -> dict:
    """Report status back to server (include api_key)."""
    try:
        p = dict(payload)
        p["job_id"] = job_id
        if API_KEY:
            p["api_key"] = API_KEY
        headers = auth_headers()
        p.update(auth_params())  # bazı serverlar body'de bekliyor
        r = session.post(f"{SERVER}/job_update", json=p, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}

# ---------- core ----------

def run_render(job: dict) -> None:
    job_id = int(job["id"])
    start = int(job.get("start") or 1)
    end   = int(job.get("end") or start)
    step  = int(job.get("step") or 1)
    output = job.get("output_dir") or ""

    # disk taraması
    existing = list_done_frames(output, start, end)
    dlog(f"[debug] output_dir={output}")
    dlog(f"[debug] found done frames (raw) = {sorted(list(existing))[:12]} ... total={len(existing)}")
    aligned = align_done_to_step(existing, start, end, step)
    dlog(f"[debug] aligned_done={aligned[:12]} ... total={len(aligned)}")

    resume_start = first_missing(start, end, step, aligned)
    print(f"[worker] resume start → frame {resume_start} (was {start})")

    # server progress fallback (varsa)
    srv_done = int(job.get("frame_done") or 0)
    if resume_start == start and srv_done > 0:
        fr = start
        for _ in range(srv_done):
            fr += step
        if fr <= end:
            print(f"[worker] resume fallback → server frame_done={srv_done}, start={fr}")
            resume_start = fr

    cmd = build_render_cmd(job, resume_start)
    print("[worker] launching:", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    graceful_pending = False
    graceful_done_mark = 0
    nimby_armed_ts = 0.0

    post_update(job_id, {"status": "running", "pid": proc.pid})

    try:
        while True:
            line = proc.stdout.readline()
            if line:
                # TODO: burada renderer stdout parser eklenebilir
                pass

            code = proc.poll()

            # ilerleme
            now_done = list_done_frames(output, start, end)
            current_done_count = len(align_done_to_step(now_done, start, end, step))

            resp = post_update(job_id, {"done": current_done_count, "status": "running", "pid": proc.pid})
            cv = resp.get("cancel", 0)
            try:
                cancel_code = int(cv) if not isinstance(cv, bool) else (1 if cv else 0)
            except Exception:
                cancel_code = 0

            dlog(f"[debug] cancel_code={cancel_code}  done={current_done_count}")

            # NIMBY'yi bir kez kur
            if cancel_code == 2 and not graceful_pending:
                graceful_pending = True
                graceful_done_mark = current_done_count
                nimby_armed_ts = time.time()
                print(f"[worker] NIMBY armed at done={graceful_done_mark}")

            should_terminate = False

            # Pause (immediate)
            if cancel_code == 1:
                print("[worker] Pause (immediate) → terminate now")
                should_terminate = True

            # NIMBY: bir kare daha biterse dur
            elif cancel_code == 2 and graceful_pending:
                if (time.time() - nimby_armed_ts) > NIMBY_TIMEOUT_SEC:
                    print("[worker] NIMBY safety timeout → terminate")
                    should_terminate = True
                elif current_done_count > graceful_done_mark:
                    print(f"[worker] NIMBY: +{current_done_count - graceful_done_mark} frame (D1-ok) → terminate")
                    should_terminate = True

            if should_terminate:
                try:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        print("[worker] renderer did not exit in time → kill()")
                        proc.kill()
                except Exception as te:
                    print("[worker] terminate error:", te)
                break

            if code is not None:
                break

            time.sleep(0.2)

    finally:
        final_done = list_done_frames(output, start, end)
        aligned_final = align_done_to_step(final_done, start, end, step)
        post_update(job_id, {"status": "done", "done": len(aligned_final)})

# ---------- main ----------

def main():
    wid = 0
    while wid == 0:
        wid = register()
        if wid == 0:
            time.sleep(2)

    while True:
        job = poll_next_job(wid)
        if not job:
            time.sleep(1.0)
            continue
        try:
            s = int(job.get("start") or 0)
            e = int(job.get("end") or 0)
            jid = job.get("id")
            print(f"[worker] got job id={jid} {s}-{e}")
            if jid is None:
                # muhtemelen yetkisiz/hata JSON'u geldi → bekle ve tekrar dene
                time.sleep(1.0)
                continue
            run_render(job)
        except Exception as e:
            print("[worker] run_render error:", e)
            try:
                post_update(int(job.get("id", 0)), {"status": "failed"})
            except Exception:
                pass

if __name__ == "__main__":
    main()
