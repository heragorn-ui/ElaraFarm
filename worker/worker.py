# -*- coding: utf-8 -*-
# ElaraFarm Worker v0.9.8 — Pause (immediate) & NIMBY (after-frame) + resume from first missing frame

import os, re, time, threading, subprocess, shutil
from pathlib import Path
from typing import Dict, Any, Set, List, Optional
import requests
DEBUG = os.environ.get("ELARA_DEBUG", "0") not in ("", "0", "false", "False")
def dlog(*a):
    if DEBUG:
        print(*a)

SERVER      = os.environ.get("ELARA_SERVER", "http://127.0.0.1:8000")
JOIN_SECRET = os.environ.get("ELARA_JOIN_SECRET", "CHANGE_ME")
WORKER_NAME = os.environ.get("ELARA_WORKER_NAME", os.environ.get("COMPUTERNAME","worker"))

LOG_DIR     = Path(os.environ.get("ELARA_LOG_DIR", r"C:\ElaraFarm\worker\logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# --- D1 thresholds (done-detection) ---
# A frame file is considered "done" only if:
#  - size >= MIN_SIZE_KB (to avoid partial files)
#  - last modification time is older than QUIET_SEC (file not being written anymore)
MIN_SIZE_KB = int(os.environ.get("ELARA_MIN_FRAME_SIZE_KB", "128"))
QUIET_SEC   = float(os.environ.get("ELARA_FRAME_QUIET_SEC", "2.5"))


# Try to locate Maya Render.exe
RENDER_EXE_CANDIDATES = [
    os.environ.get("ELARA_RENDER_EXE") or "",
    r"C:\Program Files\Autodesk\Maya2025\bin\Render.exe",
    r"C:\Program Files\Autodesk\Maya2024\bin\Render.exe",
    r"C:\Program Files\Autodesk\Maya2023\bin\Render.exe",
    "Render.exe",
]
def find_render_exe()->str:
    for p in RENDER_EXE_CANDIDATES:
        if not p: continue
        pp=Path(p)
        if pp.exists(): return str(pp)
        w=shutil.which(str(pp))
        if w: return w
    return "Render.exe"
RENDER_EXE=find_render_exe()

session=requests.Session()
WORKER_ID=None; API_KEY=None

FRAME_RE = re.compile(r"(?:^|[^\d])(\d{1,6})(?:[^\d]|$)")
IMAGE_EXTS = (".exr",".png",".jpg",".jpeg",".tif",".tiff",".bmp",".hdr",".tx",".tga")

# Per-extension minimal sizes (KB) — if not specified, MIN_SIZE_KB is used
EXT_MIN_KB = {
    ".png": 32,
    ".jpg": 16,
    ".jpeg": 16,
    ".bmp": 32,
    ".tif": 64,
    ".tiff": 64,
    ".exr": MIN_SIZE_KB,  # use global default for EXR
}
# ...
ext = f.suffix.lower()
min_kb = EXT_MIN_KB.get(ext, MIN_SIZE_KB)
if st.st_size < min_kb * 1024:
    continue

def list_done_frames(output_dir:str, start:int, end:int)->Set[int]:
    """Scan output directory and collect frames that look fully written (D1 filter)."""
    root = Path(output_dir or "")
    s: Set[int] = set()
    if not root.exists():
        return s

    now_ts = time.time()
    try:
        for f in root.rglob("*"):
            if not f.is_file():
                continue
            if f.suffix.lower() not in IMAGE_EXTS:
                continue

            # D1 filter: size + quiet-time
            try:
                st = f.stat()
            except Exception:
                continue
            ext = f.suffix.lower()
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
            except:
                pass
    except Exception as e:
        print("[worker] rglob error:", e)

    return s


def first_missing(start:int, end:int, step:int, done:Set[int]) -> Optional[int]:
    """Find first missing frame in [start..end] stepping by 'step'. Returns None if all done."""
    fr = int(start)
    st = max(1, int(step))
    while fr <= int(end):
        if fr not in done:
            return fr
        fr += st
    return None

def post_json(url:str, payload:Dict[str,Any])->Dict[str,Any]:
    r=session.post(f"{SERVER}{url}", json=payload, timeout=15)
    r.raise_for_status()
    return r.json()

def register():
    global WORKER_ID, API_KEY
    r=session.post(f"{SERVER}/register_worker", json={"join_secret":JOIN_SECRET,"name":WORKER_NAME}, timeout=10)
    r.raise_for_status()
    data=r.json()
    WORKER_ID=data["worker_id"]; API_KEY=data["api_key"]
    print(f"[worker] registered id={WORKER_ID}")

def get_next_job():
    r=session.get(f"{SERVER}/next_job", params={"worker_id":WORKER_ID,"api_key":API_KEY}, timeout=10)
    r.raise_for_status()
    return r.json().get("job")

def run_render(job:Dict[str,Any]):
    jid=job["id"]; scene=job["scene"]; project=job["project"]; output=job["output_dir"]
    camera=job.get("camera") or ""; layer=job.get("layer") or ""
    start=int(job["start_frame"]); end=int(job["end_frame"]); step=max(1,int(job.get("by_step") or 1))
    width=int(job.get("width") or 1920); height=int(job.get("height") or 1080)
    renderer=(job.get("renderer") or "arnold").lower()
    frame_total=((end-start)//step)+1
   
    # Fallback: if disk scan found nothing but server remembers progress, respect it
    srv_done = int(job.get("frame_done") or 0)
    if resume_start == start and srv_done > 0:
        # compute frame number from srv_done
        fr = start
        step = max(1, step)
        # advance srv_done frames
        for _ in range(srv_done):
            fr += step
        if fr <= end:
            print(f"[worker] resume fallback → using server frame_done={srv_done}, start={fr}")
            resume_start = fr

    # --- Resume logic: detect already-rendered frames and start from the first missing one ---
    existing_done = list_done_frames(output, start, end)
    aligned_done: Set[int] = {fr for fr in existing_done if (fr - start) % step == 0}
    dlog(f"[debug] output_dir={output}")
    dlog(f"[debug] found done frames (raw) = {sorted(list(existing_done))[:12]} ... total={len(existing_done)}")

    # If everything is already rendered, finish without launching Render.exe
    if len(aligned_done) >= frame_total:
        try:
            post_json("/job_update", {
                "worker_id": WORKER_ID, "api_key": API_KEY, "job_id": jid,
                "status": "done", "frame_total": frame_total,
                "frame_done": len(aligned_done), "frame_failed": 0,
                "frame_running": 0, "log_tail": "resume: all frames already present on disk"
            })
        except Exception as e:
            print("[worker] finalize-done update failed:", e)
        return

    resume_start = first_missing(start, end, step, aligned_done)
    dlog(f"[debug] aligned_done={sorted(list(aligned_done))[:12]} ... total={len(aligned_done)}")
    dlog(f"[debug] resume_start={resume_start}  (start={start}, end={end}, step={step})")

    if resume_start is None:  # safety (same as all-done)
        try:
            post_json("/job_update", {
                "worker_id": WORKER_ID, "api_key": API_KEY, "job_id": jid,
                "status": "done", "frame_total": frame_total,
                "frame_done": len(aligned_done), "frame_failed": 0,
                "frame_running": 0, "log_tail": "resume: no missing frames"
            })
        except Exception as e:
            print("[worker] finalize-done update failed:", e)
        return

    print(f"[worker] resume start → frame {resume_start} (was {start})")

    log_path=LOG_DIR/f"job_{jid}.log"; tail:List[str]=[]

    def push_tail(line:str):
        if not line: return
        tail.append(line.rstrip("\n"))
        if len(tail)>200: del tail[:len(tail)-200]

    # Respect Maya file naming: do NOT pass -im/-of (let Maya/Layers handle file names)
    cmd=[RENDER_EXE,"-r",renderer,"-s",str(resume_start),"-e",str(end),"-b",str(step),
         "-proj",project,"-rd",output,"-x",str(width),"-y",str(height)]
    if camera: cmd+=["-cam",camera]
    if layer:  cmd+=["-rl",layer]
    cmd+=[scene]

    print("[worker] launching:", " ".join(cmd))

    log_f=open(log_path,"w",encoding="utf-8",errors="replace")
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, encoding="utf-8", errors="replace")

    def reader():
        for line in proc.stdout:
            log_f.write(line); push_tail(line)
        try: proc.stdout.close()
        except: pass
    t=threading.Thread(target=reader,daemon=True); t.start()

    # initial update
    try:
        post_json("/job_update", {"worker_id":WORKER_ID,"api_key":API_KEY,"job_id":jid,
                                  "status":"running","frame_total":frame_total,
                                  "frame_done":len(aligned_done),
                                  "frame_failed":0,"frame_running":1,"log_tail":"\n".join(tail)})
    except Exception as e:
        print("[worker] first update failed:", e)

    # --- Control state for Pause/NIMBY ---
    prev_done: Set[int] = set(aligned_done)
    graceful_pending = False          # armed when cancel_code==2 first seen
    graceful_done_mark = len(prev_done)  # done count at the moment NIMBY is requested

    # main loop
    while True:
        time.sleep(2.0)
        code = proc.poll()

        # scan disk again and align to step
        cur_done = list_done_frames(output, start, end)
        cur_aligned: Set[int] = {fr for fr in cur_done if (fr - start) % step == 0}
        delta = sorted(list(cur_aligned - prev_done))
        prev_done = cur_aligned
        current_done_count = len(cur_aligned)

        if delta:
            try:
                post_json("/frame_update", {"worker_id":WORKER_ID,"api_key":API_KEY,"job_id":jid,"frames_done":delta})
            except Exception as e:
                print("[worker] frame_update error:", e)

        # periodic job_update → read cancel code
        try:
            resp = post_json("/job_update", {"worker_id":WORKER_ID,"api_key":API_KEY,"job_id":jid,"status":"running",
                                             "frame_total":frame_total,"frame_done":current_done_count,"frame_failed":0,
                                             "frame_running":1,"log_tail":"\n".join(tail)})
            # parse cancel: 0 none, 1 immediate (Pause), 2 graceful (NIMBY)
            cv = resp.get("cancel", 0)
            try:
                cancel_code = int(cv) if not isinstance(cv, bool) else (1 if cv else 0)
            except Exception:
                cancel_code = 0
        except Exception as e:
            print("[worker] update error:", e)
            cancel_code = 0

                # Log which cancel code we got (0 none, 1 immediate, 2 graceful)
        dlog(f"[debug] cancel_code={cancel_code}  done={current_done_count} fail=0 run=1")

        # Arm NIMBY once
        if cancel_code == 2 and not graceful_pending:
            graceful_pending = True
            graceful_done_mark = current_done_count
            nimby_armed_ts = time.time()
            print(f"[worker] NIMBY armed at done={graceful_done_mark}")

        # Decide termination
        should_terminate = False

        # Immediate pause
        if cancel_code == 1:
            print("[worker] Pause (immediate) → terminate now")
            should_terminate = True

        # Graceful (NIMBY): terminate only after one more D1-valid frame is observed
        elif cancel_code == 2 and graceful_pending:
            # Safety window: if stuck too long, bail out
            try:
                nimby_to = float(os.environ.get("ELARA_NIMBY_TIMEOUT_SEC", "600"))
            except Exception:
                nimby_to = 600.0
            if time.time() - nimby_armed_ts > nimby_to:
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

        # Renderer finished naturally?
        if code is not None:
            break



    # finalize
    try: t.join(timeout=2)
    except: pass

    final_done = list_done_frames(output, start, end)
    final_aligned: Set[int] = {fr for fr in final_done if (fr - start) % step == 0}
    status = "done" if (proc.returncode == 0 and len(final_aligned) >= frame_total) else "failed"

    # flush any last-delta frames
    try:
        last_delta = sorted(list(final_aligned - prev_done))
        if last_delta:
            post_json("/frame_update", {"worker_id":WORKER_ID,"api_key":API_KEY,"job_id":jid,"frames_done":last_delta})
    except Exception:
        pass

    # final job_update (server will preserve paused/cancelled if cancel was requested)
    try:
        post_json("/job_update", {"worker_id":WORKER_ID,"api_key":API_KEY,"job_id":jid,"status":status,"frame_total":frame_total,
                                  "frame_done":len(final_aligned),
                                  "frame_failed":0 if status=='done' else max(0, frame_total - len(final_aligned)),
                                  "frame_running":0,"log_tail":"\n".join(tail)})
    except Exception as e:
        print("[worker] final update error:", e)

    try: log_f.close()
    except: pass

def main():
    print("=== Elara Worker ==="); print("SERVER:", SERVER); print("RENDER_EXE:", RENDER_EXE)
    register()
    while True:
        try:
            job = get_next_job()
        except Exception as e:
            print("[worker] next_job error:", e); time.sleep(2.0); continue
        if not job:
            time.sleep(2.0); continue
        print(f"[worker] got job id={job['id']} {job['start_frame']}-{job['end_frame']}")
        try:
            run_render(job)
        except Exception as e:
            print("[worker] run_render error:", e)
            try:
                total=((int(job["end_frame"])-int(job["start_frame"]))//max(1,int(job.get('by_step') or 1)))+1
                post_json("/job_update", {"worker_id":WORKER_ID,"api_key":API_KEY,"job_id":job["id"],"status":"failed",
                                          "frame_total": total,
                                          "frame_done": 0, "frame_failed": total, "frame_running": 0,
                                          "log_tail": f"worker exception: {e}"})
            except Exception:
                pass

if __name__=="__main__":
    main()
    # worker test
    # python worker.py
