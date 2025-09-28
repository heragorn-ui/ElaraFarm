# -*- coding: utf-8 -*-
"""
ElaraFarm – Minimal FastAPI Server (auth fixed, GET/POST next_job)
- Accepts API key from headers/query/cookie (multiple names)
- next_job supports GET and POST; worker_id from body or query
- In-memory job store (simple & robust for debugging)
- Simple HTML UI to submit and control jobs
"""

import os
import time
import threading
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, Response

app = FastAPI(title="ElaraFarm Server (minimal)")

# -------------------------
# Config / ENV
# -------------------------
USER_API_KEY = os.environ.get("ELARA_USER_API_KEY", "").strip()
JOIN_SECRET = os.environ.get("ELARA_JOIN_SECRET", "JOIN123").strip()

# -------------------------
# In-memory store
# -------------------------
JOBS: Dict[int, Dict[str, Any]] = {}   # id -> job dict
JOB_COUNTER = 0
WORKERS: Dict[int, Dict[str, Any]] = {}  # id -> {"name":..., "last_seen":...}
WORKER_COUNTER = 0

STORE_LOCK = threading.Lock()

# cancel codes: 0 none, 1 pause-now, 2 nimby (finish frame)
CANCEL_NONE = 0
CANCEL_PAUSE = 1
CANCEL_NIMBY = 2


# -------------------------
# Auth helpers
# -------------------------
def _read_api_key_from_request(req: Request) -> str:
    """Accept API key from headers / query / cookies with many common names."""
    h = req.headers
    # headers (case-insensitive in FastAPI/Starlette)
    for key in (
        "x-elara-user-api-key",
        "x-user-api-key",
        "x-api-key",
        "user_api_key",
        "user-api-key",
    ):
        v = h.get(key)
        if v:
            return v

    # Authorization: Bearer <key>
    auth = h.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()

    # query
    q = req.query_params
    for name in ("api_key", "user_api_key", "USER_API_KEY", "userApiKey"):
        v = q.get(name)
        if v:
            return v

    # cookie
    ck = req.cookies.get("USER_API_KEY") or req.cookies.get("user_api_key")
    if ck:
        return ck

    return ""


def _require_api_key(req: Request):
    # if server key empty => dev mode (no auth)
    if not USER_API_KEY:
        return
    provided = _read_api_key_from_request(req)
    if provided != USER_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# -------------------------
# Utils
# -------------------------
def _now_ts() -> float:
    return time.time()


def _next_job_id() -> int:
    global JOB_COUNTER
    JOB_COUNTER += 1
    return JOB_COUNTER


def _next_worker_id() -> int:
    global WORKER_COUNTER
    WORKER_COUNTER += 1
    return WORKER_COUNTER


# -------------------------
# UI (HTML)
# -------------------------
@app.get("/", response_class=HTMLResponse)
async def index_page():
    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>ElaraFarm – Minimal Web UI</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/daisyui@4.12.10/dist/full.min.css">
  <script src="https://cdn.jsdelivr.net/npm/tailwindcss-jit-cdn"></script>
  <style> body {{ padding: 18px; }} .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; }} </style>
</head>
<body>
  <h1 class="text-2xl font-bold mb-3">ElaraFarm</h1>

  <div class="mb-4">
    <form method="post" action="/submit_job" class="space-y-2">
      <div class="grid grid-cols-2 gap-2">
        <label class="input input-bordered flex items-center gap-2">Scene
          <input class="grow" type="text" name="scene" placeholder="D:\\PATH\\scene.ma" required>
        </label>
        <label class="input input-bordered flex items-center gap-2">Project root
          <input class="grow" type="text" name="project_root" placeholder="D:\\PATH\\Maya_Projects">
        </label>
        <label class="input input-bordered flex items-center gap-2">Output dir
          <input class="grow" type="text" name="output_dir" placeholder="D:\\PATH\\renders\\V01">
        </label>
        <label class="input input-bordered flex items-center gap-2">Camera
          <input class="grow" type="text" name="camera" placeholder="renderCam">
        </label>
        <label class="input input-bordered flex items-center gap-2">Layer
          <input class="grow" type="text" name="layer" placeholder="">
        </label>
        <label class="input input-bordered flex items-center gap-2">Renderer
          <input class="grow" type="text" name="renderer" value="arnold">
        </label>
        <label class="input input-bordered flex items-center gap-2">Start
          <input class="grow" type="number" name="start" value="1">
        </label>
        <label class="input input-bordered flex items-center gap-2">End
          <input class="grow" type="number" name="end" value="10">
        </label>
        <label class="input input-bordered flex items-center gap-2">By step
          <input class="grow" type="number" name="step" value="1">
        </label>
        <label class="input input-bordered flex items-center gap-2">Width
          <input class="grow" type="number" name="width" value="1920">
        </label>
        <label class="input input-bordered flex items-center gap-2">Height
          <input class="grow" type="number" name="height" value="1080">
        </label>
      </div>
      <button class="btn btn-primary">Submit Job</button>
    </form>
  </div>

  <h2 class="text-xl font-semibold mb-2">Jobs</h2>
  <div id="jobs"></div>

  <script>
    async function fetchJobs() {{
      const r = await fetch('/jobs_summary');
      const js = await r.json();
      const rows = js.jobs.map(p => `
        <tr>
          <td class="mono">${{p.id}}</td>
          <td><span class="badge">${{p.status}}</span></td>
          <td class="mono">${{p.scene || ''}}</td>
          <td class="mono">${{p.start}}–${{p.end}}</td>
          <td>${{p.renderer}}</td>
          <td>
            <div class="w-64 bg-base-200 rounded">
              <div class="h-2 rounded bg-success" style="width:${{Math.min(100, Math.round(100*p.done/Math.max(1,p.total)))}}%"></div>
            </div>
            <div class="text-xs mono mt-1">done:${{p.done}} / total:${{p.total}}</div>
          </td>
          <td>
            <div class="join">
              <button class="btn btn-xs join-item" onclick="doPost('/action/pause_job?id=${{p.id}}')">Pause</button>
              <button class="btn btn-xs join-item" onclick="doPost('/action/nimby_job?id=${{p.id}}')">NIMBY</button>
              <button class="btn btn-xs join-item" onclick="doPost('/action/resume_job?id=${{p.id}}')">Resume</button>
              <button class="btn btn-xs btn-error join-item" onclick="doPost('/action/cancel_job?id=${{p.id}}')">Cancel</button>
              <button class="btn btn-xs btn-error join-item" onclick="doPost('/action/delete_job?id=${{p.id}}')">Delete</button>
            </div>
          </td>
        </tr>
      `).join('');
      const table = `
        <div class="overflow-x-auto">
          <table class="table table-sm">
            <thead><tr>
              <th>ID</th><th>Status</th><th>Scene</th><th>Frames</th><th>Renderer</th><th>Progress</th><th>Actions</th>
            </tr></thead>
            <tbody>\${{rows || '<tr><td colspan=7>No jobs</td></tr>'}}</tbody>

          </table>
        </div>`;
      document.getElementById('jobs').innerHTML = table;
    }}
    async function doPost(url) {{
      await fetch(url, {{ method:'POST' }});
      fetchJobs();
    }}
    fetchJobs();
    setInterval(fetchJobs, 1500);
  </script>
</body>
</html>
"""
    return HTMLResponse(html)


# -------------------------
# Jobs API
# -------------------------
@app.post("/submit_job")
async def submit_job(request: Request):
    form = await request.form()
    scene = (form.get("scene") or "").strip()
    if not scene:
        return JSONResponse({"ok": False, "error": "scene required"}, status_code=400)

    job = {
        "id": _next_job_id(),
        "scene": scene,
        "project_root": (form.get("project_root") or "").strip(),
        "output_dir": (form.get("output_dir") or "").strip(),
        "camera": (form.get("camera") or "").strip(),
        "layer": (form.get("layer") or "").strip(),
        "renderer": (form.get("renderer") or "arnold").strip(),
        "start": int(form.get("start") or 1),
        "end": int(form.get("end") or 1),
        "step": int(form.get("step") or 1),
        "width": int(form.get("width") or 1920),
        "height": int(form.get("height") or 1080),

        # runtime fields
        "status": "queued",
        "done": 0,
        "total": 0,  # compute below
        "cancel": CANCEL_NONE,
        "updated": _now_ts(),
    }
    # compute total
    start, end, step = job["start"], job["end"], max(1, job["step"])
    job["total"] = 1 + (max(0, end - start) // step)

    with STORE_LOCK:
        JOBS[job["id"]] = job

    return HTMLResponse('<script>location.href="/";</script>')


@app.get("/jobs_summary")
async def jobs_summary():
    with STORE_LOCK:
        rows = []
        for j in JOBS.values():
            rows.append({
                "id": j["id"],
                "scene": j.get("scene", ""),
                "renderer": j.get("renderer", ""),
                "start": j.get("start", 0),
                "end": j.get("end", 0),
                "step": j.get("step", 1),
                "status": j.get("status", "queued"),
                "done": j.get("done", 0),
                "total": j.get("total", 0),
                "cancel": j.get("cancel", 0),
            })
    return {"jobs": rows}


# -------------------------
# Worker API
# -------------------------
@app.post("/register_worker")
async def register_worker(request: Request):
    # join secret check
    try:
        data = await request.json()
    except Exception:
        data = {}

    if (data.get("join_secret") or "") != JOIN_SECRET:
        # return id=0 to show failure on worker
        return {"worker_id": 0, "api_key": USER_API_KEY}

    with STORE_LOCK:
        wid = _next_worker_id()
        WORKERS[wid] = {"name": data.get("name") or f"worker_{wid}", "last_seen": _now_ts()}

    # IMPORTANT: return "worker_id" (worker reads this)
    return {"worker_id": wid, "api_key": USER_API_KEY}


@app.api_route("/next_job", methods=["GET", "POST"])
async def next_job(request: Request):
    # Auth (flexible)
    _require_api_key(request)

    # Parse body (for POST)
    data: Dict[str, Any] = {}
    if request.method == "POST":
        try:
            data = await request.json()
        except Exception:
            data = {}

    wid_raw = data.get("worker_id") or data.get("id") \
        or request.query_params.get("worker_id") or request.query_params.get("id")
    try:
        wid = int(wid_raw) if wid_raw is not None else 0
    except Exception:
        wid = 0

    with STORE_LOCK:
        # update heartbeat
        if wid in WORKERS:
            WORKERS[wid]["last_seen"] = _now_ts()

        # find first job not finished
        for j in sorted(JOBS.values(), key=lambda x: x["id"]):
            if j["status"] in ("queued", "running") and j.get("done", 0) < j.get("total", 1):
                # mark running
                j["status"] = "running"
                j["updated"] = _now_ts()
                return j

    # no jobs
    return Response(status_code=204)


@app.post("/job_update")
async def job_update(request: Request):
    _require_api_key(request)

    try:
        data = await request.json()
    except Exception:
        data = {}

    jid = int(data.get("job_id") or 0)
    with STORE_LOCK:
        j = JOBS.get(jid)
        if not j:
            return {"ok": False}

        # apply updates
        if "done" in data:
            try:
                j["done"] = int(data["done"])
            except Exception:
                pass
        if "status" in data:
            j["status"] = str(data["status"])
        j["updated"] = _now_ts()

        # if finished
        if j["done"] >= j["total"]:
            j["status"] = "done"
            j["cancel"] = CANCEL_NONE

        # return current cancel code so worker can react
        return {"ok": True, "cancel": j.get("cancel", CANCEL_NONE)}


# -------------------------
# Actions from UI
# -------------------------
def _set_cancel(job_id: int, code: int):
    with STORE_LOCK:
        j = JOBS.get(job_id)
        if not j:
            return
        j["cancel"] = code
        if code == CANCEL_NONE and j["status"] == "paused":
            j["status"] = "running"
        j["updated"] = _now_ts()


@app.post("/action/pause_job")
async def action_pause_job(id: int):
    _set_cancel(id, CANCEL_PAUSE)
    with STORE_LOCK:
        if id in JOBS:
            JOBS[id]["status"] = "paused"
    return {"ok": True}


@app.post("/action/nimby_job")
async def action_nimby_job(id: int):
    _set_cancel(id, CANCEL_NIMBY)
    return {"ok": True}


@app.post("/action/resume_job")
async def action_resume_job(id: int):
    _set_cancel(id, CANCEL_NONE)
    with STORE_LOCK:
        if id in JOBS and JOBS[id]["status"] in ("paused", "running", "queued"):
            JOBS[id]["status"] = "running"
    return {"ok": True}


@app.post("/action/cancel_job")
async def action_cancel_job(id: int):
    with STORE_LOCK:
        if id in JOBS:
            JOBS[id]["status"] = "canceled"
            JOBS[id]["cancel"] = CANCEL_PAUSE
    return {"ok": True}


@app.post("/action/delete_job")
async def action_delete_job(id: int):
    with STORE_LOCK:
        if id in JOBS:
            del JOBS[id]
    return {"ok": True}


# -------------------------
# Dev entrypoint
# -------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
