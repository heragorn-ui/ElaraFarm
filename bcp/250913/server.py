# -*- coding: utf-8 -*-
# ElaraFarm Server v0.9.2 — SSE live + frame grid + resubmit + split-to-frames + custom modals

import os, time, json, sqlite3, secrets, asyncio
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = os.path.join(os.path.dirname(__file__), "elarafarm.db")
JOIN_SECRET = os.environ.get("ELARA_JOIN_SECRET", "CHANGE_ME")
USER_API_KEY = os.environ.get("ELARA_USER_API_KEY", "CHANGE_ME")
LOG_DIR = os.environ.get("ELARA_LOG_DIR", r"C:\ElaraFarm\worker\logs")
AUTO_RETRY_DEFAULT = 2

app = FastAPI(title="ElaraFarm Server", version="0.9.2")
app.add_middleware(CORSMiddleware,
    allow_origins=["http://127.0.0.1","http://localhost"],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def now(): return time.time()
def db():
    c = sqlite3.connect(DB_PATH, check_same_thread=False); c.row_factory = sqlite3.Row; return c

def init_db():
    c=db();x=c.cursor()
    x.execute("""CREATE TABLE IF NOT EXISTS workers(
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, api_key TEXT, last_seen REAL)""")
    x.execute("""CREATE TABLE IF NOT EXISTS jobs(
        id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT, created REAL, updated REAL,
        scene TEXT, project TEXT, output_dir TEXT,
        start_frame INTEGER, end_frame INTEGER, by_step INTEGER,
        camera TEXT, width INTEGER, height INTEGER, renderer TEXT, layer TEXT,
        worker_id INTEGER, log_tail TEXT,
        group_id TEXT, part_index INTEGER, part_count INTEGER,
        frame_total INTEGER DEFAULT 0, frame_done INTEGER DEFAULT 0, frame_failed INTEGER DEFAULT 0, frame_running INTEGER DEFAULT 0,
        eta_seconds REAL, error_count INTEGER DEFAULT 0, priority INTEGER DEFAULT 0,
        retries INTEGER DEFAULT 0, max_retries INTEGER DEFAULT 2,
        cancel_requested INTEGER DEFAULT 0, deleted INTEGER DEFAULT 0
    )""")
    x.execute("""CREATE TABLE IF NOT EXISTS job_frames(
        job_id INTEGER, frame INTEGER, status TEXT, tries INTEGER DEFAULT 0, updated REAL,
        PRIMARY KEY(job_id,frame))""")
    x.execute("CREATE INDEX IF NOT EXISTS idx_job_frames_job ON job_frames(job_id)")
    c.commit(); c.close()
init_db()

# ---------------- SSE bus ----------------
class EventBus:
    def __init__(self): self.clients=[]; self.lock=asyncio.Lock()
    async def add(self): 
        q=asyncio.Queue()
        async with self.lock: self.clients.append(q)
        return q
    async def remove(self,q):
        async with self.lock:
            if q in self.clients: self.clients.remove(q)
    async def publish(self, typ, data):
        payload=json.dumps({"type":typ,"data":data})
        async with self.lock:
            for q in list(self.clients):
                try: q.put_nowait(payload)
                except: pass
bus=EventBus()

@app.get("/events")
async def events():
    async def gen():
        q=await bus.add()
        try:
            yield "event: ping\ndata: 1\n\n"
            while True:
                try:
                    d=await q.get()
                    yield f"event: msg\ndata: {d}\n\n"
                except asyncio.CancelledError:
                    break
                except:
                    yield "event: ping\ndata: 1\n\n"
        finally:
            await bus.remove(q)
    return HTMLResponse(gen(), media_type="text/event-stream")

def require_user_api_key(k:str):
    if not k or k!=USER_API_KEY: raise HTTPException(401,"Invalid USER_API_KEY")

def worker_from_auth(worker_id:int, api_key:str):
    c=db();x=c.cursor();x.execute("SELECT 1 FROM workers WHERE id=? AND api_key=?", (worker_id,api_key))
    if not x.fetchone(): c.close(); raise HTTPException(401,"Invalid worker auth")
    c.close()

# ---------------- UI ----------------
@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!doctype html><html><head><meta charset="utf-8"/>
<title>ElaraFarm — Live Render</title>
<style>
:root{--fg:#222;--muted:#666;--br:#dcdcdc;--bg:#fff;--acc:#2c7be5;--ok:#21bf73;--run:#f6c445;--fail:#e55353;--queued:#999;--barbg:#f7f7f7}
*{box-sizing:border-box} body{font-family:Segoe UI,Arial,sans-serif;color:var(--fg);background:var(--bg);margin:24px;max-width:1280px}
fieldset{border:1px solid var(--br);padding:14px;margin:10px 0 18px;border-radius:8px} legend{padding:0 6px}
label{display:block;font-size:12px;margin:6px 0 4px} input,select,button{padding:10px 12px;margin:4px 0;border:1px solid var(--br);border-radius:6px;font-size:13px}
input[type=text],input[type=password]{width:100%}
table{border-collapse:collapse;width:100%} th,td{border:1px solid var(--br);padding:6px;font-size:12px;vertical-align:top} th{background:#fafafa}
.small{font-size:12px;color:var(--muted)} .right{text-align:right}
.status{font-weight:600} .status.queued{color:var(--queued)} .status.running{color:#d49100} .status.done{color:var(--ok)} .status.failed{color:var(--fail)} .status.cancelled{color:#8e44ad}
.bar{width:360px;height:12px;border:1px solid var(--br);border-radius:20px;overflow:hidden;background:var(--barbg)}
.bar .done{height:100%;background:var(--ok);float:left;transition:width .35s ease} .bar .running{height:100%;background:var(--run);float:left;transition:width .35s ease}
.btn{display:inline-block;padding:6px 10px;border:1px solid var(--br);border-radius:6px;background:#fff;color:#333;text-decoration:none;cursor:pointer;font-size:12px}
.btn:hover{background:#f3f6ff;border-color:#b7cdf7}
.btn-primary{background:#2c7be5;color:#fff;border-color:#2c7be5}.btn-primary:hover{background:#2367c8}
.btn-danger{background:#e55353;color:#fff;border-color:#e55353}.btn-danger:hover{background:#c94444}
.btn-ghost{background:#fff;color:#555;border-color:#ccc}.btn-ghost:hover{background:#f5f5f5}
.btn-sm{padding:4px 8px;font-size:11px;border-radius:5px}
.subrow td{background:#fcfcfc}
.gridwrap{padding:8px 0}
.framegrid{display:grid;grid-auto-rows:12px;gap:2px}
.frame{width:12px;height:12px;border:1px solid #ddd;background:#f0f0f0;cursor:pointer}
.frame.q{background:#eee} .frame.r{background:#f6c445} .frame.d{background:#21bf73} .frame.f{background:#e55353}
.frame.sel{outline:2px solid #2c7be5}
.toolbar{display:flex;gap:8px;align-items:center;margin:6px 0}
.mono{font-family:Consolas,monospace}
.hide{display:none}

/* modal */
dialog{border:none;border-radius:8px;max-width:90vw;width:600px;padding:0}
dialog::backdrop{background:rgba(0,0,0,.35)}
.modal-h{padding:10px 14px;border-bottom:1px solid #ddd;font-weight:600}
.modal-b{padding:14px}
.modal-f{padding:10px 14px;border-top:1px solid #ddd;display:flex;gap:8px;justify-content:flex-end}
.toast{position:fixed;right:16px;bottom:16px;background:#333;color:#fff;padding:10px 14px;border-radius:8px;opacity:0;transform:translateY(10px);transition:all .25s}
.toast.show{opacity:1;transform:translateY(0)}
pre.log{white-space:pre-wrap;border:1px solid #ddd;padding:10px;border-radius:6px;background:#fafafa;max-height:60vh;overflow:auto;font-family:Consolas,monospace;font-size:12px}
</style></head>
<body>
<h2>ElaraFarm – Minimal Web UI</h2>

<form method="post" action="/submit_job" id="jobform">
  <fieldset><legend>Auth</legend>
    <label>USER_API_KEY</label>
    <div style="display:flex;gap:8px;align-items:center">
      <input id="user_api_key" name="user_api_key" type="password" class="full" placeholder="Enter USER_API_KEY" required/>
      <label style="display:flex;align-items:center;gap:6px"><input id="showkey" type="checkbox" onclick="const f=document.getElementById('user_api_key');f.type=(f.type==='password')?'text':'password';"> Show</label>
    </div>
  </fieldset>

  <fieldset><legend>Maya/Arnold Job</legend>
    <label>Scene (.ma/.mb)</label><input id="scene" name="scene" class="full mono" required/>
    <label>Project (Maya project root)</label><input id="project" name="project" class="full mono" required/>
    <label>Output directory</label><input id="output_dir" name="output_dir" class="full mono" required/>
    <div style="display:flex;gap:12px;flex-wrap:wrap">
      <div><label>Camera</label><input name="camera" id="camera" placeholder="renderCam"></div>
      <div><label>Render layer (optional)</label><input name="layer" id="layer" placeholder="(optional)"></div>
      <div><label>Start frame</label><input type="number" id="start_frame" name="start_frame" value="1001"></div>
      <div><label>End frame</label><input type="number" id="end_frame" name="end_frame" value="1010"></div>
      <div><label>By step</label><input type="number" id="by_step" name="by_step" value="1"></div>
      <div><label>Chunk size (0 = none)</label><input type="number" id="chunk_size" name="chunk_size" value="0"></div>
    </div>
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:6px">
      <div><label>Width</label><input type="number" id="width" name="width" value="1920"></div>
      <div><label>Height</label><input type="number" id="height" name="height" value="1080"></div>
      <div><label>Renderer</label><select id="renderer" name="renderer"><option value="arnold" selected>arnold</option></select></div>
    </div>
    <p class="small">Tek makine için en hızlı yol: <b>Chunk size = 0</b>, <b>By step = 1</b>. Render layer adını (örn: <i>mask</i>) doldur.</p>
    <div style="display:flex;gap:6px;flex-wrap:wrap">
      <button class="btn btn-primary" type="submit">Submit Job</button>
      <button class="btn btn-ghost" type="button" onclick="clearForm()">Clear</button>
    </div>
  </fieldset>
</form>

<h3>Jobs</h3>
<div id="jobs"></div>

<!-- Log modal -->
<dialog id="logdlg">
  <div class="modal-h" id="logtitle">Log</div>
  <div class="modal-b"><pre class="log" id="logtext"></pre></div>
  <div class="modal-f">
    <button class="btn btn-ghost btn-sm" id="logrefresh">Refresh</button>
    <button class="btn btn-danger btn-sm" onclick="document.getElementById('logdlg').close()">Close</button>
  </div>
</dialog>

<!-- Confirm modal -->
<dialog id="confdlg">
  <div class="modal-h" id="conftitle">Confirm</div>
  <div class="modal-b" id="confmsg">Are you sure?</div>
  <div class="modal-f">
    <button class="btn btn-ghost btn-sm" onclick="document.getElementById('confdlg').close()">Cancel</button>
    <button class="btn btn-danger btn-sm" id="confok">OK</button>
  </div>
</dialog>

<div id="toast" class="toast">done</div>

<script>
const KEY="elara_form";
const FIELDS=["user_api_key","scene","project","output_dir","camera","layer","start_frame","end_frame","by_step","chunk_size","width","height","renderer"];
function saveForm(){const d={};for(const k of FIELDS){const el=document.getElementById(k);if(el)d[k]=el.value;}localStorage.setItem(KEY,JSON.stringify(d));}
function loadForm(){try{const d=JSON.parse(localStorage.getItem(KEY)||"{}");for(const k of FIELDS){const el=document.getElementById(k);if(el&&d[k]!==undefined)el.value=d[k];}}catch(e){}}
function clearForm(){for(const k of FIELDS){const el=document.getElementById(k);if(el)el.value="";}localStorage.removeItem(KEY);}
document.getElementById('jobform').addEventListener('submit', saveForm); loadForm();

function toast(msg){const t=document.getElementById('toast');t.textContent=msg;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),1800);}
function modalConfirm(message, onok){const d=document.getElementById('confdlg');document.getElementById('confmsg').textContent=message;const ok=document.getElementById('confok');ok.onclick=()=>{d.close();onok&&onok();};d.showModal();}

function pct(done,fail,total){if(!total||total<=0)return 0;return Math.round(100*((done+fail)/total));}

async function doPost(url, body){const opts=body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{method:'POST'};await fetch(url,opts);setTimeout(loadSummary,200);}
function actConfirm(url,msg,body){modalConfirm(msg,()=>doPost(url,body));}

async function loadSummary(){
  const r=await fetch('/jobs_summary'); const data=await r.json();
  const rows=data.map(g=>{
    const donePct=((g.done+g.failed)/Math.max(1,g.total))*100|0;
    const runPct=((g.running)/Math.max(1,g.total))*100|0;
    const gid=g.group_id||("job-"+g.single_id);
    const act = g.group_id
     ? `<div style="display:flex;gap:6px;flex-wrap:wrap">
          <button class="btn btn-ghost btn-sm" onclick="toggleParts('${gid}')">Expand</button>
          <button class="btn btn-ghost btn-sm" onclick="doPost('/action/retry_failed_group?gid=${gid}')">Retry failed</button>
          <button class="btn btn-ghost btn-sm" onclick="actConfirm('/action/cancel_group?gid=${gid}','Cancel all parts?')">Cancel group</button>
          <button class="btn btn-danger btn-sm" onclick="actConfirm('/action/delete_group?gid=${gid}','Delete group?')">Delete group</button>
        </div>`
     : `<div style="display:flex;gap:6px;flex-wrap:wrap">
          <button class="btn btn-ghost btn-sm" onclick="openFrames(${g.single_id})">Frames</button>
          <button class="btn btn-ghost btn-sm" onclick="openLog(${g.single_id})">Log</button>
          <button class="btn btn-ghost btn-sm" onclick="doPost('/action/retry_job?id=${g.single_id}')">Retry</button>
          <button class="btn btn-ghost btn-sm" onclick="actConfirm('/action/cancel_job?id=${g.single_id}','Cancel job?')">Cancel</button>
          <button class="btn btn-danger btn-sm" onclick="actConfirm('/action/delete_job?id=${g.single_id}','Delete job?')">Delete</button>
        </div>`;
    return `
      <tr>
        <td>${g.label}</td>
        <td class="status ${(g.status||'').toLowerCase()}">${g.status||""}</td>
        <td class="mono">${g.scene||""}</td>
        <td>${g.frames||""}</td>
        <td>${g.renderer||""}</td>
        <td>
          <div class="bar"><div class="done" style="width:${donePct}%"></div><div class="running" style="width:${runPct}%"></div></div>
          <div class="small">done:${g.done} / fail:${g.failed} / total:${g.total} • parts:${g.parts}</div>
        </td>
        <td>${act}</td>
        <td class="right">${g.updated?new Date(g.updated*1000).toLocaleString():""}</td>
      </tr>
      <tr id="parts-${gid}" class="subrow hide"><td colspan="8"><div id="partsbox-${gid}">Loading...</div></td></tr>`;
  }).join("");
  document.getElementById('jobs').innerHTML=
   `<div class="small"><b>Purge:</b>
      <a class="btn btn-ghost btn-sm" href="#" onclick="doPost('/purge_finished?days=7');return false;">Finished &gt; 7 days</a>
      <a class="btn btn-ghost btn-sm" href="#" onclick="doPost('/purge_finished?days=30');return false;">Finished &gt; 30 days</a>
      <a class="btn btn-ghost btn-sm" href="#" onclick="doPost('/purge_deleted');return false;">Purge Deleted</a>
    </div>
    <table><tr><th>Group / Job</th><th>Status</th><th>Scene</th><th>Frames</th><th>Renderer</th><th>Progress</th><th>Actions</th><th class="right">Updated</th></tr>${rows}</table>`;
}

async function toggleParts(gid){
  const row=document.getElementById('parts-'+gid); row.classList.toggle('hide');
  if(!row.classList.contains('hide')){
    const box=document.getElementById('partsbox-'+gid);
    box.innerHTML="Loading...";
    const r=await fetch('/group_parts?gid='+encodeURIComponent(gid)); const parts=await r.json();
    const html=parts.map(p=>{
      return `<div style="padding:6px 4px;border-bottom:1px solid #eee">
        <div class="small mono">${p.start_frame}-${p.end_frame} • ${p.status} • done:${p.frame_done}/${p.frame_total}</div>
        <div class="toolbar">
          <button class="btn btn-ghost btn-sm" onclick="openFrames(${p.id})">Frames</button>
          <button class="btn btn-ghost btn-sm" onclick="openLog(${p.id})">Log</button>
          <button class="btn btn-ghost btn-sm" onclick="doPost('/action/retry_job?id=${p.id}')">Retry</button>
          <button class="btn btn-ghost btn-sm" onclick="actConfirm('/action/cancel_job?id=${p.id}','Cancel part?')">Cancel</button>
          <button class="btn btn-danger btn-sm" onclick="actConfirm('/action/delete_job?id=${p.id}','Delete part?')">Delete</button>
        </div>
        <div id="frames-${p.id}" class="gridwrap hide"></div>
      </div>`;
    }).join("");
    box.innerHTML = html || "<i>No parts</i>";
  }
}

async function openFrames(job_id){
  const el=document.getElementById('frames-'+job_id); if(!el) return;
  el.classList.toggle('hide'); if(el.classList.contains('hide')) return;
  const data = await (await fetch('/frames_status?job_id='+job_id)).json();
  buildGrid(el, data);
}

function buildGrid(container, data){
  container.innerHTML="";
  const start=data.start_frame, end=data.end_frame;
  const total = Math.max(0, end-start+1);
  const cols = Math.min(100, Math.ceil(Math.sqrt(total)));
  const grid=document.createElement('div'); grid.className='framegrid';
  grid.style.gridTemplateColumns = `repeat(${cols}, 12px)`;
  container.appendChild(grid);

  const state = new Map();
  const sel = new Set();

  function cellClass(st){ if(st==='d') return 'frame d'; if(st==='f') return 'frame f'; if(st==='r') return 'frame r'; return 'frame q'; }
  function makeCell(fr){
    const div=document.createElement('div'); div.className=cellClass(state.get(fr)||'q'); div.title=String(fr); div.dataset.frame=String(fr);
    div.onclick=()=>{ const k=div.dataset.frame; if(sel.has(k)){ sel.delete(k); div.classList.remove('sel'); } else { sel.add(k); div.classList.add('sel'); } };
    return div;
  }

  for(const fr of data.done||[]) state.set(fr,'d');
  for(const fr of data.failed||[]) state.set(fr,'f');
  for(let fr=start; fr<=end; fr++) grid.appendChild(makeCell(fr));

  // toolbar
  const bar=document.createElement('div'); bar.className='toolbar';
  bar.innerHTML=`<div class="small mono">Job ${data.job_id} • ${start}-${end}</div><div style="flex:1"></div>
    <button class="btn btn-ghost btn-sm" id="retrySel">Retry selected</button>
    <button class="btn btn-ghost btn-sm" id="retryFailed">Retry failed</button>
    <button class="btn btn-ghost btn-sm" id="splitAll">Split to 1-frame parts (all)</button>
    <button class="btn btn-ghost btn-sm" id="splitMissing">Split to 1-frame parts (missing)</button>
    <button class="btn btn-ghost btn-sm" id="clearSel">Clear selection</button>`;
  container.prepend(bar);

  bar.querySelector('#retrySel').onclick = async ()=>{
    const frames=[...sel].map(s=>parseInt(s,10)).sort((a,b)=>a-b);
    if(!frames.length){ toast('No selection'); return; }
    await doPost('/action/resubmit_frames', {job_id:data.job_id, frames:frames});
    toast('Selected frames queued');
  };
  bar.querySelector('#retryFailed').onclick = async ()=>{
    const frames=(data.failed||[]).slice();
    if(!frames.length){ toast('No failed frames'); return; }
    await doPost('/action/resubmit_frames', {job_id:data.job_id, frames:frames});
    toast('Failed frames queued');
  };
  bar.querySelector('#splitAll').onclick = ()=>{
    modalConfirm('Split ALL frames into 1-frame jobs?', async ()=>{
      await doPost('/action/split_job_to_frames', {job_id:data.job_id, only_missing:false});
      toast('Split all → queued');
    });
  };
  bar.querySelector('#splitMissing').onclick = ()=>{
    modalConfirm('Split ONLY missing/failed frames?', async ()=>{
      await doPost('/action/split_job_to_frames', {job_id:data.job_id, only_missing:true});
      toast('Split missing → queued');
    });
  };
  bar.querySelector('#clearSel').onclick = ()=>{ sel.clear(); container.querySelectorAll('.frame.sel').forEach(d=>d.classList.remove('sel')); };

  function applyUpdate(frames, st){
    for(const fr of frames||[]){
      const q=container.querySelector(`.framegrid .frame[data-frame="${fr}"]`);
      if(q){ q.className = cellClass(st); }
      state.set(fr, st);
    }
  }
  if(!window._elara_es){
    const es=new EventSource('/events');
    es.onmessage=(e)=>{ try{ const msg=JSON.parse(e.data); if(msg.type==='frame'){ const d=msg.data; if(d.job_id!==data.job_id) return; if(d.frames_done)applyUpdate(d.frames_done,'d'); if(d.frames_failed)applyUpdate(d.frames_failed,'f'); if(d.current_frame)applyUpdate([d.current_frame],'r'); } }catch(err){} };
    window._elara_es=es;
  }
}

async function openLog(id){
  const dlg=document.getElementById('logdlg'), pre=document.getElementById('logtext'), title=document.getElementById('logtitle');
  title.textContent=`Job ${id} – Log (tail)`;
  async function fetchTail(){ const r=await fetch('/job_tail?id='+id); pre.textContent=await r.text(); }
  document.getElementById('logrefresh').onclick=fetchTail;
  await fetchTail(); dlg.showModal();
}

// initial & fallback
loadSummary(); setInterval(loadSummary, 5000);
</script>
</body></html>
"""

# ---------------- submit & summary & parts ----------------
@app.post("/submit_job")
def submit_job(
    user_api_key: str = Form(...),
    scene: str = Form(...), project: str = Form(...), output_dir: str = Form(...),
    camera: Optional[str] = Form(None), layer: Optional[str] = Form(None),
    start_frame: int = Form(1), end_frame: int = Form(1), by_step: int = Form(1),
    width: int = Form(1920), height: int = Form(1080), renderer: str = Form("arnold"),
    chunk_size: int = Form(0),
):
    require_user_api_key(user_api_key)
    step=max(1,int(by_step)); s0=int(start_frame); e0=int(end_frame)
    if e0<s0: raise HTTPException(400,"end_frame must be >= start_frame")
    c=db();x=c.cursor()
    def ins(s,e,gid,idx,cnt):
        ft=((e-s)//step)+1
        x.execute("""INSERT INTO jobs(status,created,updated,scene,project,output_dir,
           start_frame,end_frame,by_step,camera,width,height,renderer,layer,
           group_id,part_index,part_count,frame_total,frame_done,frame_failed,frame_running,
           eta_seconds,error_count,priority,retries,max_retries,cancel_requested,deleted)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
           ("queued",now(),now(),scene,project,output_dir,s,e,step,camera,width,height,renderer,layer,
            gid,idx,cnt,ft,0,0,0,None,0,0,0,AUTO_RETRY_DEFAULT,0,0))
    cs=int(chunk_size) if chunk_size else 0
    if cs>0:
        gid=secrets.token_hex(4); total=((e0-s0+1)+cs-1)//cs; a=s0; idx=1
        while a<=e0:
            b=min(a+cs-1,e0); ins(a,b,gid,idx,total); a=b+1; idx+=1
    else:
        ins(s0,e0,None,None,None)
    c.commit(); c.close()
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/" />')

@app.get("/jobs_summary")
def jobs_summary():
    c=db();x=c.cursor();x.execute("SELECT * FROM jobs WHERE deleted=0 ORDER BY created DESC")
    rows=[dict(r) for r in x.fetchall()]; c.close()
    groups={}; singles=[]
    for j in rows:
        ft=int(j.get("frame_total") or 0); fd=int(j.get("frame_done") or 0); ff=int(j.get("frame_failed") or 0); fr=int(j.get("frame_running") or 0)
        gid=j.get("group_id"); st=(j.get("status") or "").lower()
        if gid:
            g=groups.get(gid) or {"group_id":gid,"label":f"group {gid}","scene":j.get("scene"),"renderer":j.get("renderer"),
                                  "start":j.get("start_frame"),"end":j.get("end_frame"),
                                  "total":0,"done":0,"failed":0,"running":0,"parts":0,"status":"queued","updated":j.get("updated")}
            g["total"]+=ft; g["done"]+=fd; g["failed"]+=ff; g["running"]+=fr; g["parts"]+=1
            try:g["end"]=max(g["end"],int(j.get("end_frame") or g["end"]))
            except:pass
            try:g["start"]=min(g["start"],int(j.get("start_frame") or g["start"]))
            except:pass
            g["updated"]=max(g["updated"] or 0, j.get("updated") or 0)
            if g["running"]>0:g["status"]="running"
            elif g["failed"]>0:g["status"]="failed"
            elif g["done"]>0 and g["done"]==g["total"] and g["total"]>0:g["status"]="done"
            else:g["status"]="queued"
            groups[gid]=g
        else:
            singles.append({**j,"frame_total":ft,"frame_done":fd,"frame_failed":ff,"frame_running":fr})
    out=[]
    for g in groups.values():
        out.append({"group_id":g["group_id"],"label":g["label"],"scene":g["scene"],"renderer":g["renderer"],
                    "frames":f"{g['start']}-{g['end']}","total":g["total"],"done":g["done"],"failed":g["failed"],
                    "running":g["running"],"parts":g["parts"],"status":g["status"],"updated":g["updated"],"single_id":None})
    for j in singles:
        out.append({"group_id":None,"label":f"job {j['id']}","scene":j.get("scene"),"renderer":j.get("renderer"),
                    "frames":f"{j.get('start_frame')}-{j.get('end_frame')}","total":int(j.get("frame_total") or 0),
                    "done":int(j.get("frame_done") or 0),"failed":int(j.get("frame_failed") or 0),
                    "running":int(j.get("frame_running") or 0),"parts":1,"status":j.get("status"),
                    "updated":j.get("updated"),"single_id":j["id"]})
    out.sort(key=lambda x:(x["updated"] or 0), reverse=True)
    return JSONResponse(out)

@app.get("/group_parts")
def group_parts(gid:str="", job_id:int=0):
    c=db();x=c.cursor()
    if gid and gid.startswith("job-"):
        try: jid=int(gid.split("-",1)[1])
        except: jid=0
        x.execute("SELECT * FROM jobs WHERE id=? AND deleted=0",(jid,)); parts=[dict(x.fetchone() or {})]
    elif gid:
        x.execute("SELECT * FROM jobs WHERE group_id=? AND deleted=0 ORDER BY start_frame ASC",(gid,)); parts=[dict(r) for r in x.fetchall()]
    elif job_id:
        x.execute("SELECT * FROM jobs WHERE id=? AND deleted=0",(job_id,)); parts=[dict(x.fetchone() or {})]
    else: parts=[]
    c.close()
    out=[]
    for p in parts:
        tail=(p.get("log_tail") or "").strip()
        last=""
        if tail:
            lines=[ln for ln in tail.splitlines() if ln.strip()]
            if lines: last=lines[-1]
        out.append({"id":p.get("id"),"status":p.get("status"),"start_frame":p.get("start_frame"),
                    "end_frame":p.get("end_frame"),"part_index":p.get("part_index"),"part_count":p.get("part_count"),
                    "frame_total":p.get("frame_total") or 0,"frame_done":p.get("frame_done") or 0,
                    "frame_failed":p.get("frame_failed") or 0,"frame_running":p.get("frame_running") or 0,
                    "error_count":p.get("error_count") or 0,"updated":p.get("updated"),"last_line":last})
    return JSONResponse(out)

# --------------- Frame grid API ---------------
@app.post("/frame_update")
async def frame_update(payload:Dict[str,Any]):
    worker_from_auth(payload.get("worker_id"), payload.get("api_key"))
    jid=int(payload.get("job_id") or 0)
    if not jid: raise HTTPException(400,"job_id required")
    frames_done = payload.get("frames_done") or []
    frames_failed = payload.get("frames_failed") or []
    current_frame = payload.get("current_frame")
    ts=now(); c=db();x=c.cursor()
    for fr in frames_done:
        try: x.execute("""INSERT INTO job_frames(job_id,frame,status,tries,updated) 
                          VALUES(?,?,?,?,?) ON CONFLICT(job_id,frame) DO UPDATE SET status='done',updated=?""",
                          (jid,int(fr),'done',0,ts,ts))
        except: pass
    for fr in frames_failed:
        try: x.execute("""INSERT INTO job_frames(job_id,frame,status,tries,updated) 
                          VALUES(?,?,?,?,?) ON CONFLICT(job_id,frame) DO UPDATE SET status='failed',updated=?""",
                          (jid,int(fr),'failed',0,ts,ts))
        except: pass
    c.commit(); c.close()
    await bus.publish("frame", {"job_id":jid,"frames_done":frames_done,"frames_failed":frames_failed,"current_frame":current_frame})
    return {"ok":True}

@app.get("/frames_status")
def frames_status(job_id:int):
    c=db();x=c.cursor();x.execute("SELECT start_frame,end_frame FROM jobs WHERE id=?", (job_id,))
    row=x.fetchone()
    if not row: c.close(); raise HTTPException(404,"job not found")
    start=row["start_frame"]; end=row["end_frame"]
    x.execute("SELECT frame,status FROM job_frames WHERE job_id=?", (job_id,))
    done=[]; failed=[]
    for r in x.fetchall():
        if r["status"]=="done": done.append(r["frame"])
        elif r["status"]=="failed": failed.append(r["frame"])
    c.close()
    return {"job_id":job_id,"start_frame":start,"end_frame":end,"done":sorted(done),"failed":sorted(failed)}

@app.post("/action/resubmit_frames")
def resubmit_frames(payload:Dict[str,Any]):
    job_id=int(payload.get("job_id") or 0); frames:List[int]=payload.get("frames") or []
    if not job_id or not frames: return {"ok":True}
    c=db();x=c.cursor();x.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
    src=x.fetchone(); 
    if not src: c.close(); raise HTTPException(404,"job not found")
    scene=src["scene"]; project=src["project"]; output_dir=src["output_dir"]
    camera=src["camera"]; layer=src["layer"]; width=src["width"]; height=src["height"]; renderer=src["renderer"]; step=1
    frames=sorted(set(int(f) for f in frames))
    blocks=[]; a=b=frames[0]
    for fr in frames[1:]:
        if fr==b+1: b=fr
        else: blocks.append((a,b)); a=b=fr
    blocks.append((a,b))
    gid=secrets.token_hex(4)
    for idx,(s,e) in enumerate(blocks,1):
        ft=(e-s)//step+1
        x.execute("""INSERT INTO jobs(status,created,updated,scene,project,output_dir,
            start_frame,end_frame,by_step,camera,width,height,renderer,layer,
            group_id,part_index,part_count,frame_total,frame_done,frame_failed,frame_running,
            eta_seconds,error_count,priority,retries,max_retries,cancel_requested,deleted)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("queued",now(),now(),scene,project,output_dir,s,e,step,camera,width,height,renderer,layer,
             gid,idx,len(blocks),ft,0,0,0,None,0,10,0,AUTO_RETRY_DEFAULT,0,0))
    c.commit(); c.close()
    return {"ok":True,"blocks":blocks,"group_id":gid}

@app.post("/action/split_job_to_frames")
def split_job_to_frames(payload:Dict[str,Any]):
    job_id=int(payload.get("job_id") or 0); only_missing=bool(payload.get("only_missing"))
    if not job_id: return {"ok":True}
    c=db();x=c.cursor();x.execute("SELECT * FROM jobs WHERE id=?", (job_id,)); j=x.fetchone()
    if not j: c.close(); raise HTTPException(404,"job not found")
    s=j["start_frame"]; e=j["end_frame"]; all_frames=list(range(s,e+1))
    frames=all_frames
    if only_missing:
        x.execute("SELECT frame FROM job_frames WHERE job_id=? AND status='done'", (job_id,))
        done={r["frame"] for r in x.fetchall()}
        frames=[fr for fr in all_frames if fr not in done]
    scene=j["scene"]; project=j["project"]; output_dir=j["output_dir"]
    camera=j["camera"]; layer=j["layer"]; width=j["width"]; height=j["height"]; renderer=j["renderer"]
    gid=secrets.token_hex(4)
    for idx,fr in enumerate(frames,1):
        x.execute("""INSERT INTO jobs(status,created,updated,scene,project,output_dir,
            start_frame,end_frame,by_step,camera,width,height,renderer,layer,
            group_id,part_index,part_count,frame_total,frame_done,frame_failed,frame_running,
            eta_seconds,error_count,priority,retries,max_retries,cancel_requested,deleted)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("queued",now(),now(),scene,project,output_dir,fr,fr,1,camera,width,height,renderer,layer,
             gid,idx,len(frames),1,0,0,0,None,0,10,0,AUTO_RETRY_DEFAULT,0,0))
    c.commit(); c.close()
    return {"ok":True,"count":len(frames),"group_id":gid}

# --------------- Logs ---------------
@app.get("/job_tail", response_class=PlainTextResponse)
def job_tail(id:int):
    path=os.path.join(LOG_DIR, f"job_{id}.log")
    if os.path.isfile(path):
        try:
            with open(path,"r",encoding="utf-8",errors="replace") as f: return f.read()[-6000:]
        except Exception:
            try:
                with open(path,"rb") as f: return f.read().decode("utf-8","replace")[-6000:]
            except Exception: pass
    c=db();x=c.cursor();x.execute("SELECT log_tail FROM jobs WHERE id=?", (id,))
    r=x.fetchone(); c.close(); return (r["log_tail"] if r and r["log_tail"] else "")

# --------------- Actions / purge ---------------
def _ok(): return {"ok":True}

@app.post("/action/cancel_job")
def cancel_job(id:int):
    c=db();x=c.cursor();x.execute("SELECT status FROM jobs WHERE id=? AND deleted=0",(id,)); r=x.fetchone()
    if not r: c.close(); return _ok()
    st=(r["status"] or "").lower()
    if st=="queued": x.execute("UPDATE jobs SET status='cancelled', updated=? WHERE id=?", (now(),id))
    elif st=="running": x.execute("UPDATE jobs SET cancel_requested=1 WHERE id=?", (id,))
    c.commit(); c.close(); return _ok()

@app.post("/action/cancel_group")
def cancel_group(gid:str):
    c=db();x=c.cursor()
    x.execute("UPDATE jobs SET status='cancelled', updated=? WHERE group_id=? AND status='queued' AND deleted=0",(now(),gid))
    x.execute("UPDATE jobs SET cancel_requested=1 WHERE group_id=? AND status='running' AND deleted=0",(gid,))
    c.commit(); c.close(); return _ok()

@app.post("/action/retry_job")
def retry_job(id:int):
    c=db();x=c.cursor();x.execute("UPDATE jobs SET status='queued', cancel_requested=0, deleted=0, updated=? WHERE id=?",(now(),id))
    c.commit(); c.close(); return _ok()

@app.post("/action/retry_failed_group")
def retry_failed_group(gid:str):
    c=db();x=c.cursor();x.execute("UPDATE jobs SET status='queued', cancel_requested=0, deleted=0, updated=? WHERE group_id=? AND status='failed'",(now(),gid))
    c.commit(); c.close(); return _ok()

@app.post("/action/delete_job")
def delete_job(id:int):
    c=db();x=c.cursor();x.execute("SELECT status FROM jobs WHERE id=?", (id,))
    r=x.fetchone()
    if not r: c.close(); return _ok()
    st=(r["status"] or "").lower()
    if st=="running":
        x.execute("UPDATE jobs SET deleted=1, cancel_requested=1 WHERE id=?", (id,))
    else:
        x.execute("DELETE FROM jobs WHERE id=?", (id,))
        x.execute("DELETE FROM job_frames WHERE job_id=?", (id,))
    c.commit(); c.close(); return _ok()

@app.post("/action/delete_group")
def delete_group(gid:str):
    c=db();x=c.cursor();x.execute("SELECT COUNT(1) AS n FROM jobs WHERE group_id=? AND status='running'", (gid,))
    running=(x.fetchone() or {"n":0})["n"]
    if running and running>0:
        x.execute("UPDATE jobs SET deleted=1 WHERE group_id=?", (gid,))
        x.execute("UPDATE jobs SET cancel_requested=1 WHERE group_id=? AND status='running'", (gid,))
    else:
        x.execute("DELETE FROM jobs WHERE group_id=?", (gid,))
    c.commit(); c.close(); return _ok()

@app.post("/purge_finished")
def purge_finished(days:int=30):
    cutoff=now()-days*86400
    c=db();x=c.cursor()
    x.execute("DELETE FROM jobs WHERE updated<? AND status IN ('done','failed','cancelled')",(cutoff,))
    x.execute("DELETE FROM job_frames WHERE job_id NOT IN (SELECT id FROM jobs)")
    c.commit();c.close(); return _ok()

@app.post("/purge_deleted")
def purge_deleted():
    c=db();x=c.cursor();x.execute("DELETE FROM jobs WHERE deleted=1 AND status!='running'")
    x.execute("DELETE FROM job_frames WHERE job_id NOT IN (SELECT id FROM jobs)")
    c.commit();c.close(); return _ok()

# --------------- Worker lifecycle ---------------
@app.post("/register_worker")
def register_worker(payload:Dict[str,Any]):
    if payload.get("join_secret")!=JOIN_SECRET: raise HTTPException(401,"Invalid join secret")
    name=payload.get("name") or f"worker-{secrets.token_hex(3)}"; api_key=secrets.token_hex(16)
    c=db();x=c.cursor()
    try:
        x.execute("INSERT INTO workers(name, api_key, last_seen) VALUES(?,?,?)",(name,api_key,now()))
        c.commit(); wid=x.lastrowid
    except sqlite3.IntegrityError:
        x.execute("UPDATE workers SET api_key=?, last_seen=? WHERE name=?", (api_key,now(),name)); c.commit()
        x.execute("SELECT id FROM workers WHERE name=?", (name,)); wid=x.fetchone()["id"]
    c.close(); return {"worker_id":wid,"api_key":api_key}

@app.get("/next_job")
def next_job(worker_id:int, api_key:str):
    worker_from_auth(worker_id, api_key)
    c=db();x=c.cursor();x.execute("SELECT * FROM jobs WHERE status='queued' AND deleted=0 ORDER BY priority DESC, id ASC LIMIT 1")
    row=x.fetchone()
    if not row:
        x.execute("UPDATE workers SET last_seen=? WHERE id=?", (now(),worker_id)); c.commit(); c.close()
        return JSONResponse({"job":None})
    jid=row["id"]; x.execute("UPDATE jobs SET status='running', worker_id=?, updated=?, cancel_requested=0 WHERE id=?",(worker_id,now(),jid))
    c.commit(); x.execute("SELECT * FROM jobs WHERE id=?", (jid,)); job=dict(x.fetchone()); c.close()
    return {"job":job}

@app.post("/job_update")
async def job_update(payload:Dict[str,Any]):
    worker_from_auth(payload.get("worker_id"), payload.get("api_key"))
    jid=payload.get("job_id"); 
    if not jid: raise HTTPException(400,"job_id required")
    status=payload.get("status"); log_tail=payload.get("log_tail",None)
    ft=payload.get("frame_total"); fd=payload.get("frame_done"); ff=payload.get("frame_failed"); fr=payload.get("frame_running")
    eta=payload.get("eta_seconds"); err=payload.get("error_inc")
    c=db();x=c.cursor();x.execute("""SELECT status,retries,max_retries,cancel_requested,
                                     frame_total,frame_done,frame_failed,frame_running,error_count FROM jobs WHERE id=?""",(jid,))
    row=x.fetchone(); 
    if not row: c.close(); raise HTTPException(404,"job not found")
    def ival(v,d):
        try:
            if v is None: return d
            return int(v)
        except:
            return d
    new_total=ival(ft, row["frame_total"] or 0); new_done=ival(fd, row["frame_done"] or 0)
    new_fail=ival(ff, row["frame_failed"] or 0); new_run=ival(fr, row["frame_running"] or 0)
    hi=new_total if new_total>0 else 999999
    new_done=max(0,min(hi,new_done)); new_fail=max(0,min(hi,new_fail)); new_run=max(0,min(hi,new_run))
    sets=["updated=?"]; vals=[now()]
    if status: sets.append("status=?"); vals.append(status)
    if log_tail is not None: sets.append("log_tail=?"); vals.append((log_tail or "")[-4000:])
    sets+=["frame_total=?","frame_done=?","frame_failed=?","frame_running=?"]; vals+=[new_total,new_done,new_fail,new_run]
    if eta is not None:
        try: eta_f=float(eta); sets.append("eta_seconds=?"); vals.append(eta_f)
        except: pass
    if err:
        try: inc=int(err); cur=int(row["error_count"] or 0); sets.append("error_count=?"); vals.append(cur+inc)
        except: pass
    vals.append(jid); x.execute(f"UPDATE jobs SET {', '.join(sets)} WHERE id=?", vals); c.commit()
    cancel_req=bool(row["cancel_requested"])
    if (status or "").lower()=="failed" and not cancel_req:
        retries=int(row["retries"] or 0); maxr=int(row["max_retries"] or 0)
        if retries<maxr:
            x.execute("UPDATE jobs SET status='queued', retries=?, updated=?, worker_id=NULL WHERE id=?", (retries+1, now(), jid))
            c.commit()
    x.execute("SELECT cancel_requested FROM jobs WHERE id=?", (jid,)); cancel_now=bool((x.fetchone() or {"cancel_requested":0})["cancel_requested"])
    c.close()
    await bus.publish("job", {"job_id":jid,"status":status,"frame_done":new_done,"frame_failed":new_fail,"frame_total":new_total})
    return {"ok":True,"cancel":cancel_now}

if __name__=="__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
