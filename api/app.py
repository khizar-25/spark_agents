# api/app.py
import asyncio, json, logging, uuid
from typing import Any, Dict, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from agent.agent import SparkAgent, AgentRunResult
from config.goal_state import SparkGoalState, PipelineGoalType, etl_goal, streaming_goal, dq_goal

logger = logging.getLogger(__name__)
app = FastAPI(title="Spark Agent API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_jobs: Dict[str, Dict[str, Any]] = {}

class RunJobRequest(BaseModel):
    goal_description: str
    source_path: str
    target_path: str
    input_format: str = "csv"
    output_format: str = "delta"
    write_mode: str = "append"
    partition_cols: list = []
    transformations: list = ["deduplicate","cast_types","handle_nulls","add_audit_cols"]
    dq_rules: dict = {"null_threshold":0.01,"dupe_check":True}

class QuickRunRequest(BaseModel):
    preset: str
    source: str
    target: str

@app.get("/")
def root(): return {"message":"⚡ Spark Agent v2.0 is running","docs":"/docs"}

@app.get("/health")
def health(): return {"status":"ok","active_jobs":sum(1 for j in _jobs.values() if j["status"]=="running")}

@app.post("/run")
async def run_job(req: RunJobRequest, bg: BackgroundTasks):
    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {"status":"running","steps":[],"result":None}
    goal = SparkGoalState(
        goal_type=PipelineGoalType.ETL_BATCH,
        description=req.goal_description,
        source_path=req.source_path, target_path=req.target_path,
        input_format=req.input_format, output_format=req.output_format,
        write_mode=req.write_mode, partition_cols=req.partition_cols,
        transformations=req.transformations, dq_rules=req.dq_rules,
    )
    bg.add_task(_run_async, job_id, goal)
    return {"job_id":job_id,"status":"started"}

@app.post("/run/quick")
async def quick_run(req: QuickRunRequest, bg: BackgroundTasks):
    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {"status":"running","steps":[],"result":None}
    presets = {
        "etl":       etl_goal(req.source, req.target),
        "streaming": streaming_goal("kafka://localhost:9092","events",req.target),
        "dq":        dq_goal(req.source, req.target),
    }
    goal = presets.get(req.preset)
    if not goal: raise HTTPException(400, f"Unknown preset: {req.preset}")
    bg.add_task(_run_async, job_id, goal)
    return {"job_id":job_id,"preset":req.preset,"status":"started"}

@app.get("/stream/{job_id}")
async def stream(job_id: str):
    if job_id not in _jobs: raise HTTPException(404)
    async def gen():
        last = 0
        while True:
            job = _jobs.get(job_id,{})
            for s in job.get("steps",[])[last:]:
                yield f"data: {json.dumps(s)}\n\n"; last+=1
            if job.get("status") in ("done","failed") and job.get("result"):
                yield f"data: {json.dumps({'type':'final','result':job['result']})}\n\n"; break
            await asyncio.sleep(0.5)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    if job_id not in _jobs: raise HTTPException(404)
    return _jobs[job_id]

@app.get("/jobs")
def list_jobs(): return {"jobs":[{"id":k,"status":v["status"]} for k,v in _jobs.items()]}

async def _run_async(job_id: str, goal: SparkGoalState):
    def on_step(d): d["type"]="step"; _jobs[job_id]["steps"].append(d)
    try:
        agent = SparkAgent(on_step=on_step)
        loop = asyncio.get_event_loop()
        result: AgentRunResult = await loop.run_in_executor(None, agent.run, goal)
        _jobs[job_id]["status"] = "done" if result.success else "failed"
        _jobs[job_id]["result"] = result.to_dict()
    except Exception as e:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["result"] = {"error":str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.app:app", host="0.0.0.0", port=8000, reload=True)
