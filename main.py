import json
import uuid
import asyncio
import pandas as pd
import redis.asyncio as redis

from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from validators.upload_validator import UploadValidator

from models import PostBody
from celery_app import celery, run_pipeline
from connection_manager import ConnectionManager

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://0.0.0.0:5173"
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

manager = ConnectionManager()

r = redis.Redis(decode_responses=True)

async def redis_listener():
    pubsub = r.pubsub()
    await pubsub.subscribe("pipeline")
    
    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue

        data = json.loads(msg["data"])
        print(f"from sub: {data}")
        id = data["pipeline_id"]

        await manager.send_to_task(id, data)

@app.on_event("startup")
async def startup():
    asyncio.create_task(redis_listener())


UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
INPUT_DIR = Path("pipeline_inputs")
INPUT_DIR.mkdir(exist_ok=True)

@app.post("/upload")
async def upload(files: List[UploadFile] = File(...)):
    # done
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    validator = UploadValidator(files)
    manifest = await validator.save_files_and_get_samples()
    flat_manifest = validator.flatten_manifest()

    return flat_manifest


# @app.get("/result/{task_id}")
# def get_result(task_id: str):
#     res = celery.AsyncResult(task_id)
#     return {
#         "state": res.state,
#         "result": res.result if res.ready() else None,
#     }


@app.post('/submit')
def start_pipeline(body: PostBody): 
    pipeline_id = str(uuid.uuid4())
    
    rows = []
    for sample in body.data:
        rows.append(json.loads(sample.model_dump_json()))
    
    df = pd.DataFrame(rows)

    input_path = INPUT_DIR / f'{pipeline_id}.csv'
    df.to_csv(input_path)

    # print(input_path)
    task = run_pipeline.delay(pipeline_id, str(input_path)) # type: ignore

    return JSONResponse({'cel_job_id': task.id, 'pipeline_id': pipeline_id})


@app.websocket('/ws/pipeline/{pipeline_id}')
async def websocket_endpoint(websocket: WebSocket, pipeline_id: str):
    await manager.connect(pipeline_id, websocket)

    try:
        while True:
            await websocket.receive_text()
    except Exception:
        manager.disconnect(pipeline_id, websocket)



@app.get("/pipeline")
def get_pipeline():
    temp = json.load(open("./test-execution.json", "r"))
    return JSONResponse(temp)

@app.get("/pipelines/active")
def get_active_pipeline():
    temp = json.load(open("./test-execution.json", "r"))
    temp2 = json.load(open("./test-execution-2.json", "r"))
    return JSONResponse([temp, temp2])

@app.get("/pipeline/{pipeline_id}")
def get_pipeline_by_id(pipeline_id):
    temp = json.load(open('./test-execution.json', 'r'))
    print(temp)
    if temp['pipeline_id'] == pipeline_id:
        return JSONResponse(temp)
    else:
        return HTTPException(404)
    
@app.get("/pipeline/{pipeline_id}/tasks/{task_id}")
def get_task_by_id(pipeline_id, task_id):
    if pipeline_id == '1234':
        temp = json.load(open('./test-execution.json', 'r'))
    else:
        temp = json.load(open('./test-execution-2.json', 'r'))

    if temp['pipeline_id'] == pipeline_id:
        return JSONResponse(temp['tasks'][int(task_id)])
    else:
        return HTTPException(404)