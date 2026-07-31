import json
import uuid
import redis.asyncio as redis

from pathlib import Path
from fastapi import (
    FastAPI,
    UploadFile,
    File,
    HTTPException,
    WebSocket,
    Request,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from validators.upload_validator import UploadValidator

from models import PostBody
from celery_app import celery, run_pipeline
from connection_manager import ConnectionManager

from weblogs.normalize_pipeline import PipelineMetadata
from weblogs.normalize_trace import Trace
from weblogs.redis_hash_to_dict import get_schema, transform_to_schema
from snapshot_weblogs import process_pipeline_metadata, process_trace
from service import create_pipeline, update_pipeline

from utils.make_manifest_df import manifest_to_df

r = redis.Redis(decode_responses=True)

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://0.0.0.0:5173",
    "http://daedalus.ncl.ac.uk:5173",
]

app = FastAPI()  # type: ignore

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

manager = ConnectionManager()

UPLOAD_DIR = Path("uploads")
INPUT_DIR = Path("pipeline_inputs")

UPLOAD_DIR.mkdir(exist_ok=True)
INPUT_DIR.mkdir(exist_ok=True)


@app.post("/upload")
async def upload(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    validator = UploadValidator(files)
    manifest = await validator.save_files_and_get_samples()
    flat_manifest = validator.flatten_manifest()

    return flat_manifest


@app.post("/submit")
async def start_pipeline(body: PostBody):
    """
    flow:
        1. create a new entry in db
        2. pipeline_id is given by pgsql (remove python) TODO
        3. update redis hashes
        4. use celery to launch (TODO: Add priority queues)
        5. send back pipeline id
    """
    pipeline_id = str(uuid.uuid4())
    df = manifest_to_df(body.data)
    input_path = INPUT_DIR / f"{pipeline_id}.csv"
    df.to_csv(input_path)

    try:
        task = run_pipeline.delay(pipeline_id, str(input_path))  # type: ignore
        if task.id:
            await create_pipeline(df, input_path, pipeline_id)
            seed_pipeline = get_schema()
            payload = transform_to_schema(
                seed_pipeline,
                {
                    "id": pipeline_id,
                    "input": str(input_path),
                    "outdir": "",
                    "samples": ", ".join(df["sample_name"].astype(str)),
                    "started": "",
                    "completed": "",
                    "duration": "",
                    "success": "",
                    "resume": "",
                    "succeededCount": "",
                    "cachedCount": "",
                    "failedCount": "",
                },
            )
            await r.hset(
                f"pipeline_state:{pipeline_id}",
                mapping={
                    "id": pipeline_id,
                    "input": str(input_path),
                    "outdir": "",
                    "samples": ", ".join(df["sample_name"].astype(str)),
                    "started": "",
                    "completed": "",
                    "duration": "",
                    "success": "",
                    "resume": "",
                    "succeededCount": "",
                    "cachedCount": "",
                    "failedCount": "",
                },
            )  # type: ignore
            return JSONResponse(
                {
                    "cel_job_id": task.id,
                    "pipeline_id": pipeline_id,
                    "seedPipeline": payload,
                }
            )
    except Exception as e:
        print(f"Failed to start task: {e}")
        raise HTTPException(
            status_code=500, detail="Internal Broker Error: Could not start pipeline"
        )


@app.post("/nextflow/weblog")
async def get_execution_summary(request: Request):
    payload = await request.json()

    if "metadata" in payload.keys():
        payload["metadata"]["runId"] = payload["runId"]
        pipeline_update = PipelineMetadata(**payload["metadata"])
        await process_pipeline_metadata(pipeline_update)

        await r.set(
            f'run_to_pid:{payload["runId"]}',
            pipeline_update.parameters.pipeline_id,
            ex=86400,
        )

        await r.publish(
            f"updates:{pipeline_update.parameters.pipeline_id}",
            pipeline_update.model_dump_json(),
        )
        print(f"published(pipeline): {pipeline_update.parameters.pipeline_id}")

    if "trace" in payload.keys():
        trace_update = Trace(**payload["trace"])

        pid = await r.get(f'run_to_pid:{payload["runId"]}')
        if pid:
            await process_trace(trace_update, pid)
            await r.publish(f"updates:{pid}", trace_update.model_dump_json())

            print(f"published(trace): {pid}")
        else:
            print(f"Trace without associated pid: {payload['runId']}")


@app.get("/snapshot")
async def send_snapshot():
    # match_keys will be a list of keys like ['pipeline_state:1', 'pipeline_state:2']
    ps_keys = []
    async for key in r.scan_iter(match="pipeline_state:*"):
        ps_keys.append(key)

    tr_keys = []
    async for key in r.scan_iter(match="trace:*"):
        tr_keys.append(key)

    async with r.pipeline(transaction=False) as pipe:
        for key in ps_keys:
            pipe.hgetall(key)
        ps_results = await pipe.execute()

        for key in tr_keys:
            pipe.hgetall(key)
        tr_results = await pipe.execute()

    pipeline_snapshots = dict(zip(ps_keys, ps_results))
    trace_snapshots = dict(zip(tr_keys, tr_results))

    active_pipelines = []
    for pipeline_id, pipeline_data in pipeline_snapshots.items():
        if pipeline_data["completed"] == "None":
            pipeline = get_schema()
            pipeline["parameters"]["input"] = pipeline_data.get("input", "")
            pipeline["parameters"]["outdir"] = pipeline_data.get("outdir", "")
            pipeline["parameters"]["pipeline_id"] = str(pipeline_id).split(":")[1]
            pipeline["parameters"]["sample_ids"] = pipeline_data.get("samples", "")

            pipeline["workflow"]["start"] = pipeline_data["started"]
            pipeline["workflow"]["complete"] = pipeline_data["completed"]
            pipeline["workflow"]["duration"] = pipeline_data["duration"]
            pipeline["workflow"]["success"] = pipeline_data["success"]
            pipeline["workflow"]["resume"] = pipeline_data["resume"]
            pipeline["workflow"]["stats"]["succeededCount"] = pipeline_data[
                "succeededCount"
            ]
            pipeline["workflow"]["stats"]["cachedCount"] = pipeline_data["cachedCount"]
            pipeline["workflow"]["stats"]["failedCount"] = pipeline_data["failedCount"]
            active_pipelines.append(pipeline)

    for trace_id, trace_data in trace_snapshots.items():
        for pipeline in active_pipelines:
            if pipeline["parameters"]["id"] == trace_id.split(":")[1]:
                pipeline["tasks"].append(trace_data)

    return JSONResponse(
        {
            "redis_data": active_pipelines,
        }
    )


@app.websocket("/ws/pipeline/{pipeline_id}")
async def websocket_endpoint(pipeline_id: str, websocket: WebSocket):
    await manager.connect(pipeline_id, websocket)
    pubsub = r.pubsub()
    await pubsub.subscribe(f"updates:{pipeline_id}")

    try:
        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue
            data = json.loads(msg["data"])
            await manager.send_pipeline_updates(pipeline_id, data, websocket)
            print(f"sent {data} for {pipeline_id} using {websocket}")

    except WebSocketDisconnect:
        manager.disconnect(pipeline_id, websocket)
        print(f"disconnected {pipeline_id}")
