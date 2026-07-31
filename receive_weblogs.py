from fastapi import (
    FastAPI,
    Request,
)
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
# import json
from weblogs.normalize_pipeline import PipelineMetadata
from weblogs.normalize_trace import Trace

from db.db import SessionLocal 
from db.services.execution_service import ExecutionService
from weblogs.normalize_pipeline import PipelineMetadata
from weblogs.normalize_trace import Trace
from db.model import Pipeline, PipelineTask
from sqlalchemy import delete, select
from fastapi import Form, UploadFile, File, status, HTTPException
from typing import List, LiteralString
import pandas as pd
import io
import subprocess
import os

app = FastAPI()

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://0.0.0.0:5173",
    "http://daedalus.ncl.ac.uk:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/nextflow/weblog")
async def get_execution_summary(request: Request):
    payload = await request.json()

    async with SessionLocal() as db:

        service = ExecutionService(db)
        if "metadata" in payload:

            payload["metadata"]["runId"] = payload["runId"]

            metadata = PipelineMetadata(**payload["metadata"])

            await service.upsert_pipeline(metadata)

        if "trace" in payload:

            payload["trace"]["runId"] = payload["runId"]
            payload["trace"]["runName"] = payload["runName"]

            trace = Trace(**payload["trace"])

            await service.upsert_task(trace)

    return JSONResponse(
        content={"message": "Webhook processed"},
        status_code=200,
    )

    if "metadata" in payload.keys():
        payload["metadata"]["runId"] = payload["runId"]
        pipeline_update = PipelineMetadata(**payload["metadata"])
        print(pipeline_update)

    if "trace" in payload.keys():
        payload["trace"]["runId"] = payload["runId"]
        trace_update = Trace(**payload["trace"])
        print(trace_update)
        print(trace_update.runId)

@app.get('/pipelines')
async def get_pipeline(request: Request):
    async with SessionLocal() as db:
        service = ExecutionService(db)
        return await service.get_pipelines()

@app.get('/pipelines/{runId}/tasks')
async def get_tasks(runId: str):
    async with SessionLocal() as db:
        service = ExecutionService(db)
        return await service.get_tasks(runId)

@app.get('/pipelines/past')
async def get_past_pipelines(request: Request):
    async with SessionLocal() as db:
        service = ExecutionService(db)
        return await service.get_past_exec()


@app.post('/submit/sheet')
async def get_uploaded_sheet(output_dir: str = Form(...),
    threshold: str = Form(...),
    is_default: bool = Form(False),
    files: List[UploadFile] = File(...)):
    validated_files = []
    print(threshold, is_default)

    # is_default = bool(is_default)

    template_df = pd.read_csv('samplesheet_b6_single.csv')

    for file in files:
        # Read file into memory
        contents = await file.read()
        
        try:
            # Parse CSV to verify columns
            df = pd.read_csv(io.BytesIO(contents))
            df.to_csv(f'uploads/{file.filename}', index= False)
        
            if list(df.columns) != list(template_df.columns):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File '{file.filename}' is not a valid sample sheet"
                )
        except Exception as e:
            print(e)
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to parse '{file.filename}'. Ensure it is a valid CSV."
            )

        validated_files.append(os.path.abspath(f'uploads/{file.filename}'))
        launched_files = []

        cwd = os.getcwd()
        os.chdir('/home/atharva/dev/pipeline/Atharva-Tikhe-picnac/launcher/')
        for file in validated_files:
            task = subprocess.Popen(f"python3 runner.py {file} {output_dir} {threshold}", shell=True)
            returncode = task.wait()
            print(task.stdout)
            launched_files.append(returncode)
        os.chdir(cwd)

        print(launched_files)
        return {
            "message": f"Sheet submitted! ",
            "output_dir": output_dir,
            "processed_files": validated_files
        }

@app.get('/health')
async def send_health():
    return JSONResponse({'server': 'healthy'})