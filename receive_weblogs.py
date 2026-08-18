from fastapi import (
    FastAPI,
    Request,
)
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# import json
from weblogs.normalize_pipeline import PipelineMetadata, CohortMetadata
from weblogs.normalize_trace import Trace

from db.db import SessionLocal
from db.services.execution_service import ExecutionService
from weblogs.normalize_pipeline import PipelineMetadata
from weblogs.normalize_trace import Trace
from db.model import Pipeline, PipelineTask, Cohort
from sqlalchemy import delete, select
from fastapi import Form, UploadFile, File, status, HTTPException
from typing import List
import pandas as pd
import io
import subprocess
import os
import uuid

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


@app.get("/pipelines")
async def get_pipeline(request: Request):
    async with SessionLocal() as db:
        service = ExecutionService(db)
        return await service.get_pipelines()


@app.get("/pipelines/{runId}/tasks")
async def get_tasks(runId: str):
    async with SessionLocal() as db:
        service = ExecutionService(db)
        return await service.get_tasks(runId)


@app.get("/pipelines/past")
async def get_past_pipelines(request: Request):
    async with SessionLocal() as db:
        service = ExecutionService(db)
        return await service.get_past_exec()


@app.post("/submit/sheet")
async def get_uploaded_sheet(
    output_dir: str = Form(...),
    threshold: str = Form(...),
    is_default: bool = Form(False),
    files: List[UploadFile] = File(...),
):
    template_df = pd.read_csv("samplesheet_b6_single.csv")
    validated_files = []
    
    for file in files:
        contents = await file.read()
        
        # try:
        df = pd.read_csv(io.BytesIO(contents))
        
    
        if list(df.columns) != list(template_df.columns):
            print(df.columns)
            print(template_df.columns)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File '{file.filename}' is not a valid sample sheet",
            )
        
        path = f'uploads/{file.filename}'
        df.to_csv(path, index=False)
        # df.to_csv(f"/home/atharva/dev/executions/{output_dir}/{file.filename}", index = False)
        validated_files.append(os.path.abspath(path))
        # except HTTPException:
        #     raise
        # except Exception as e:
        #     raise HTTPException(
        #         status_code=status.HTTP_400_BAD_REQUEST,
        #         detail=f"Failed to parse '{file.filename}'. Ensure it is a valid CSV.",
        #     ) from e
    
    async with SessionLocal() as db:
        service = ExecutionService(db)
        cohort_meta = CohortMetadata(
            samplesheet = str(files[0].filename),
            threshold = threshold,
            output_dir=output_dir
        )
        
        cohort = await service.create_cohort(cohort_meta)
        cohort_id = cohort.id
    
    print(f"Created cohort: {cohort_id}")
        
    
    try:
        for file_path in validated_files:
            result = subprocess.run(
                [
                    "python3",
                    "runner.py",
                    file_path,
                    output_dir,
                    threshold,
                    str(cohort_id),
                ],
                cwd="/home/atharva/dev/pipeline/Atharva-Tikhe-picnac/launcher/",
                check=True,
            )
            print(f"Pipeline completed for {file_path}")
    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Pipeline execution failed "
                f"with exit code {e.returncode}"
            ),
        ) from e
        
    return {
        "message": "Sheet submitted",
        "cohort_id": str(cohort_id),
        "output_dir": output_dir,
        "processed_files": validated_files,
    }

@app.get("/health")
async def send_health():
    return JSONResponse({"server": "healthy"})