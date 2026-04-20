from celery import Celery
import redis
import json
import subprocess
import pandas as pd

celery = Celery(
    "pipeline",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1"
)

celery.conf.update(
    task_track_started =True,
    result_expires = 3600
)

r = redis.Redis(decode_responses=True)

def emit(pipeline_id, event, payload=None):
    r.publish(
        "pipeline",
        json.dumps({
            "pipeline_id": pipeline_id,
            "event": event,
            "payload": payload or {}
        })
    )


@celery.task
def run_pipeline(pipeline_id, input):
    emit(pipeline_id, "submitted")

    sample_names = pd.read_csv(input)['sample_name']

    command = f"source ~/.zshrc && conda activate nextflow && cd ~/ncl/dissertation/test-pipeline/ && nextflow run /Users/atharvatikhe/ncl/dissertation/test-pipeline/main.nf --input {input} --outdir /Users/atharvatikhe/ncl/dissertation/test-pipeline/ --pipeline_id '{pipeline_id}' --sample_ids {' '.join(sample_names)}"

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True
    )

    emit(pipeline_id, "running")

    for line in process.stderr: # type:ignore
        print(f"CELERY WORKER ERROR: {line}")

    process.wait()

    emit(pipeline_id, "completed", {"exit_code": process.returncode})