from celery import Celery
import redis
import json
import subprocess
import time

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
    print(f'publishing: {pipeline_id} - {event} - {payload}')
    r.publish(
        f"pipeline",
        json.dumps({
            "pipeline_id": pipeline_id,
            "event": event,
            "payload": payload or {}
        })
    )



@celery.task
def run_pipeline(pipeline_id, input):
    emit(pipeline_id, "submitted")

    emit(pipeline_id, "nextflow_started")

    time.sleep(15)

    process = subprocess.Popen(
        ["echo", f"{input}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    emit(pipeline_id, "running")

    for line in process.stdout: # type:ignore
        print(line)
        emit(pipeline_id, "log", {"line": line.strip()})

    process.wait()

    emit(pipeline_id, "completed", {"exit_code": process.returncode})