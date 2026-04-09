from weblogs.normalize_pipeline import PipelineMetadata
from weblogs.normalize_trace import Trace
import redis.asyncio as redis

r = redis.Redis(decode_responses=True)

async def process_pipeline_metadata(data: PipelineMetadata):
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'id', str(data.parameters.pipeline_id)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'started', str(data.workflow.start)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'completed', str(data.workflow.complete)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'numSuccessed', str(data.workflow.stats.succeededCount)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'numFailed', str(data.workflow.stats.failedCount)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'duration', str(data.workflow.duration)) # type: ignore


async def process_trace(data: Trace, pipeline_id):
    await r.hset(f'trace:{pipeline_id}', 'taskId', str(data.task_id)) # type: ignore
    await r.hset(f'trace:{pipeline_id}', 'status', str(data.status)) # type: ignore
    await r.hset(f'trace:{pipeline_id}', 'name', str(data.name)) # type: ignore
    await r.hset(f'trace:{pipeline_id}', 'workdir', str(data.workdir)) # type: ignore
    await r.hset(f'trace:{pipeline_id}', 'actualTime', str(data.actual_time)) # type: ignore