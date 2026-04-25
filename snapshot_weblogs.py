from weblogs.normalize_pipeline import PipelineMetadata
from weblogs.normalize_trace import Trace
import redis.asyncio as redis

r = redis.Redis(decode_responses=True)

async def process_pipeline_metadata(data: PipelineMetadata):
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'input', str(data.parameters.input)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'outdir', str(data.parameters.outdir)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'id', str(data.parameters.pipeline_id)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'samples', str(data.parameters.sample_ids)) # type: ignore
    
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'started', str(data.workflow.start)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'completed', str(data.workflow.complete)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'duration', str(data.workflow.duration)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'success', str(data.workflow.success)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'resume', str(data.workflow.resume)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'succeededCount', str(data.workflow.stats.succeededCount)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'cachedCount', str(data.workflow.stats.cachedCount)) # type: ignore
    await r.hset(f"pipeline_state:{data.parameters.pipeline_id}", 'failedCount', str(data.workflow.stats.failedCount)) # type: ignore
    

async def process_trace(data: Trace, pipeline_id):
    if pipeline_id != "unknown":
        await r.hset(f'trace:{pipeline_id}:{data.task_id}', 'task_id', str(data.task_id)) # type: ignore
        await r.hset(f'trace:{pipeline_id}:{data.task_id}', 'status', str(data.status)) # type: ignore
        await r.hset(f'trace:{pipeline_id}:{data.task_id}', 'name', str(data.name)) # type: ignore
        await r.hset(f'trace:{pipeline_id}:{data.task_id}', 'workdir', str(data.workdir)) # type: ignore
        await r.hset(f'trace:{pipeline_id}:{data.task_id}', 'actual_time', str(data.actual_time)) # type: ignore
        await r.hset(f'trace:{pipeline_id}:{data.task_id}', 'cpus', str(data.cpus)) # type: ignore
        await r.hset(f'trace:{pipeline_id}:{data.task_id}', 'realtime', str(data.realtime)) # type: ignore
        await r.hset(f'trace:{pipeline_id}:{data.task_id}', 'hash', str(data.hash)) # type: ignore