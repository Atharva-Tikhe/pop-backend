import redis.asyncio as redis
import redis
import json

r = redis.Redis()

def get_schema():
    return {
        'parameters': {
            'input': None,
            'outdir': None,
            'id': None,
            'samples': None,
        },
        'workflow': {
            'start': None,
            'complete': None,
            'duration': None,
            'success': None,
            'resume': None,
            'stats': {
                'succeededCount': None,
                'cachedCount': None,
                'failedCount': None
            }
        },
        "tasks": []
    }

def transform_to_schema(pipeline_schema, redis_pipeline_data):
    pipeline_schema['parameters']['input'] = redis_pipeline_data.get('input', '')
    pipeline_schema['parameters']['outdir'] = redis_pipeline_data.get('outdir', '')
    pipeline_schema['parameters']['pipeline_id'] = redis_pipeline_data.get('id', '')
    pipeline_schema['parameters']['sample_ids'] = redis_pipeline_data.get('samples', '')

    # Direct workflow metadata
    pipeline_schema['workflow']['start'] = redis_pipeline_data.get('started', '')
    pipeline_schema['workflow']['complete'] = redis_pipeline_data.get('completed', '')
    pipeline_schema['workflow']['duration'] = redis_pipeline_data.get('duration', '')
    pipeline_schema['workflow']['success'] = redis_pipeline_data.get('success', '')
    pipeline_schema['workflow']['resume'] = redis_pipeline_data.get('resume', '')

    # Nested stats metadata
    pipeline_schema['workflow']['stats']['succeededCount'] = redis_pipeline_data.get('succeededCount', '')
    pipeline_schema['workflow']['stats']['cachedCount'] = redis_pipeline_data.get('cachedCount', '')
    pipeline_schema['workflow']['stats']['failedCount'] = redis_pipeline_data.get('failedCount', '')

    return pipeline_schema


def send_snapshot():
    ps_keys = []
    for key in r.scan_iter(match='pipeline_state:*'):    
        ps_keys.append(key)

    tr_keys = []
    for key in r.scan_iter(match='trace:*'):
        tr_keys.append(key)
    
    with r.pipeline(transaction=False) as pipe:
        for key in ps_keys: 
            pipe.hgetall(key)
        ps_results = pipe.execute()

        for key in tr_keys:
            pipe.hgetall(key)
        tr_results = pipe.execute()

    pipeline_snapshots = dict(zip(ps_keys, ps_results))
    trace_snapshots = dict(zip(tr_keys, tr_results))

    active_pipelines = []

    active_pipelines = []
    for trace_id, trace_data in trace_snapshots.items():
        active_pipelines.append({trace_id: trace_data})

    print(active_pipelines)
