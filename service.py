from db.model import Pipeline
from db.db import SessionLocal
from pandas import DataFrame

async def create_pipeline(manifest: DataFrame, input_path, pipeline_id):
    async with SessionLocal() as session:
        pipeline = Pipeline(
            id = pipeline_id,
            manifest=manifest.to_json(),
            name = None,
            input = str(input_path)
        )
        session.add(pipeline)
        await session.commit()
        await session.refresh(pipeline)
        print(f'CREATED: {pipeline}')
        return pipeline.id

async def update_pipeline(pipeline_id, status):
    async with SessionLocal() as session:
        pipeline = await session.get(Pipeline, pipeline_id)
        pipeline.status = status # type: ignore
        await session.commit()