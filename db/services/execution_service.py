from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.model import Pipeline, PipelineTask
from weblogs.normalize_pipeline import PipelineMetadata
from weblogs.normalize_trace import Trace


class ExecutionService:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def upsert_pipeline(self, metadata: PipelineMetadata) -> Pipeline:
        """
        Creates or updates a pipeline execution.
        """

        result = await self.db.execute(
            select(Pipeline).where(Pipeline.id == metadata.runId)
        )

        pipeline = result.scalar_one_or_none()

        if pipeline is None:
            pipeline = Pipeline(
                id=metadata.runId,
                created_at=datetime.utcnow()
            )
            self.db.add(pipeline)

        # pipeline.name = metadata.parameters.pipeline_id # type: ignore
        pipeline.name = metadata.workflow.runName # type: ignore
        pipeline.input = metadata.parameters.input # type: ignore
        # print(type(metadata.workflow.start_time), type(metadata.workflow.complete_time))

        # pipeline.start_time = ( # type: ignore
        #     datetime.fromisoformat(metadata.workflow.start_time) # type: ignore
        #     if metadata.workflow.start_time
        #     else None
        # )

        # pipeline.end_time = ( # type: ignore
        #     datetime.fromisoformat(metadata.workflow.complete_time) # type: ignore
        #     if metadata.workflow.complete_time
        #     else None
        # )

        pipeline.duration = metadata.workflow.duration # type: ignore
        pipeline.success = metadata.workflow.success # type: ignore

        pipeline.status = self._calculate_pipeline_status(metadata) # type: ignore

        pipeline.manifest = metadata.model_dump() # type: ignore

        pipeline.updated_at = datetime.utcnow() # type: ignore

        await self.db.commit()
        await self.db.refresh(pipeline)

        return pipeline

    async def upsert_task(self, trace: Trace) -> PipelineTask:
        """
        Creates or updates a Nextflow task.
        """

        result = await self.db.execute(
            select(PipelineTask).where(
                PipelineTask.pipeline_id == trace.runId,
                PipelineTask.task_id == trace.task_id,
            )
        )

        task = result.scalar_one_or_none()

        if task is None:
            task = PipelineTask(
                pipeline_id=trace.runId,
                task_id=trace.task_id,
            )

            self.db.add(task)

        task.process_name = trace.name # type: ignore
        task.hash = trace.hash # type: ignore
        task.status = trace.status # type: ignore
        task.workdir = trace.workdir # type: ignore
        task.cpus = trace.cpus # type: ignore
        task.runtime = trace.actual_time # type: ignore

        await self.db.commit()
        await self.db.refresh(task)

        return task

    @staticmethod
    def _calculate_pipeline_status(metadata: PipelineMetadata) -> str:

        if metadata.workflow.complete is None:
            return "RUNNING"

        if metadata.workflow.success:
            return "COMPLETED"

        return "FAILED"

    async def get_pipelines(self):
        result = await self.db.execute(select(Pipeline).where(Pipeline.status == "RUNNING").order_by(Pipeline.created_at.desc()))
        
        return result.scalars().all()

    async def get_pipeline(self, id):
        result = await self.db.execute(select(Pipeline, Pipeline.id))

        return result.scalars().all()

    async def get_tasks(self, runId ):
        result = await self.db.execute(select(PipelineTask).where(PipelineTask.pipeline_id == runId).order_by(PipelineTask.task_id.desc()))
        return result.scalars().all()

    async def get_past_exec(self):
        result = await self.db.execute(select(Pipeline).where(Pipeline.status != "RUNNING").order_by(Pipeline.created_at.desc()))
        return result.scalars().all()
    