from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.model import Pipeline, PipelineTask, Cohort
from weblogs.normalize_pipeline import PipelineMetadata, CohortMetadata
from weblogs.normalize_trace import Trace


class ExecutionService:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_cohort(self, metadata: CohortMetadata) -> Cohort:
        """
        Should run at the start of POP submission;
        represent entire samplesheet based cohort
        """
        cohort = Cohort(
            name=metadata.name,
            samplesheet=metadata.samplesheet,
            threshold=metadata.threshold,
            panel=metadata.panel,
            output_dir=metadata.output_dir,
            status=metadata.status,
        )

        self.db.add(cohort)

        await self.db.commit()
        await self.db.refresh(cohort)

        return cohort

        result = await self.db.execute(select(Cohort).where(Cohort.id == metadata.id))

        cohort = result.scalar_one_or_none()

        if cohort is None:
            cohort = Cohort(
                name=metadata.name,
                samplesheet=metadata.samplesheet,
                threshold=metadata.threshold,
                panel=metadata.panel,
                output_dir=metadata.output_dir,
            )
            self.db.add(cohort)

        cohort.updated_at = datetime.utcnow()  # type: ignore

        pipeline_result = await self.db.execute(
            select(Pipeline).where(Pipeline.cohort_id == cohort.id)
        )
        pipelines = pipeline_result.scalars().all()

        if not pipelines:
            await self.db.commit()
            await self.db.refresh(cohort)
            return cohort

        if any(p.status == "FAILED" for p in pipelines):
            cohort.status = "PARTIAL_FAILURE"  # type: ignore
        elif all(p.status == "COMPLETED" for p in pipelines):
            cohort.status = "COMPLETED"  # type: ignore
        else:
            cohort.status = "RUNNING"  # type: ignore

        # print(cohort)

        await self.db.commit()
        await self.db.refresh(cohort)

        return cohort

    async def update_cohort_status(self, id):
        result = await self.db.execute(select(Pipeline).where(Pipeline.cohort_id == id))

        pipelines = result.scalars().all()

        cohort_result = await self.db.execute(select(Cohort).where(Cohort.id == id))

        cohort = cohort_result.scalar_one_or_none()

        if cohort is None:
            raise ValueError(f"Cohort {id} does not exist")

        if not pipelines:
            cohort.status = "SUBMITTED"  # type: ignore

        elif any(pipeline.status == "FAILED" for pipeline in pipelines):
            cohort.status = "PARTIAL_FAILURE"  # type: ignore

        elif all(pipeline.status == "COMPLETED" for pipeline in pipelines):
            cohort.status = "COMPLETED"  # type: ignore
        else:
            cohort.status = "RUNNING"  # type: ignore

        await self.db.commit()
        await self.db.refresh(cohort)

        return cohort

    async def upsert_pipeline(self, metadata: PipelineMetadata) -> Pipeline:
        """
        Creates or updates a pipeline execution.
        """
        cohort_id = metadata.parameters.cohort_id

        if cohort_id is None:
            raise ValueError(f"Pipeline {metadata.runId} does not have a cohort id")

        result = await self.db.execute(select(Cohort).where(Cohort.id == cohort_id))

        cohort = result.scalar_one_or_none()

        if cohort is None:
            raise ValueError(
                f"Pipeline {metadata.runId} references "
                f"non-existent cohort {cohort_id}"
            )

        result = await self.db.execute(
            select(Pipeline).where(Pipeline.id == metadata.runId)
        )

        pipeline = result.scalar_one_or_none()

        if pipeline is None:
            pipeline = Pipeline(
                id=metadata.runId,
                cohort_id=cohort.id,
            )

            self.db.add(pipeline)

        pipeline.cohort_id = cohort.id
        pipeline.name = metadata.workflow.runName  # type: ignore
        pipeline.sample_name = metadata.parameters.sample_ids  # type: ignore
        pipeline.input = metadata.parameters.input  # type: ignore
        pipeline.duration = metadata.workflow.duration  # type: ignore
        pipeline.success = metadata.workflow.success  # type: ignore

        pipeline.status = self._calculate_pipeline_status(metadata)  # type: ignore

        pipeline.manifest = metadata.model_dump(mode="json")  # type: ignore

        await self.db.commit()
        await self.db.refresh(pipeline)

        # Update cohort status after pipeline update.
        await self.update_cohort_status(cohort.id)

        return pipeline

        result = await self.db.execute(
            select(Pipeline).where(Pipeline.id == metadata.runId)
        )

        pipeline = result.scalar_one_or_none()

        if pipeline is None:
            pipeline = Pipeline(
                id=metadata.runId,
                created_at=datetime.utcnow(),
                cohort_id=metadata.parameters.cohort_id,
            )
            self.db.add(pipeline)

        pipeline.cohort_id = metadata.parameters.cohort_id  # type: ignore
        # pipeline.name = metadata.parameters.pipeline_id # type: ignore
        pipeline.name = metadata.workflow.runName  # type: ignore
        pipeline.sample_name = metadata.parameters.sample_ids  # type: ignore
        pipeline.input = metadata.parameters.input  # type: ignore
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

        pipeline.duration = metadata.workflow.duration  # type: ignore
        pipeline.success = metadata.workflow.success  # type: ignore

        pipeline.status = self._calculate_pipeline_status(metadata)  # type: ignore

        pipeline.manifest = metadata.model_dump()  # type: ignore

        pipeline.updated_at = datetime.utcnow()  # type: ignore

        await self.db.commit()
        await self.db.refresh(pipeline)

        return pipeline

    async def upsert_task(self, trace: Trace) -> PipelineTask:
        """
        Creates or updates a Nextflow task.
        """
        result = await self.db.execute(
            select(Pipeline).where(Pipeline.id == trace.runId)
        )

        pipeline = result.scalar_one_or_none()

        if pipeline is None:
            raise ValueError(
                f"Cannot create task {trace.task_id}: "
                f"pipeline {trace.runId} does not exist"
            )

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

        task.process_name = trace.name  # type: ignore
        task.hash = trace.hash  # type: ignore
        task.status = trace.status  # type: ignore
        task.workdir = trace.workdir  # type: ignore
        task.cpus = trace.cpus  # type: ignore
        task.runtime = trace.actual_time  # type: ignore

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

    async def get_cohorts(self):
        result = await self.db.execute(select(Cohort))
        return result.scalars().all()

    async def get_pipelines(self):
        result = await self.db.execute(
            select(Pipeline)
            .where(Pipeline.status == "RUNNING")
            .order_by(Pipeline.created_at.desc())
        )

        return result.scalars().all()

    async def get_pipeline(self, id):
        result = await self.db.execute(select(Pipeline, Pipeline.id))

        return result.scalars().all()

    async def get_tasks(self, runId):
        result = await self.db.execute(
            select(PipelineTask)
            .where(PipelineTask.pipeline_id == runId)
            .order_by(PipelineTask.task_id.desc())
        )
        return result.scalars().all()

    async def get_past_exec(self):
        result = await self.db.execute(
            select(Pipeline)
            .where(Pipeline.status != "RUNNING")
            .order_by(Pipeline.created_at.desc())
        )
        return result.scalars().all()

