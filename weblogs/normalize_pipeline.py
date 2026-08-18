# import uuid
from uuid import UUID
from pydantic import BaseModel, model_validator, UUID4
from datetime import datetime

class Parameters(BaseModel):
    input: str
    outdir: str
    pipeline_id: str
    sample_ids: str | None
    cohort_id: UUID | None
    threshold: float | str

class TimeStamps(BaseModel):
    dayOfMonth: int
    monthValue: int
    hour: int
    minute: int
    year: int
    
class Stats(BaseModel):
    succeededCount: int
    cachedCount: int
    failedCount: int

class Workflow(BaseModel):
    start: str | TimeStamps
    complete: None | TimeStamps | str
    duration: None | float | int
    success: bool
    resume: bool
    stats: Stats
    runName: str

class PipelineMetadata(BaseModel):
    parameters: Parameters
    workflow: Workflow
    runId: str

    @model_validator(mode='after')
    def calculate_time(self) -> 'PipelineMetadata':
        if isinstance(self.workflow.start, TimeStamps) and isinstance(self.workflow.complete, TimeStamps):
            calculated_start = datetime(self.workflow.start.year,
                            self.workflow.start.monthValue, 
                            self.workflow.start.dayOfMonth,
                            self.workflow.start.hour,
                            self.workflow.start.minute)
            self.workflow.start = str(calculated_start)

            calculated_completed = datetime(self.workflow.complete.year,
                              self.workflow.complete.monthValue, 
                              self.workflow.complete.dayOfMonth,
                              self.workflow.complete.hour,
                              self.workflow.complete.minute)
            self.workflow.complete = str(calculated_completed)
        return self

    @model_validator(mode='after')
    def convert_duration(self) -> 'PipelineMetadata':
        if isinstance(self.workflow.duration, int):
            self.workflow.duration = self.workflow.duration / 1000 
        return self
    
class CohortMetadata(BaseModel):
    name: str = "ALL_cohort"
    samplesheet : str
    status: str = "SUBMITTED"
    threshold : str
    panel : str = 'ALLTOGETHER1'
    output_dir: str