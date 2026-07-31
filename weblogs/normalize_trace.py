from pydantic import BaseModel, model_validator
from typing import Optional

class Trace(BaseModel):
    task_id: int
    status: str
    hash: str
    name: str
    workdir: str
    realtime: Optional[int] = 0    
    actual_time: float = 0.0
    cpus: int = 1
    hash: str
    runId: str
    runName: str
    
    @model_validator(mode='after')
    def calculate_time(self) -> 'Trace':
        if self.realtime:
            self.actual_time = self.realtime / 1000
        return self