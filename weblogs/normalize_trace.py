from pydantic import BaseModel, Field, model_validator
from typing import Optional
import json

class Trace(BaseModel):
    task_id: int
    status: str
    hash: str
    name: str
    workdir: str
    realtime: Optional[int] = 0    
    actual_time: float = 0.0

    @model_validator(mode='after')
    def calculate_time(self) -> 'Trace':
        if self.realtime:
            self.actual_time = self.realtime / 1000
        return self
    
    

test = json.load(open('weblog_completed.json'))
model = Trace(**test['trace'])
print(model.actual_time)