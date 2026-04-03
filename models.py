from pydantic import BaseModel
from typing import Annotated


class flatManifest(BaseModel):
    sample_name: str
    file_type: str
    platform: str
    files: str | dict[str, str]
    priority: str
    analyst_name: str

class PostBody(BaseModel):
    data: list[flatManifest]
