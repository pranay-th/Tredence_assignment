from typing import Callable, Any
from pydantic import BaseModel, Field


class NodeDef(BaseModel):
    name: str
    func: str  
    meta: dict = Field(default_factory=dict)  
