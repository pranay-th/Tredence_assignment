from typing import Any, Dict, List
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class StateModel(BaseModel):
    data: Dict[str, Any] = Field(default_factory=dict)


class RunModel(BaseModel):
    run_id: str
    graph_id: str
    status: RunStatus = RunStatus.PENDING
    state: StateModel = Field(default_factory=StateModel)
    logs: List[str] = Field(default_factory=list)
    current_node: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
