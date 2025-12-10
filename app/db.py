from sqlmodel import Field, SQLModel, create_engine, Session, JSON
from typing import Optional
import os

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./workflow.db")
engine = create_engine(DATABASE_URL, echo=False, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {})


class GraphORM(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    graph_id: str = Field(index=True, unique=True)
    definition: dict = Field(sa_column=JSON)  # store full graph definition


class RunORM(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(index=True, unique=True)
    graph_id: str = Field(index=True)
    status: str
    state: dict = Field(sa_column=JSON)
    logs: list = Field(sa_column=JSON, default_factory=list)
    metrics: dict = Field(sa_column=JSON, default_factory=dict)
    current_node: Optional[str] = None


def init_db():
    SQLModel.metadata.create_all(engine)


def save_graph(graph_id: str, definition: dict):
    with Session(engine) as s:
        existing = s.exec(GraphORM.select().where(GraphORM.graph_id == graph_id)).first()
        if existing:
            existing.definition = definition
            s.add(existing)
        else:
            g = GraphORM(graph_id=graph_id, definition=definition)
            s.add(g)
        s.commit()


def load_graph(graph_id: str) -> dict | None:
    with Session(engine) as s:
        g = s.exec(GraphORM.select().where(GraphORM.graph_id == graph_id)).first()
        return g.definition if g else None


def list_graphs_db() -> list:
    with Session(engine) as s:
        return [g.definition for g in s.exec(GraphORM.select()).all()]


def save_run(run: dict):
    with Session(engine) as s:
        existing = s.exec(RunORM.select().where(RunORM.run_id == run["run_id"])).first()
        if existing:
            existing.status = run["status"]
            existing.state = run["state"]
            existing.logs = run.get("logs", [])
            existing.metrics = run.get("metrics", {})
            existing.current_node = run.get("current_node")
            s.add(existing)
        else:
            r = RunORM(
                run_id=run["run_id"],
                graph_id=run["graph_id"],
                status=run["status"],
                state=run.get("state", {}),
                logs=run.get("logs", []),
                metrics=run.get("metrics", {}),
                current_node=run.get("current_node"),
            )
            s.add(r)
        s.commit()


def load_run(run_id: str) -> dict | None:
    with Session(engine) as s:
        r = s.exec(RunORM.select().where(RunORM.run_id == run_id)).first()
        if not r:
            return None
        return {
            "run_id": r.run_id,
            "graph_id": r.graph_id,
            "status": r.status,
            "state": r.state or {},
            "logs": r.logs or [],
            "metrics": r.metrics or {},
            "current_node": r.current_node,
        }
