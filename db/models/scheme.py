# models/schema.py
from sqlmodel import SQLModel, Field, Relationship
from enum import Enum
from typing import Optional, List
from datetime import datetime
import uuid

class JobStatus(str, Enum):
    pending = 'pending'
    running = 'running'
    completed = 'completed'
    failed = 'failed'

class DataLocationType(str, Enum):
    master = 'master'
    node = 'node'

class TaskStatus(str, Enum):
    pending = 'pending'
    running = 'running'
    completed = 'completed'
    failed = 'failed'

class LogEventType(str, Enum):
    started = 'started'
    completed = 'completed'
    failed = 'failed'
    ping = 'ping'

class PaymentStatus(str, Enum):
    pending = 'pending'
    completed = 'completed'
    failed = 'failed'

class User(SQLModel, table=True):
    user_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    username: str = Field(index=True, nullable=False)
    email: str = Field(index=True, nullable=False)
    password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Node(SQLModel, table=True):
    node_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    username: str
    ram: int
    cpu_cores: int
    ip_address: str
    port: int
    created_at: datetime = Field(default_factory=datetime.utcnow)

class NodeHeartbeat(SQLModel, table=True):
    node_id: uuid.UUID = Field(foreign_key="node.node_id", primary_key=True)
    last_ping: datetime = Field(default_factory=datetime.utcnow)

class Job(SQLModel, table=True):
    job_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    user_id: uuid.UUID = Field(foreign_key="user.user_id")
    status: JobStatus = Field(default=JobStatus.pending)
    created_at: datetime = Field(default_factory=datetime.utcnow)

## This should be in S3 if we want to store them ( no need to keep ids on the db)
class Data(SQLModel, table=True):
    data_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="job.job_id")
    size_bytes: int
    created_at: datetime = Field(default_factory=datetime.utcnow)


class DataLocation(SQLModel, table=True):
    data_id: uuid.UUID = Field(foreign_key="data.data_id", primary_key=True)
    node_id: uuid.UUID = Field(foreign_key="node.node_id", primary_key=True)



class Task(SQLModel, table=True):
    task_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="job.job_id")
    node_id: Optional[uuid.UUID] = Field(default=None, foreign_key="node.node_id")
    data_id: uuid.UUID = Field(foreign_key="data.data_id")
    status: TaskStatus = Field(default=TaskStatus.pending)
    required_ram: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0

class TaskDependency(SQLModel, table=True):
    task_id: uuid.UUID = Field(foreign_key="task.task_id", primary_key=True)
    depends_on_task_id: uuid.UUID = Field(foreign_key="task.task_id", primary_key=True)

class NodeLog(SQLModel, table=True):
    log_id: Optional[int] = Field(default=None, primary_key=True)
    node_id: uuid.UUID = Field(foreign_key="node.node_id")
    task_id: Optional[uuid.UUID] = Field(default=None, foreign_key="task.task_id")
    event_type: LogEventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class Payment(SQLModel, table=True):
    payment_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    node_id: uuid.UUID = Field(foreign_key="node.node_id")
    amount: float
    task_id: Optional[uuid.UUID] = Field(default=None, foreign_key="task.task_id")
    status: PaymentStatus = Field(default=PaymentStatus.pending)
    created_at: datetime = Field(default_factory=datetime.utcnow)

class NodeResources(SQLModel, table=True):
    node_id: uuid.UUID = Field(foreign_key="node.node_id", primary_key=True)
    rem_ram: int
    rem_cpu_cores: int
