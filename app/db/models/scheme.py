from sqlalchemy import Column, String, Integer, Float, DateTime, ForeignKey, Enum as SQLEnum, UniqueConstraint, Boolean 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from enum import Enum
from typing import Optional, List
from datetime import datetime
import uuid
from app.utils.constants import DEFAULT_PORT
from sqlalchemy.dialects.postgresql import BIGINT
# everything concerned with data is in bytes

Base = declarative_base()

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

class DataStatus(str, Enum):
    pending = 'pending'
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

class JobType(str, Enum):
    simple = 'simple'
    complex = 'complex'     

class User(Base):
    __tablename__ = "user"
    
    user_id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, nullable=False, index=True, unique=True) 
    email = Column(String, nullable=False, index=True, unique=True)
    password = Column(String)
    created_at = Column(DateTime, default=datetime.now)

    # Relationships
    jobs = relationship("Job", back_populates="user")
    nodes = relationship("Node", back_populates="user")


# ram is in bytes, cpu_cores is in cores
class Node(Base):
    __tablename__ = "node"

    node_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("user.user_id"))
    node_name = Column(String, default= lambda: str(uuid.uuid4()))
    ram = Column(BIGINT, nullable=False)  # RAM in bytes
    ip_address = Column(String, nullable=False)
    port = Column(Integer, default=DEFAULT_PORT)
    created_at = Column(DateTime, default=datetime.now)
    machine_fingerprint = Column(String, nullable=False)
    is_alive = Column(Boolean, default=False)   

    # Relationships
    heartbeat = relationship("NodeHeartbeat", back_populates="node", uselist=False)
    resources = relationship("NodeResources", back_populates="node", uselist=False)
    tasks = relationship("Task", back_populates="node")
    logs = relationship("NodeLog", back_populates="node")
    payments = relationship("Payment", back_populates="node")
    user = relationship("User", back_populates="nodes")

    __table_args__ = (
        UniqueConstraint("machine_fingerprint", "user_id", name="uq_machine_fingerprint_user_id"), # Ensure user doesn't have duplicate node names
    )

class NodeHeartbeat(Base):
    __tablename__ = "node_heartbeat"
    
    node_id = Column(Integer, ForeignKey("node.node_id"), primary_key=True)
    last_ping = Column(DateTime, default=datetime.now)

    # Relationships
    node = relationship("Node", back_populates="heartbeat")

class Job(Base):
    __tablename__ = "job"
    
    job_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("user.user_id"))
    job_name = Column(String)
    job_type = Column(SQLEnum(JobType))
    status = Column(SQLEnum(JobStatus), default=JobStatus.pending)
    created_at = Column(DateTime, default=datetime.now)
    script_name = Column(String)
    script_path = Column(String, nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="jobs")
    tasks = relationship("Task", back_populates="job")
    data_files = relationship("Data", back_populates="job")


class Data(Base):
    __tablename__ = "data"
    
    data_id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String)
    job_id = Column(Integer, ForeignKey("job.job_id"))
    parent_task_id = Column(Integer, ForeignKey("task.task_id"), nullable=True)
    data_location = Column(SQLEnum(DataLocationType), default=DataLocationType.master)
    created_at = Column(DateTime, default=datetime.now)
    status = Column(SQLEnum(DataStatus), default=DataStatus.pending)    
    
    # Relationships
    job = relationship("Job", back_populates="data_files")
    dependent_tasks = relationship("Task", secondary="task_data_dependency", back_populates="data_dependencies")
    parent_task = relationship("Task", back_populates="data_files")

class Task(Base):
    __tablename__ = "task"
    task_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("job.job_id"))
    node_id = Column(Integer, ForeignKey("node.node_id"))
    status = Column(SQLEnum(TaskStatus), default=TaskStatus.pending)
    required_ram = Column(BIGINT)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    retry_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now)
    

    # Relationships
    job = relationship("Job", back_populates="tasks")
    node = relationship("Node", back_populates="tasks")
    data_files = relationship("Data", back_populates="parent_task")
    data_dependencies = relationship("Data", secondary="task_data_dependency", back_populates="dependent_tasks")
    logs = relationship("NodeLog", back_populates="task")
    payment = relationship("Payment", back_populates="task", uselist=False)
    data_files = relationship("Data", back_populates="parent_task")


class TaskDataDependency(Base):
    __tablename__ = "task_data_dependency"
    task_id = Column(Integer, ForeignKey("task.task_id"), primary_key=True)
    data_id = Column(Integer, ForeignKey("data.data_id"), primary_key=True)

class NodeLog(Base):
    __tablename__ = "node_log"
    
    log_id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(Integer, ForeignKey("node.node_id"))
    task_id = Column(Integer, ForeignKey("task.task_id"), nullable=True)
    event_type = Column(SQLEnum(LogEventType))
    timestamp = Column(DateTime, default=datetime.now)

    # Relationships
    node = relationship("Node", back_populates="logs")
    task = relationship("Task", back_populates="logs")

class Payment(Base):
    __tablename__ = "payment"
    
    payment_id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(Integer, ForeignKey("node.node_id"))
    amount = Column(Float)
    task_id = Column(Integer, ForeignKey("task.task_id"), unique=True)
    status = Column(SQLEnum(PaymentStatus), default=PaymentStatus.pending)
    created_at = Column(DateTime, default=datetime.now)

    # Relationships
    node = relationship("Node", back_populates="payments")
    task = relationship("Task", back_populates="payment") 

class NodeResources(Base):
    __tablename__ = "node_resources"
    
    node_id = Column(Integer, ForeignKey("node.node_id"), primary_key=True)
    rem_ram = Column(BIGINT)
    rem_cpu_cores = Column(Integer)

    # Relationships
    node = relationship("Node", back_populates="resources")