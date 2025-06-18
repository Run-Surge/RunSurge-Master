# models/schema.py
from sqlalchemy import Column, String, Integer, Float, DateTime, ForeignKey, Enum as SQLEnum, UniqueConstraint  
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from enum import Enum
from typing import Optional, List
from datetime import datetime
import uuid
from app.utils.constants import DEFAULT_PORT

# everything concenred with data is in bytes

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

class LogEventType(str, Enum):
    started = 'started'
    completed = 'completed'
    failed = 'failed'
    ping = 'ping'

class PaymentStatus(str, Enum):
    pending = 'pending'
    completed = 'completed'
    failed = 'failed'

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
    node_name = Column(String)
    user_id = Column(Integer, ForeignKey("user.user_id"))
    ram = Column(Integer)
    cpu_cores = Column(Integer)
    ip_address = Column(String, nullable=False)
    port = Column(Integer, default=DEFAULT_PORT)
    created_at = Column(DateTime, default=datetime.now)

    # Relationships
    heartbeat = relationship("NodeHeartbeat", back_populates="node", uselist=False)
    resources = relationship("NodeResources", back_populates="node", uselist=False)
    tasks = relationship("Task", back_populates="node")
    data_locations = relationship("DataLocation", back_populates="node")
    logs = relationship("NodeLog", back_populates="node")
    payments = relationship("Payment", back_populates="node")
    user = relationship("User", back_populates="nodes")

    __table_args__ = (
        UniqueConstraint("node_name", "user_id", name="uq_node_name_user_id"), # Ensure user doesn't have duplicate node names
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
    status = Column(SQLEnum(JobStatus), default=JobStatus.pending)
    created_at = Column(DateTime, default=datetime.now)
    script_name = Column(String, nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="jobs")
    tasks = relationship("Task", back_populates="job")
    # One-to-many relationship: one job can have multiple data files
    data_files = relationship("Data", back_populates="job")


class Data(Base):
    __tablename__ = "data"
    
    data_id = Column(Integer, primary_key=True, autoincrement=True)
    path = Column(String)
    job_id = Column(Integer, ForeignKey("job.job_id"))  # Keep this - Data belongs to Job
    size_bytes = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)

    # Relationships
    job = relationship("Job", back_populates="data_files")
    locations = relationship("DataLocation", back_populates="data")
    tasks = relationship("Task", back_populates="data")

class DataLocation(Base):
    __tablename__ = "data_location"
    
    data_id = Column(Integer, ForeignKey("data.data_id"), primary_key=True)
    node_id = Column(Integer, ForeignKey("node.node_id"), primary_key=True)

    # Relationships
    data = relationship("Data", back_populates="locations")
    node = relationship("Node", back_populates="data_locations")

class Task(Base):
    __tablename__ = "task"
    
    task_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("job.job_id"))
    node_id = Column(Integer, ForeignKey("node.node_id"), nullable=True)
    data_id = Column(Integer, ForeignKey("data.data_id"))
    status = Column(SQLEnum(TaskStatus), default=TaskStatus.pending)
    required_ram = Column(Integer)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    retry_count = Column(Integer, default=0)

    # Relationships
    job = relationship("Job", back_populates="tasks")
    node = relationship("Node", back_populates="tasks")
    data = relationship("Data", back_populates="tasks")
    dependencies = relationship(
        "Task",
        secondary="task_dependency",
        primaryjoin="Task.task_id==TaskDependency.task_id",
        secondaryjoin="Task.task_id==TaskDependency.depends_on_task_id",
        backref="dependent_tasks"
    )
    logs = relationship("NodeLog", back_populates="task")
    payments = relationship("Payment", back_populates="task")

class TaskDependency(Base):
    __tablename__ = "task_dependency"
    
    task_id = Column(Integer, ForeignKey("task.task_id"), primary_key=True)
    depends_on_task_id = Column(Integer, ForeignKey("task.task_id"), primary_key=True)

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
    task_id = Column(Integer, ForeignKey("task.task_id"), nullable=True)
    status = Column(SQLEnum(PaymentStatus), default=PaymentStatus.pending)
    created_at = Column(DateTime, default=datetime.now)

    # Relationships
    node = relationship("Node", back_populates="payments")
    task = relationship("Task", back_populates="payments")

class NodeResources(Base):
    __tablename__ = "node_resources"
    
    node_id = Column(Integer, ForeignKey("node.node_id"), primary_key=True)
    rem_ram = Column(Integer)
    rem_cpu_cores = Column(Integer)

    # Relationships
    node = relationship("Node", back_populates="resources")