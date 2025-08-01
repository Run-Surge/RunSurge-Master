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
    submitted = 'submitted'
    pending_schedule = 'pending_schedule'
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

class EarningStatus(str, Enum):
    pending = 'pending'
    paid = 'paid'

class PaymentStatus(str, Enum):
    pending = 'pending'
    completed = 'completed'

class JobType(str, Enum):
    simple = 'simple'
    complex = 'complex'     

## Group Status
## submitted: not all jobs data files are uploaded
## running: all the jobs data files are uploaded and at least one job is running
## completed: all Jobs are completed and the aggregator is executed
## failed: at least one Job is failed

class GroupStatus(str, Enum):
    submitted = 'submitted'
    running = 'running'
    pending_aggregation = 'pending_aggregation'
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
    groups = relationship("Group", back_populates="user")
    payments = relationship("Payment", back_populates="user")
    earnings = relationship("Earning", back_populates="user")

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
    payment_factor = Column(Float, default=1.0)
    last_heartbeat = Column(DateTime, default=datetime.now)

    # Relationships
    tasks = relationship("Task", back_populates="node")
    logs = relationship("NodeLog", back_populates="node")
    earnings = relationship("Earning", back_populates="node")
    user = relationship("User", back_populates="nodes")

    __table_args__ = (
        UniqueConstraint("machine_fingerprint", "user_id", name="uq_machine_fingerprint_user_id"), # Ensure user doesn't have duplicate node names
    )

# This class for the Jobs of the type complex, 1 group can have multiple jobs
class Group(Base):
    __tablename__ = "group"
    
    group_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("user.user_id"))


    group_name = Column(String)
    python_file_name = Column(String)
    aggregator_file_name = Column(String)
    aggregator_file_path = Column(String)
    num_of_jobs = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    status = Column(SQLEnum(GroupStatus), default=GroupStatus.submitted)
    output_data_id = Column(Integer, ForeignKey("data.data_id"), nullable=True)
    ## 3apqr should update this
    payment_status = Column(SQLEnum(PaymentStatus), default=PaymentStatus.pending)
    payment_amount = Column(Float, default=0)
    payment_date = Column(DateTime, nullable=True)

    # Relationships
    jobs = relationship("Job", back_populates="group")
    user = relationship("User", back_populates="groups")
    output_data_file = relationship("Data", back_populates="parent_group", uselist=False)

class Job(Base):
    __tablename__ = "job"
    
    job_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("user.user_id"))
    job_name = Column(String)
    job_type = Column(SQLEnum(JobType))
    status = Column(SQLEnum(JobStatus), default=JobStatus.submitted)
    created_at = Column(DateTime, default=datetime.now)
    script_name = Column(String, nullable=True)
    script_path = Column(String, nullable=True)
    output_data_id = Column(Integer, ForeignKey("data.data_id"), nullable=True)
    #### FOR Complex Job ####
    group_id = Column(Integer, ForeignKey("group.group_id"), nullable=True)

    ### for simple & complex jobs ####
    required_ram = Column(BIGINT, nullable=True)
    num_of_tasks = Column(Integer, nullable=True)

    # Relationships
    user = relationship("User", back_populates="jobs")
    tasks = relationship("Task", back_populates="job")
    input_data_files = relationship("InputData", back_populates="job") # All input data files related to this job
    output_data_file = relationship("Data", back_populates="parent_job", uselist=False) # The output data file related to this job
    group = relationship("Group", back_populates="jobs")
    payment = relationship("Payment", back_populates="job",uselist=False)

class InputData(Base):
    __tablename__ = "input_data"
    input_data_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("job.job_id"))
    chunk_index = Column(Integer)
    total_chunks = Column(Integer)
    file_name = Column(String)
    created_at = Column(DateTime, default=datetime.now)

    # Relationships
    job = relationship("Job", back_populates="input_data_files")
    
class Data(Base):
    __tablename__ = "data"
    
    data_id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String)
    parent_task_id = Column(Integer, ForeignKey("task.task_id"), nullable=True)
    data_location = Column(SQLEnum(DataLocationType), default=DataLocationType.master)
    created_at = Column(DateTime, default=datetime.now)
    status = Column(SQLEnum(DataStatus), default=DataStatus.pending)    
    
    # Relationships
    dependent_tasks = relationship("Task", secondary="task_data_dependency", back_populates="data_dependencies")
    parent_task = relationship("Task", back_populates="data_files")  # The task this data file is its output
    parent_job = relationship("Job", back_populates="output_data_file")  # The job this data file is its output
    parent_group = relationship("Group", back_populates="output_data_file")  # The group this data file is its output


    

class Task(Base):
    __tablename__ = "task"
    task_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("job.job_id"))
    node_id = Column(Integer, ForeignKey("node.node_id"))
    required_ram = Column(BIGINT)
    # This should be the time to be displayed to the user of its earning
    status = Column(SQLEnum(TaskStatus), default=TaskStatus.pending)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    total_active_time = Column(Float, nullable=True)
    avg_memory_bytes = Column(BIGINT, nullable=True)
    retry_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now)
    

    # Relationships
    job = relationship("Job", back_populates="tasks")
    node = relationship("Node", back_populates="tasks")
    data_files = relationship("Data", back_populates="parent_task")
    data_dependencies = relationship("Data", secondary="task_data_dependency", back_populates="dependent_tasks")
    earning = relationship("Earning", back_populates="task", uselist=False)


class TaskDataDependency(Base):
    __tablename__ = "task_data_dependency"
    task_id = Column(Integer, ForeignKey("task.task_id"), primary_key=True)
    data_id = Column(Integer, ForeignKey("data.data_id"), primary_key=True)

class NodeLog(Base):
    __tablename__ = "node_log"
    
    log_id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(Integer, ForeignKey("node.node_id"))
    timestamp = Column(DateTime, default=datetime.now)
    number_of_tasks = Column(Integer)
    memory_usage_bytes = Column(BIGINT)

    # Relationships
    node = relationship("Node", back_populates="logs")

class Payment(Base):
    __tablename__ = "payment"
    payment_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("user.user_id"))
    job_id = Column(Integer, ForeignKey("job.job_id"))
    amount = Column(Float)
    status = Column(SQLEnum(PaymentStatus), default=PaymentStatus.pending)
    created_at = Column(DateTime, default=datetime.now)
    payment_date = Column(DateTime, nullable=True)

    # Relationships
    user = relationship("User", back_populates="payments")
    job = relationship("Job", back_populates="payment")

class Earning(Base):
    __tablename__ = "earning"
    earning_id = Column(Integer, primary_key=True, autoincrement=True)    
    node_id = Column(Integer, ForeignKey("node.node_id"))
    user_id = Column(Integer, ForeignKey("user.user_id"))
    amount = Column(Float)
    task_id = Column(Integer, ForeignKey("task.task_id"), unique=True)
    status = Column(SQLEnum(EarningStatus), default=EarningStatus.pending)
    created_at = Column(DateTime, default=datetime.now)
    earning_date = Column(DateTime, nullable=True)
    # Relationships
    node = relationship("Node", back_populates="earnings")
    task = relationship("Task", back_populates="earning")
    user = relationship("User", back_populates="earnings")