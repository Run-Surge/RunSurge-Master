import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
from dotenv import load_dotenv
from db.models.scheme import (
    Base, User, Node, NodeHeartbeat, Job, Data, DataLocation,
    Task, TaskDependency, NodeLog, Payment, NodeResources,
    JobStatus, TaskStatus, LogEventType, PaymentStatus
)
from passlib.context import CryptContext

load_dotenv()
DB_USER = os.getenv("DB_USER", "your_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "your_project_db")
DB_PORT = os.getenv("DB_PORT", "5432")


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# URL encode the password to handle special characters
encoded_password = quote_plus(DB_PASSWORD)
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)
session = Session()

def create_sample_data():
    # Create Users
    users = [
        User(
            username=f"user{i}",
            email=f"user{i}@example.com",
            password=pwd_context.hash("123456"),  # In real app, these would be properly hashed
        ) for i in range(1, 6)
    ]
    session.add_all(users)
    session.commit()    

    # Create Nodes
    nodes = [
        Node(
            node_name=f"node{i}",
            user_id=users[i-1].user_id,  # Each node belongs to a user
            ram=8192 * i,  # 8GB, 16GB, 24GB, 32GB, 40GB
            cpu_cores=i * 2,  # 2, 4, 6, 8, 10 cores
            ip_address=f"192.168.1.{i}",
            port=8000 + i
        ) for i in range(1, 6)
    ]
    session.add_all(nodes)
    session.commit()

    # Create Node Resources and Heartbeats
    for node in nodes:
        # Create Node Resources
        resources = NodeResources(
            node_id=node.node_id,
            rem_ram=node.ram,
            rem_cpu_cores=node.cpu_cores
        )
        session.add(resources)

        # Create Node Heartbeat
        heartbeat = NodeHeartbeat(
            node_id=node.node_id,
            last_ping=datetime.now()
        )
        session.add(heartbeat)
    session.commit()

    # Create Jobs
    jobs = []
    for user in users:
        for i in range(2):  # 2 jobs per user
            job = Job(
                user_id=user.user_id,
                status=JobStatus.completed if i % 2 == 0 else JobStatus.running
            )
            jobs.append(job)
    session.add_all(jobs)
    session.commit()

    # Create Data entries
    data_entries = []
    for job in jobs:
        data = Data(
            job_id=job.job_id,
            size_bytes=1024 * 1024 * (int(job.job_id.split('-')[0], 16) % 10 + 1)  # Random size between 1MB and 10MB
        )
        data_entries.append(data)
    session.add_all(data_entries)
    session.commit()

    # Create Data Locations
    for data in data_entries:
        # Assign each data to 2 random nodes
        for node in nodes[:2]:
            data_location = DataLocation(
                data_id=data.data_id,
                node_id=node.node_id
            )
            session.add(data_location)
    session.commit()

    # Create Tasks
    tasks = []
    for job in jobs:
        for i in range(3):  # 3 tasks per job
            task = Task(
                job_id=job.job_id,
                node_id=nodes[i % len(nodes)].node_id,
                data_id=data_entries[jobs.index(job)].data_id,
                status=TaskStatus.completed if i % 2 == 0 else TaskStatus.running,
                required_ram=2048 * (i + 1),  # 2GB, 4GB, 6GB
                started_at=datetime.now() - timedelta(hours=i),
                completed_at=datetime.now() - timedelta(hours=i-1) if i % 2 == 0 else None,
                retry_count=0
            )
            tasks.append(task)
    session.add_all(tasks)
    session.commit()

    # Create Task Dependencies
    for i in range(0, len(tasks)-1, 2):
        if i + 1 < len(tasks):
            dependency = TaskDependency(
                task_id=tasks[i+1].task_id,
                depends_on_task_id=tasks[i].task_id
            )
            session.add(dependency)
    session.commit()

    # Create Node Logs
    for node in nodes:
        for task in tasks[:2]:  # 2 logs per node
            for event_type in [LogEventType.started, LogEventType.completed]:
                log = NodeLog(
                    node_id=node.node_id,
                    task_id=task.task_id,
                    event_type=event_type,
                    timestamp=datetime.now() - timedelta(hours=1)
                )
                session.add(log)
    session.commit()

    # Create Payments
    for node in nodes:
        for task in tasks[:2]:  # 2 payments per node
            payment = Payment(
                node_id=node.node_id,
                amount=10.0 * (task.required_ram / 1024),  # $10 per GB of RAM
                task_id=task.task_id,
                status=PaymentStatus.completed if task.status == TaskStatus.completed else PaymentStatus.pending
            )
            session.add(payment)
    session.commit()

if __name__ == "__main__":
    # Create all tables
    # Base.metadata.create_all(engine)
    create_sample_data()

