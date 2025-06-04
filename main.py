from fastapi import FastAPI, Depends, HTTPException
from db.session import get_session, init_db
from db.repositories.factory import RepositoryFactory, get_repositories
from db.models.scheme import User, Node, Job
from db.models.request_response import UserCreate, UserRead, NodeCreate, NodeRead, JobCreate, JobRead
import uuid
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Job Management API"}

@app.post("/users/", response_model=UserRead)
def create_user(user: UserCreate, repos: RepositoryFactory = Depends(get_repositories)):
    if repos.user.username_or_email_exists(user.username, user.email):
        raise HTTPException(status_code=400, detail="Username or email already exists.")
    created = repos.user.create_user(user.username, user.email, user.password)
    return created

@app.post("/nodes/", response_model=NodeRead)
def register_node(node: NodeCreate, repos: RepositoryFactory = Depends(get_repositories)):
    return repos.node.create_node(node.username, node.ram, node.cpu_cores, node.ip_address, node.port)

@app.post("/jobs/", response_model=JobRead)
def create_job(job: JobCreate, repos: RepositoryFactory = Depends(get_repositories)):
    user = repos.user.get_by_id(job.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    return repos.job.create_job(job.user_id)
