# models/schemas.py
from sqlmodel import SQLModel
from typing import Optional
from datetime import datetime
import uuid

# -----------------------------
# User Schemas
# -----------------------------
class UserCreate(SQLModel):
    username: str
    email: str
    password: str

class UserRead(SQLModel):
    user_id: uuid.UUID
    username: str
    email: str
    created_at: datetime

# -----------------------------
# Node Schemas
# -----------------------------
class NodeCreate(SQLModel):
    username: str
    ram: int
    cpu_cores: int
    ip_address: str
    port: int

class NodeRead(SQLModel):
    node_id: uuid.UUID
    username: str
    ram: int
    cpu_cores: int
    ip_address: str
    port: int
    created_at: datetime
# -----------------------------
# Job Schemas
# -----------------------------
class JobCreate(SQLModel):
    user_id: uuid.UUID

class JobRead(SQLModel):
    job_id: uuid.UUID
    user_id: uuid.UUID
    status: str
    created_at: datetime
