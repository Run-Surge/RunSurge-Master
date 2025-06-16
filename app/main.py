from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from app.db.session import init_db
from app.api import user, node, job, auth


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(user.router, prefix="/api/user")
app.include_router(node.router, prefix="/api/node")
app.include_router(job.router, prefix="/api/job")
app.include_router(auth.router, prefix="/api/auth")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Job Management API"}






