from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from app.db.session import init_db
from app.api import user, node, job, auth


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield

app = FastAPI(
    title="RunSurge Master API",
    description="API for managing distributed computing tasks across worker nodes",
    version="1.0.0",
    docs_url="/docs",  # Swagger UI endpoint
    redoc_url="/redoc",  # ReDoc endpoint
    openapi_url="/openapi.json",  # OpenAPI schema endpoint
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(user.router, prefix="/api/user", tags=["Users"])
app.include_router(node.router, prefix="/api/node", tags=["Nodes"])
app.include_router(job.router, prefix="/api/job", tags=["Jobs"])
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Welcome to the Job Management API"}






