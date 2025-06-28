from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from app.db.session import init_db
from app.api import user, node, job, auth, group

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    ## Thread Cleanup
    ## Without it, the thread will not stop and the application will not reload correctly on reload
    print("Application shutdown initiated.")

app = FastAPI(
    title="RunSurge Master API",
    description="API for managing distributed computing tasks across worker nodes",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://10.10.10.249:3000",
        "http://10.10.10.218:3000",
        "http://10.10.10.219",
        "http://10.10.10.234:3000",
        "http://localhost:3000",
        "http://localhost"
    ],  # Allow access from specified origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(user.router, prefix="/api/users", tags=["Users"])
app.include_router(node.router, prefix="/api/node", tags=["Nodes"])
app.include_router(job.router, prefix="/api/job", tags=["Jobs"])
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(group.router, prefix="/api/group", tags=["Groups"])


@app.get("/", tags=["Root"])
def read_root(request: Request):
    request_host = request.client.host
    request_port = request.client.port
    print(f"Request from {request_host}:{request_port}")
    return {"message": "Welcome to the Job Management API"}
