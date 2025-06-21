from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from app.db.session import init_db,AsyncSessionLocal
from app.api import user, node, job, auth
from threading import Thread, Event
from app.utils.worker import worker_main


global worker_thread, worker_stop_event
worker_thread: Thread = None
worker_stop_event: Event = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_thread, worker_stop_event

    worker_stop_event = Event()
    worker_thread = Thread(target=worker_main, args=(worker_stop_event,AsyncSessionLocal), daemon=False)
    worker_thread.start()
    print("Background worker thread started.")

    await init_db()
    yield
    ## Thread Cleanup
    ## Without it, the thread will not stop and the application will not reload correctly on reload
    print("Application shutdown initiated. Signaling worker to stop...")
    if worker_stop_event:
        worker_stop_event.set()

    if worker_thread and worker_thread.is_alive():
        print("Waiting for worker thread to finish (timeout=10s)...")
        worker_thread.join(timeout=10)
        if worker_thread.is_alive():
            print("WARNING: Worker thread did not stop within timeout. It might be force-killed.")
        else:
            print("Worker thread stopped.")

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
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(user.router, prefix="/api/users", tags=["Users"])
app.include_router(node.router, prefix="/api/node", tags=["Nodes"])
app.include_router(job.router, prefix="/api/job", tags=["Jobs"])
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Welcome to the Job Management API"}
