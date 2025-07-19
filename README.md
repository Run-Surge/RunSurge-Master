# 🚀 RunSurge-Master

RunSurge is a distributed computing platform that manages and executes tasks across multiple worker nodes. This repository contains the core backend module of the platform.

---

## 🧱 Architecture

RunSurge follows a **Service-Oriented Architecture (SOA)** with the following key components:

- **Master Node**: Coordinates task scheduling, resource allocation, and communication between components.  
- **Worker Nodes**: Execute assigned tasks and process data chunks in parallel.  
- **gRPC Communication Layer**: Enables efficient communication between master and worker nodes.  
- **REST API**: Provides interfaces for user interaction and system management.

---

## ⚙️ Core Components

### 📅 1. Scheduler

The scheduler is responsible for:

- Analyzing job requirements and dependencies.  
- Allocating tasks to appropriate worker nodes based on memory constraints.  
- Creating execution plans to optimize resource usage.  
- Supporting both sequential and parallel execution strategies.  
- Managing task dependencies and data flow between nodes.

Key algorithms:

- Estimate peak memory usage of code blocks.  
- Merge compatible blocks when possible.  
- Split unschedulable blocks into parallel tasks.

---

### 🔀 2. Parallelizer

The parallelizer enables distributed execution by:

- Building Data Dependency Graphs (DDGs) to analyze block relationships.  
- Estimating memory footprints of functions and code segments.  
- Determining chunking strategies for parallelizable code.  
- Generating execution plans for concurrent task dispatch.

Features:

- Static analysis of Python code.  
- Memory usage estimation.  
- Dependency tracking.

---

### 🧩 3. Aggregator Service

This service combines results from distributed tasks:

- Monitors groups awaiting aggregation.  
- Executes predefined aggregator scripts to merge outputs.  
- Updates status upon completion or failure.  
- Runs independently from task execution to improve modularity.

Purpose:

- Consolidates partial results into a final output.  
- Finalizes jobs once all subtasks are completed.

---

### 🖥️ 4. Main Backend (FastAPI)

Provides the primary REST API for:

- User authentication and account management.  
- Job submission and status tracking.  
- Node registration and health monitoring.  
- Group and job orchestration.  
- System-wide statistics and logs.

Endpoints:

- `/api/users` – User management  
- `/api/node` – Node registration and status  
- `/api/job` – Job submission and monitoring  
- `/api/auth` – Authentication  
- `/api/group` – Job grouping  
- `/api/statistics` – System metrics

---

### 🔌 5. gRPC Application

Facilitates backend-node communication:

- **Master Service**: Manages registration, task assignments, and data exchange.  
- **Worker Client**: Sends and receives job data and updates.  
- **Data Transfer**: Efficiently transmits payloads between nodes.  
- **Heartbeat Mechanism**: Tracks worker node status and availability.

---

## 🛠️ Technologies Used

- **Python 3.11** – Core language  
- **FastAPI** – REST API framework  
- **gRPC** – Inter-service communication  
- **SQLAlchemy** – ORM for PostgreSQL  
- **PostgreSQL** – Relational database  
- **Pydantic** – Request/response validation  
- **AsyncIO** – Async I/O for concurrency  
- **semgrep** – Static code analysis for security  
- **Jinja2** – Script templating  
- **NetworkX** – Graph analysis (for DDGs)

---

## 🧰 Getting Started

### 📋 Requirements

- Python 3.11.9  
- PostgreSQL

### ⚙️ Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file:
   ```env
   # Security
   SECRET_KEY=your-secret-key-here
   ALGORITHM=HS256
   ACCESS_TOKEN_EXPIRE_MINUTES=60
   REFRESH_TOKEN_EXPIRE_DAYS=7

   # Database
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_HOST=localhost
   DB_NAME=runsurge
   DB_PORT=5432

   # Master
   MASTER_PORT=8000
   GRPC_PORT=50051
   ```

4. Start the main application:
   ```bash
   python main.py
   ```

5. Start the gRPC server:
   ```bash
   python main_grpc.py
   ```

6. Start the scheduler service:
   ```bash
   python scheduler.py
   ```

7. Start the aggregator service:
   ```bash
   python aggregator.py
   ```

---

## 📘 API Documentation

* Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 📂 Project Structure

```
RunSurge-Master/
├── app/                # Main application code
│   ├── api/            # API endpoints
│   ├── core/           # Core functionality
│   ├── db/             # Database models and repositories
│   ├── schemas/        # Pydantic models
│   ├── services/       # Business logic
│   ├── templates/      # Jinja template files
│   └── utils/          # Utility functions
├── gRPCApp/            # gRPC server implementation
├── Parallelization/    # Code analysis and parallelization
│   ├── DDG.py              # Data Dependency Graph builder
│   ├── Memory_Estimator.py # Memory usage estimation
│   └── Parallelizer.py     # Parallelization logic
├── protos_def/         # Protocol buffer definitions
├── scheduler.py        # Task scheduling logic
├── aggregator.py       # Result aggregation service
├── main.py             # FastAPI application entry point
├── main_grpc.py        # gRPC server entry point
└── requirements.txt    # Project dependencies
```
