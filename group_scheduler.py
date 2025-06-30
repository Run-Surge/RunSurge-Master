import json
import argparse
import ast
import re
import math
import csv
import os
import sys
import traceback
sys.path.append(os.path.join(os.path.dirname(__file__), 'protos'))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.utils.constants import JOBS_DIRECTORY_PATH
from app.utils.utils import convert_nodes_into_Json
from app.services.task import get_task_service
from app.services.data import get_data_service
from app.services.node import get_node_service
from app.services.job import get_job_service
from app.services.data import get_input_data_service
from app.db.session import  get_db_context, init_db
from app.db.models.scheme import JobStatus
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.worker_client import WorkerClient
from protos import worker_pb2
from app.core.config import Settings
import traceback
from sqlalchemy.types import BigInteger
from app.utils.constants import GROUPS_DIRECTORY_PATH
settings = Settings()
input_file = None

# input to single task job scheduler
# job_id, input_data_id, input_filename, peak_memory, session

async def read_python_file_bytes(file_path: str) -> bytes:
    if not os.path.exists(file_path):
        print(f"  -> FATAL ERROR: File {file_path} does not exist.")
        return ""
    
    with open(file_path, 'rb') as f:
        return f.read()

async def single_task_job_scheduler(
    group_id: int,
    job_id: int,
    input_file_name: str,
    input_file_size: BigInteger,
    peak_memory: BigInteger,
    session: AsyncSession,
):
    """
    A simplified scheduler that correctly handles detached ORM objects by extracting
    necessary data into local variables immediately.
    """
    # print("from new scheduler")
    # print(f"group_id: {group_id}, job_id: {job_id}, input_data_id: {input_data_id}, input_file_size: {input_file_size}, peak_memory: {peak_memory}, input_filename: {input_filename}")
    print(f"\n--- Running Simple Scheduler for Job ID: {job_id} ---")
    job_dir = os.path.join(JOBS_DIRECTORY_PATH, str(job_id))
    os.makedirs(job_dir, exist_ok=True)

    # --- 1. Get Node Resources ---
    node_service = get_node_service(session)
    task_service = get_task_service(session)
    nodes = await node_service.get_all_nodes()

    nodes_data = [{"name": node.node_name, "memory": node.ram - await task_service.get_total_node_ram(node.node_id) - 500000000, "ip_address": node.ip_address, "port": node.port} for node in nodes]
    node_map = {node.node_name: node.node_id for node in nodes}
    print(nodes)

    if not nodes:
        print(f"  -> FATAL ERROR: No worker nodes are available. Cannot schedule job {job_id}.")
        job_service = get_job_service(session)
        # await job_service.update_job_status(job_id, JobStatus.pending_schedule)
        return

    node_address_map = {node["name"]: (node["ip_address"], node["port"]) for node in nodes_data}
    # print(f"  -> Available nodes: {nodes_data}")

    # --- 2. Find a Fitting Node ---
    sorted_nodes = sorted(nodes_data, key=lambda x: x["memory"])
    # print(f"  -> Sorted nodes by memory: {sorted_nodes}")
    fitting_node = next(
        (node for node in sorted_nodes if node["memory"] >= peak_memory), None
    )

    if not fitting_node:
        print(f"  -> FATAL ERROR: No node has enough memory ({peak_memory} required). Cannot schedule job {job_id}.")
        job_service = get_job_service(session)
        # await job_service.update_job_status(job_id, JobStatus.pending_schedule)
        return
    
    # Extract node info immediately
    node_name = fitting_node['name']
    assigned_node_id = node_map.get(node_name)
    ip_address, port = node_address_map.get(node_name)
    print(f"  -> Found fitting node: '{node_name}' at {ip_address}:{port}")

    # --- 3. Create Task and Output Data Records in DB ---
    data_service = get_data_service(session)
    input_data_obj = await data_service.create_data(file_name=input_file_name, parent_task_id=None)
    input_data_id = input_data_obj.data_id
    created_task_obj = await task_service.create_task(
        job_id=job_id, data_ids=[input_data_id],
        required_ram=peak_memory, node_id=assigned_node_id
    )
    # --- ### CORRECTION ###: Extract the ID immediately ---
    created_task_id = created_task_obj.task_id
    print(f"  -> Created Task with ID: {created_task_id}")

    output_filename = f"output_{job_id}.zip"
    output_data_obj = await data_service.create_data(
        file_name=output_filename,
        parent_task_id=created_task_id
    )
    # --- ### CORRECTION ###: Extract the ID immediately ---
    output_data_id = output_data_obj.data_id
    job_service = get_job_service(session)
    await job_service.update_job_output_data_id(job_id, output_data_id)
    print(f"  -> Created Output Data record with ID: {output_data_id}")
    
#     # --- 4. Prepare for Task Assignment ---
#     task_script_name = f"task_{created_task_id}_simple_job.py"
    script_path = os.path.join(GROUPS_DIRECTORY_PATH, str(group_id), str(group_id) + ".py")
    print(f"  -> Script path: {script_path}")
    script_content_bytes = await read_python_file_bytes(script_path)
    
    output_data_infos = [
        worker_pb2.OutputDataInfo(
            data_id=output_data_id,
            data_name='output.zip'
        )
    ]

    task_assignment_message = worker_pb2.TaskAssignment(
        task_id=created_task_id, python_file=script_content_bytes,
        python_file_name=os.path.basename(script_path), required_data_ids=[input_data_id],
        output_data_infos=output_data_infos, job_id=job_id
    )

    # --- 5. Assign Task and Notify about Initial Data ---
    print(f"\n--- Assigning Simple Task {created_task_id} to {node_name} at {ip_address}:{port} ---")
    worker_client = WorkerClient()
    success = await worker_client.assign_task(
        task_assignment=task_assignment_message, ip_address=ip_address, port=port
    )

    if success:
        print(f"  -> Successfully assigned Task {created_task_id}.")
        print(f"    -> Notifying {node_name} that initial data '{group_id}' is ready.")
        notification = worker_pb2.DataNotification(
            task_id=created_task_id, data_id=input_data_id,
            data_name=input_file_name, ip_address=settings.GRPC_IP,
            port=settings.GRPC_PORT, hash="", is_zipped=True
        )
        notify_success = await worker_client.notify_data(notification, ip_address, port)
        if notify_success:
            print(f"  -> Successfully notified {node_name} about initial data '{group_id}'.")
        else:
            print(f"  -> FAILED to notify {node_name} about initial data '{group_id}'.")
        # Update job status to running only after successful assignment and notification4
        job_service = get_job_service(session)
        await job_service.update_job_status(job_id, JobStatus.running)
    else:
        print(f"  -> FAILED to assign Task {created_task_id}.")

    print(f"--- Simple Scheduling for Job {job_id} Complete. ---")