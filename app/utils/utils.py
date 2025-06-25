
import os
from fastapi import UploadFile
from app.db.models.scheme import Node, Data 
from fastapi import HTTPException
from app.utils.constants import FILE_SIZE_LIMIT, DATA_CHUNK_SIZE_LIMIT, ZIP_FILE_CHUNK_SIZE_LIMIT

def Create_directory(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def save_file(file: UploadFile, path: str):
    with open(path, "wb") as f:
        f.write(file.file.read())
def append_chunk_to_file(input_file: UploadFile, file_path: str, file_name: str):
    with open(os.path.join(file_path, f"{file_name}.csv"), "ab") as f:
        f.write(input_file.file.read())

def append_chunk_to_zip_file(input_file: UploadFile, file_path: str, file_name: str):
    with open(os.path.join(file_path, f"{file_name}.zip"), "ab") as f:
        f.write(input_file.file.read())


def validate_file(file: UploadFile):
    if not file.filename.endswith('.py'):
        raise HTTPException(status_code=400, detail="Only Python files (.py) are allowed")
    if file.size == 0:
        raise HTTPException(status_code=400, detail="File is empty")
    if file.size > FILE_SIZE_LIMIT:
        raise HTTPException(status_code=400, detail="File size exceeds 10MB limit") 
    
def validate_data_chunk(file: UploadFile):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files (.csv) are allowed")
    if file.size == 0:
        raise HTTPException(status_code=400, detail="File is empty")
    if file.size > DATA_CHUNK_SIZE_LIMIT:
        raise HTTPException(status_code=400, detail="File size exceeds 10MB limit") 

def validate_zip_file_chunk(file: UploadFile):
    if file.size == 0:
        raise HTTPException(status_code=400, detail="File is empty")
    if file.size > ZIP_FILE_CHUNK_SIZE_LIMIT:
        raise HTTPException(status_code=400, detail="File size exceeds 10MB limit") 

def convert_nodes_into_Json(data: list[Node]):
    print(f'number of nodes {len(data)}')
    nodes_list = [{"name": node.node_name, "memory": node.rem_ram, "ip_address": node.ip_address, "port": node.port} for node in data]
    node_map = {node.node_name: node.node_id for node in data}
    return nodes_list, node_map


def get_data_path(file_name: str, job_id: int):
    return os.path.join('Jobs', str(job_id), file_name)

def format_bytes(bytes: int) -> str:
    if bytes < 1024:
        return f"{bytes} B"
    elif bytes < 1024 * 1024:
        return f"{bytes / 1024} KB"
    elif bytes < 1024 * 1024 * 1024:
        return f"{bytes / (1024 * 1024)} MB"
    else:
        return f"{bytes / (1024 * 1024 * 1024)} GB"