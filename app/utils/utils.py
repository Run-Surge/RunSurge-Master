
import os
from fastapi import UploadFile
from app.db.models.scheme import Node, Data 

def Create_directory(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def save_file(file: UploadFile, path: str):
    with open(path, "wb") as f:
        f.write(file.file.read())

def convert_nodes_into_Json(data: list[Node]):
    nodes_list = [{"name": node.node_name, "memory": node.ram} for node in data]
    node_map = {node.node_name: node.node_id for node in data}
    return nodes_list, node_map


def get_data_path(data: Data):
    return os.path.join('Jobs', str(data.job_id), data.file_name)

def format_bytes(bytes: int) -> str:
    if bytes < 1024:
        return f"{bytes} B"
    elif bytes < 1024 * 1024:
        return f"{bytes / 1024} KB"
    elif bytes < 1024 * 1024 * 1024:
        return f"{bytes / (1024 * 1024)} MB"
    else:
        return f"{bytes / (1024 * 1024 * 1024)} GB"