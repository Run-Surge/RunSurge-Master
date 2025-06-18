
import os
from fastapi import UploadFile
from app.db.models.scheme import Node

def Create_directory(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def save_file(file: UploadFile, path: str):
    with open(path, "wb") as f:
        f.write(file.file.read())

def convert_nodes_into_Json(data: list[Node]):
    ## convert the node.name and node.ram into a json object
    return [{"name": node.node_name, "memory": node.ram} for node in data]
