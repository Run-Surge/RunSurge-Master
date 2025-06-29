
import os
from fastapi import UploadFile
from app.db.models.scheme import Node, Job, JobType
from fastapi import HTTPException
from app.utils.constants import FILE_SIZE_LIMIT, DATA_CHUNK_SIZE_LIMIT, ZIP_FILE_CHUNK_SIZE_LIMIT,GROUPS_DIRECTORY_PATH,JOBS_DIRECTORY_PATH,TEMP_DIRECTORY_PATH
import shutil
import re
import jinja2

AGGREGATION_REGEX = r'####Code Before For Loop####\s+(.*?)\s+##########Loop Code##########\s+(.*?)\s+##########Code After For Loop##########\s+(.*?)$'
   
import uuid
from app.utils.vulnerability_scanner import run_semgrep

def Create_directory(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def save_file(file: UploadFile, path: str):
    with open(path, "wb") as f:
        f.write(file.file.read())

def save_file_from_str(file_content: str, path: str):
    with open(path, "w") as f:
        f.write(file_content)

def append_chunk_to_file(input_file: UploadFile, file_path: str, file_name: str):
    with open(os.path.join(file_path, f"{file_name}.csv"), "ab") as f:
        f.write(input_file.file.read())

def append_chunk_to_zip_file(input_file: UploadFile, file_path: str, file_name: str):
    with open(os.path.join(file_path, f"{file_name}.zip"), "ab") as f:
        f.write(input_file.file.read())


def  validate_file(file: UploadFile):
    if not file.filename.endswith('.py'):
        raise HTTPException(status_code=400, detail="Only Python files (.py) are allowed")
    if file.size == 0:
        raise HTTPException(status_code=400, detail="File is empty")
    if file.size > FILE_SIZE_LIMIT:
        raise HTTPException(status_code=400, detail="File size exceeds 10MB limit") 
    random_name = str(uuid.uuid4())
    PATH = f"{TEMP_DIRECTORY_PATH}/{random_name}.py"
    os.makedirs(TEMP_DIRECTORY_PATH, exist_ok=True)
    save_file(file, f"{TEMP_DIRECTORY_PATH}/{random_name}.py")
    if(run_semgrep(f"{TEMP_DIRECTORY_PATH}/{random_name}.py", "app/utils/dangerous.yaml")):
        os.remove(f"{TEMP_DIRECTORY_PATH}/{random_name}.py")
        raise HTTPException(status_code=400, detail="Vulnerable script detected")
    os.remove(f"{TEMP_DIRECTORY_PATH}/{random_name}.py")
    file.file.seek(0)
    
def validate_aggregator_file(file: UploadFile):
    match = re.match(AGGREGATION_REGEX, file.file.read().decode('utf-8'), re.DOTALL)
    if not match:
        raise HTTPException(status_code=400, detail="Invalid aggregator file")
    
    return match.groups()
    
def create_aggregator_file(group_id: int,before_code: str, loop_code: str, after_code: str, zip_pathes: list[str]) -> str:
    template = jinja2.Template(open('app/templates/aggregation.py.j2', 'r').read())
    
    # Clean up the sections
    before_code = before_code.strip()
    loop_code = loop_code.strip()
    after_code = after_code.strip()
    
    # Format the loop code with proper indentation for the template
    formatted_loop_code = '\n'.join('            ' + line for line in loop_code.splitlines())
    
    return template.render(
        code_before_for_loop=before_code,
        code_after_for_loop=after_code, 
        zip_pathes=zip_pathes, 
        code=formatted_loop_code,
        output_zip_path=f'{GROUPS_DIRECTORY_PATH}/{group_id}/group_output.zip'
    )

    
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
    nodes_list = [{"name": node.node_name, "memory": node.ram, "ip_address": node.ip_address, "port": node.port} for node in data]
    node_map = {node.node_name: node.node_id for node in data}
    return nodes_list, node_map


def get_data_path(file_name: str, job: Job):
    if job.job_type == JobType.complex:
        return os.path.join(GROUPS_DIRECTORY_PATH, str(job.group_id), file_name)
    else:
        return os.path.join(JOBS_DIRECTORY_PATH, str(job.job_id), file_name)

def format_bytes(bytes: int) -> str:
    if bytes < 1024:
        return f"{bytes} B"
    elif bytes < 1024 * 1024:
        return f"{bytes / 1024} KB"
    elif bytes < 1024 * 1024 * 1024:
        return f"{bytes / (1024 * 1024)} MB"
    else:
        return f"{bytes / (1024 * 1024 * 1024)} GB"
    
def get_file_size(path: str) -> int:
    return os.path.getsize(path)

def clean_up_files():
    if os.path.exists(GROUPS_DIRECTORY_PATH):
        shutil.rmtree(GROUPS_DIRECTORY_PATH)
    if os.path.exists(JOBS_DIRECTORY_PATH):
        shutil.rmtree(JOBS_DIRECTORY_PATH)