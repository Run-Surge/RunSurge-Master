
import os
from fastapi import UploadFile
def Create_directory(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def save_file(file: UploadFile, path: str):
    with open(path, "wb") as f:
        f.write(file.file.read())