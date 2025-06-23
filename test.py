# main.py
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import os
import shutil
import uuid

app = FastAPI()

# Define directories for temporary chunks and final uploads
TEMP_CHUNKS_DIR = Path("temp_chunks")
UPLOADS_DIR = Path("uploads")

# Ensure directories exist
TEMP_CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

@app.post("/api/upload-chunk")
async def upload_chunk(
    fileId: str = Form(...),
    fileName: str = Form(...),
    chunkIndex: int = Form(...),
    totalChunks: int = Form(...),
    chunk: UploadFile = File(...)
):
    """
    Receives individual file chunks and stores them temporarily.
    """
    if not fileId or not fileName or chunkIndex is None or totalChunks is None:
        raise HTTPException(status_code=400, detail="Missing required chunk metadata.")

    # Create a sub-directory for this file's chunks
    file_chunks_dir = TEMP_CHUNKS_DIR / fileId
    file_chunks_dir.mkdir(parents=True, exist_ok=True)

    # Define the path for the current chunk
    chunk_path = file_chunks_dir / f"{chunkIndex}.part"

    try:
        # Write the chunk data to the temporary file
        with open(chunk_path, "wb") as buffer:
            # Use shutil.copyfileobj for efficient streaming of large chunks
            shutil.copyfileobj(chunk.file, buffer)

        # Store chunk metadata (optional, but good for robust resumable uploads)
        # For simplicity, we're relying on the presence of the chunk file.
        # In a real app, you might use a database/Redis to track uploaded chunks.

        return JSONResponse(
            status_code=200,
            content={
                "message": f"Chunk {chunkIndex}/{totalChunks} uploaded successfully.",
                "chunkIndex": chunkIndex,
                "fileId": fileId
            }
        )
    except Exception as e:
        print(f"Error saving chunk {chunkIndex} for file {fileId}: {e}")
        # Clean up the potentially incomplete chunk file if an error occurs during write
        if chunk_path.exists():
            os.remove(chunk_path)
        raise HTTPException(status_code=500, detail=f"Failed to save chunk {chunkIndex}.")


@app.post("/api/upload-complete")
async def upload_complete(
    fileId: str,
    fileName: str,
    totalChunks: int
):
    """
    Reassembles all chunks into a single file and cleans up temporary chunks.
    """
    if not fileId or not fileName or totalChunks is None:
        raise HTTPException(status_code=400, detail="Missing required finalization metadata.")

    file_chunks_dir = TEMP_CHUNKS_DIR / fileId
    final_file_path = UPLOADS_DIR / fileName # Be careful with filename collisions, use uuid if needed

    # Ensure all chunks are present before reassembly
    # In a real-world scenario, you might want to retry missing chunks or use a more robust check.
    expected_chunk_paths = [file_chunks_dir / f"{i}.part" for i in range(totalChunks)]
    missing_chunks = [p for p in expected_chunk_paths if not p.exists()]

    if missing_chunks:
        # Clean up partially uploaded chunks if reassembly cannot proceed
        if file_chunks_dir.exists():
            shutil.rmtree(file_chunks_dir)
        raise HTTPException(
            status_code=400,
            detail=f"Missing chunks for file {fileId}. Cannot finalize upload. Missing: {[p.name for p in missing_chunks]}"
        )

    try:
        with open(final_file_path, "wb") as outfile:
            for i in range(totalChunks):
                chunk_path = file_chunks_dir / f"{i}.part"
                with open(chunk_path, "rb") as infile:
                    # Efficiently copy chunk content to the final file
                    shutil.copyfileobj(infile, outfile)
                os.remove(chunk_path) # Delete chunk after it's appended

        # Clean up the temporary directory for this file's chunks
        shutil.rmtree(file_chunks_dir)

        # Construct a URL for the uploaded file (if served statically, e.g., via Nginx/CDN)
        # For simplicity, we just return the path relative to the uploads directory.
        public_url = f"/uploads/{fileName}" # Adjust if you serve from a different path

        return JSONResponse(
            status_code=200,
            content={
                "message": "File reassembled and saved successfully.",
                "filePath": public_url,
                "fileId": fileId
            }
        )
    except Exception as e:
        print(f"Error finalizing file {fileId}: {e}")
        # Clean up if reassembly fails
        if final_file_path.exists():
            os.remove(final_file_path)
        if file_chunks_dir.exists():
            shutil.rmtree(file_chunks_dir)
        raise HTTPException(status_code=500, detail="Failed to finalize file upload.")

# Optional: Endpoint to clean up old temp files (for development/testing)
@app.post("/api/clean-temp-chunks")
async def clean_temp_chunks():
    try:
        shutil.rmtree(TEMP_CHUNKS_DIR)
        TEMP_CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
        return JSONResponse(status_code=200, content={"message": "Temporary chunk directory cleaned."})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clean temporary chunks: {e}")

# Example of how to serve static files (e.g., your uploaded files)
# For production, it's better to use a dedicated web server like Nginx or a CDN.
from fastapi.staticfiles import StaticFiles
app.mount("/uploads", StaticFiles(directory=UPLOADS_DIR), name="uploads")