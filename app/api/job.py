from fastapi import Depends, HTTPException, APIRouter, Form
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.schemas.job import JobRead, JobDetailRead
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.job import get_job_service
from fastapi import UploadFile, File
from app.core.security import get_current_user_from_cookie
from app.db.models.scheme import JobType, JobStatus
from app.utils.constants import JOBS_DIRECTORY_PATH
from Parallelization.Parallelizer import Parallelizer
from app.services.data import get_input_data_service
from app.utils.utils import append_chunk_to_file, validate_data_chunk
from app.services.data import get_data_service
from fastapi.responses import FileResponse
router = APIRouter()

@router.post("/", response_model=JobRead)
async def create_job(
    job_name: str = Form(...),
    job_type: JobType = Form(...),
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db), 
    current_user = Depends(get_current_user_from_cookie)
):
    job_service = get_job_service(session)
    job = await job_service.create_job_with_script(
        user_id=current_user["user_id"],
        file=file,
        job_name=job_name,
        job_type=job_type
    )
    return job

@router.get("/{job_id}", response_model=JobDetailRead)
async def get_job(
    job_id: int,
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    job_service = get_job_service(session)
    job = await job_service.get_job(job_id)
    if job.user_id != current_user["user_id"]:
        raise HTTPException(status_code=403, detail="Forbidden")
    input_data_service = get_input_data_service(session)
    input_data = await input_data_service.get_input_data(job_id)
    if input_data is None:
        input_file_name = ""
    else:
        input_file_name = input_data.file_name
    return JobDetailRead(
        job_id=job.job_id,
        status=job.status,
        created_at=job.created_at,
        job_name=job.job_name,
        job_type=job.job_type,
        script_name=job.script_name,
        input_file_name=input_file_name,
        payment_amount=job.payment.amount if job.payment else None
    )

# This endpoint is used to upload data- chunks related to a certain job
@router.post("/{job_id}/upload-data")
async def upload_data(
    job_id: int,
    file: UploadFile = File(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        job_service = get_job_service(session)
        await job_service.check_job_status(job_id)
        validate_data_chunk(file)
        input_data_service = get_input_data_service(session)
        data = None
        if( chunk_index == 0):
            data = await input_data_service.create_input_data(job_id, chunk_index, total_chunks,file.filename)
        else :
            data = await input_data_service.get_input_data(job_id)
            if(data.chunk_index != chunk_index-1):
                raise HTTPException(status_code=400, detail="Chunk index is wrong")
            data.chunk_index += 1
            await input_data_service.update_input_data(data)
        file_path = os.path.join(JOBS_DIRECTORY_PATH, str(job_id))
        append_chunk_to_file(input_file=file, file_path=file_path,file_name=str(data.input_data_id))
        if(chunk_index == total_chunks - 1):
            job= await job_service.get_job(data.job_id)
            print("Here")
            Parallelizer(job.script_path,job.job_id,data.input_data_id)
            await job_service.update_job_status(job.job_id,JobStatus.pending_schedule)    
            return {
                "message": "Data uploaded successfully and job scheduled",
                "success": True,
                "data_chunk_index": chunk_index,
                "job_id": job_id
            }
        else:
            return {
                "message": "Data chunk uploaded successfully",
                "success": True,
                "data_chunk_index": chunk_index,
                "job_id": job_id
            }
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


#-------------------------------------------------Complex Job-------------------------------------------------


@router.get("/{job_id}/result")
async def get_job_result(
    job_id: int,
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    job_service = get_job_service(session)
    job = await job_service.get_job(job_id)
    if job.user_id != current_user["user_id"]:
        raise HTTPException(status_code=403, detail="Forbidden")
    result_path = await job_service.get_output_file_path(job_id)
    if not os.path.exists(result_path):
        raise HTTPException(status_code=404, detail="Result file not found")
    return FileResponse(
        path=result_path,
        media_type="text/csv",
        filename=f"job_{job_id}_result.csv"
    )
    