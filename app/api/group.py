from fastapi import APIRouter, Depends, Form, UploadFile, File, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.core.security import get_current_user_from_cookie
from app.schemas.group import GroupRead, GroupDetailRead
from app.services.group import get_group_service
from app.services.job import get_job_service
from app.db.models.scheme import JobType
from typing import List
import traceback
from app.utils.utils import append_chunk_to_zip_file, validate_zip_file_chunk
from app.services.data import get_input_data_service
from app.db.models.scheme import JobStatus, GroupStatus, PaymentStatus
import os
from app.utils.constants import GROUPS_DIRECTORY_PATH
from fastapi.responses import FileResponse
from app.utils.utils import create_aggregator_file, save_file_from_str

from app.services.payment import get_payment_service
from app.services.task import get_task_service
from app.db.models.scheme import EarningStatus
from datetime import datetime
router = APIRouter()

@router.post("/",response_model=GroupRead)
async def create_group(
    group_name: str = Form(...),
    num_of_jobs: int = Form(...),
    aggregator_file: UploadFile = File(...),
    python_file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        group_service = get_group_service(session)
        group, python_path, aggregator_path, before_code, loop_code, after_code = await group_service.create_group(
            group_name=group_name,
            python_file=python_file,
            num_of_jobs=num_of_jobs,
            user_id=current_user["user_id"],
            aggregator_file=aggregator_file
            )
        job_service = get_job_service(session)
        group_name = group.group_name
        group_id = group.group_id
        for i in range(num_of_jobs):
            await job_service.create_complex_job(
                user_id=current_user["user_id"],
                job_name=f"{group_name}_job_{i+1}",
                script_path=python_path,
                job_type=JobType.complex,
                group_id=group_id,
            )
        # Refresh group to include the newly created jobs
        group = await group_service.get_group_by_id(group_id)
        output_zips = [f'{GROUPS_DIRECTORY_PATH}/{group_id}/output_{job.job_id}.zip' for job in group.jobs]
        aggregator_file = create_aggregator_file(group.group_id, before_code, loop_code, after_code, output_zips)
        save_file_from_str(aggregator_file, aggregator_path)
        return group
    except HTTPException as e:
        raise e
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/{group_id}", response_model=GroupDetailRead)
async def get_group_by_id(
    group_id: int,
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        group_service = get_group_service(session)
        group = await group_service.get_group_by_id(group_id=group_id)
        payment_service = get_payment_service(session)
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")
        completed=True
        print("Here")
        for job in group.jobs:
            completed = completed and job.status == JobStatus.completed
            input_data = await get_input_data_service(session).get_input_data(job.job_id)
            if input_data:
                job.input_file_name = input_data.file_name
            else:
                job.input_file_name = "No input file"
        if completed:
            for job in group.jobs:
                payment = await payment_service.get_payment_by_job_id(job.job_id)
                if payment:
                    job.payment_amount = payment.amount
        return group
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



#-------------------------------------------------Upload Complex Job Data-------------------------------------------------



@router.post("/{group_id}/{job_id}/upload-zip-file")
async def upload_zip_file(
    group_id: int,
    job_id: int,    
    file: UploadFile = File(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    required_ram: int = Form(...),
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        job_service = get_job_service(session)
        await job_service.check_job_status(job_id)
        validate_zip_file_chunk(file)
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
        file_path = os.path.join(GROUPS_DIRECTORY_PATH,str(group_id))
        append_chunk_to_zip_file(input_file=file, file_path=file_path,file_name=str(data.input_data_id))
        if(chunk_index == total_chunks - 1):
            job= await job_service.get_job(data.job_id)
            job.required_ram = required_ram
            job.status = JobStatus.pending_schedule
            await job_service.update_complex_job_ram_and_status(job.job_id,job)
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
    

@router.get("/{group_id}/result")
async def get_group_result(
    group_id: int,
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    group_service = get_group_service(session)
    group = await group_service.get_group_by_id(group_id=group_id)
    if group.user_id != current_user["user_id"]:
        raise HTTPException(status_code=403, detail="Forbidden")
    if group.status != GroupStatus.completed:
        raise HTTPException(status_code=400, detail="Group is not completed yet")
    file_path = os.path.join(GROUPS_DIRECTORY_PATH, str(group_id), f"group_output.zip")
    return FileResponse(
        path=file_path,
        filename=f"group_output.zip",
        media_type="application/zip"
    )


#-------------------------------------------------Pay Group-------------------------------------------------

@router.post("/{group_id}/payment")
async def pay_group(
    group_id: int,
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        group_service = get_group_service(session)
        group = await group_service.get_group_by_id(group_id)
        if group.user_id != current_user["user_id"]:
            raise HTTPException(status_code=403, detail="Forbidden")
        if group.payment_status == PaymentStatus.completed:
            raise HTTPException(status_code=400, detail="Payment already completed")
        await group_service.pay_group(group_id)
        return {
            "message": "Payment completed successfully",
            "success": True
        }
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500,detail="Payment failed")