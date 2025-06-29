
from protos import worker_pb2_grpc, worker_pb2
import logging
import grpc
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from protos import common_pb2
import aiofiles

class WorkerClient:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
      
    @asynccontextmanager
    async def _get_worker_stub(self, worker_address: str) -> AsyncGenerator[worker_pb2_grpc.WorkerServiceStub, None]:
        channel = None
        try:
            channel = grpc.aio.insecure_channel(worker_address)
            stub = worker_pb2_grpc.WorkerServiceStub(channel)
            yield stub
        except Exception as e:
            self.logger.error(f"Failed to create stub to worker: {e}")
            raise
        finally:
            if channel:
                try:
                    await channel.close()
                except Exception as e:
                    self.logger.warning(f"Error closing channel: {e}")

    async def notify_data(self, data_notification: worker_pb2.DataNotification, ip_address: str, port: int) -> bool:
        async with self._get_worker_stub(f"{ip_address}:{port}") as stub:
            try:
                await stub.NotifyData(data_notification)
                return True
            except Exception as e:
                self.logger.error(f"Failed to notify data: {e}")
                return False
            

    async def stream_data(self, data_identifier: common_pb2.DataIdentifier, ip_address: str, port: int, file_path: str) -> bool:
        async with self._get_worker_stub(f"{ip_address}:{port}") as stub:
            try:
                response_iterator = stub.StreamData(data_identifier)
                async with aiofiles.open(file_path, 'wb') as f:
                    async for chunk in response_iterator:
                        await f.write(chunk.chunk_data)
                self.logger.info(f"Data streamed to {file_path}")
                return True
            except Exception as e:
                self.logger.error(f"Failed to stream data: {e}")
                return False
            

    async def assign_task(self, task_assignment: worker_pb2.TaskAssignment, ip_address: str, port: int) -> bool:
        async with self._get_worker_stub(f"{ip_address}:{port}") as stub:
            try:
                await stub.AssignTask(task_assignment)
                return True
            except Exception as e:
                self.logger.error(f"Failed to assign task: {e}")
                return False
            