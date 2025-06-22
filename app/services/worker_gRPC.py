from app.db.models.scheme import NodeResources
from protos import worker_pb2_grpc, worker_pb2
import logging
import grpc
from contextlib import asynccontextmanager
from typing import AsyncGenerator

class WorkerGRPCService:
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