"""gRPC servicer implementation for the MasterService."""

import grpc
import traceback
import os
import time
from protos import master_pb2
from protos import master_pb2_grpc
from protos import common_pb2
from app.core.logging import setup_logging
from app.services.user import get_user_service
from app.db.session import get_db_context
from app.schemas.node import  NodeRegisterGRPC, NodeUpdateGRPC    
from app.schemas.user import UserLoginCreate
from app.services.node import get_node_service
from gRPCApp.utils import parse_grpc_peer_address
from app.services.task import get_task_service
from app.services.data import get_data_service
from app.utils.utils import format_bytes

class MasterServicer(master_pb2_grpc.MasterServiceServicer):
    """gRPC servicer implementation for MasterService."""
    
    def __init__(self):
        self.logger = setup_logging("MasterServicer")
        self.logger.info("MasterServicer initialized")

    async def NodeRegister(self, request: master_pb2.NodeRegisterRequest, context: grpc.ServicerContext):
        self.logger.info(f"NodeRegister request: {request}")
        try:
            async with get_db_context() as session:
            
                user_service = get_user_service(session)
                user = await user_service.login_user(UserLoginCreate(
                                                     username_or_email=request.username
                                                    ,password=request.password
                                                      ))

                node_service = get_node_service(session)
                ip, port = parse_grpc_peer_address(context.peer())
                node = await node_service.register_node(NodeRegisterGRPC(
                    user_id=user.user_id,
                    machine_fingerprint=request.machine_fingerprint,
                    memory_bytes=request.memory_bytes,
                    ip_address=ip,
                    port=port
                ))

                self.logger.info(f"Node registered successfully: {node}")
    
            return master_pb2.NodeRegisterResponse(node_id=node.node_id, success=True, message="Node registered successfully")
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Error in NodeRegister: {e}")
            return common_pb2.StatusResponse(success=False, message=str(e))
    
    async def TaskComplete(self, request: master_pb2.TaskCompleteRequest, context: grpc.ServicerContext):
        self.logger.info(f"TaskComplete request: {request}")
        #TODO: add authorization
        try:
            async with get_db_context() as session:
                task_service = get_task_service(session)
                await task_service.complete_task(request)
                
        except Exception as e:
            self.logger.error(f"Error in TaskComplete: {e}")
            return common_pb2.StatusResponse(success=False, message=str(e))
        return common_pb2.StatusResponse(success=True, message="Task completed successfully")

    async def NodeUnregister(self, request: master_pb2.NodeUnregisterRequest, context: grpc.ServicerContext):
        self.logger.info(f"NodeUnregister request: {request}")
        try:
            async with get_db_context() as session:
                node_service = get_node_service(session)
                await node_service.update_node_status(NodeUpdateGRPC(
                    node_id=request.node_id,
                    is_alive=False
                ))
                return common_pb2.StatusResponse(success=True, message="Node unregistered successfully")
        except Exception as e:
            self.logger.error(f"Error in NodeUnregister: {e}")
            return common_pb2.StatusResponse(success=False, message=str(e))


    async def StreamData(self, request: common_pb2.DataIdentifier, context: grpc.ServicerContext):
        self.logger.info(f"StreamData request: {request}")
        try:
            data_id = request.data_id
            self.logger.debug(f"StreamData called for data {data_id}")
            
            async with get_db_context() as session:
                data_service = get_data_service(session)
                data_path = await data_service.get_data_path(data_id)
            
            chunk_size = 1 * 1024 * 1024  # 1MB chunks
            total_size = os.path.getsize(data_path)
            bytes_sent = 0
            
            self.logger.debug(f"Streaming data {data_id}, total size: {format_bytes(total_size)}")
            start_time = time.time()
            with open(data_path, 'rb') as f:

                while bytes_sent < total_size:
                    # Calculate chunk boundaries
                    start = int(bytes_sent)
                    end = int(min(start + chunk_size, total_size))
                    chunk_data = f.read(chunk_size)
                    
                    # Create chunk
                    is_last = (end >= total_size)
                    chunk = common_pb2.DataChunk(
                        chunk_data=chunk_data,
                        is_last_chunk=is_last
                    )
                    
                    yield chunk
                    
                    bytes_sent = end
                    # self.logger.debug(f"Sent chunk {start}-{end} of {format_bytes(total_size)} (last: {is_last})")
            end_time = time.time()
            self.logger.debug(f"Completed streaming data {data_id} in {end_time - start_time} seconds")
            
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Error in StreamData: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")