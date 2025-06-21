"""gRPC servicer implementation for the MasterService."""

import grpc
import traceback

from protos import master_pb2
from protos import master_pb2_grpc
from protos import common_pb2
from app.core.logging import setup_logging
from app.services.user import get_user_service
from app.db.session import get_db_context
from app.schemas.node import  NodeRegisterGRPC, NodeUpdateGRPC    
from app.schemas.user import UserLoginCreate
from app.services.node import get_node_service
from gRPCApp.utils import get_peer_ip_and_port

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
                ip, port = get_peer_ip_and_port(context.peer())
                node = await node_service.register_node(NodeRegisterGRPC(
                    user_id=user.user_id,
                    machine_fingerprint=request.machine_fingerprint,
                    memory_bytes=request.memory_bytes,
                    ip_address=ip,
                    port=port
                ))

                self.logger.info(f"Node registered successfully: {node}")
    
            return common_pb2.StatusResponse(success=True, message="Node registered successfully")
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Error in NodeRegister: {e}")
            return common_pb2.StatusResponse(success=False, message=str(e))
    
    def JobComplete(self, request, context):
        self.logger.info(f"JobComplete request: {request}")
        return master_pb2.JobCompleteResponse()

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
