import sys
import grpc
import signal
import asyncio
from app.core.logging import setup_logging
from protos import master_pb2_grpc
from protos import master_pb2
from gRPCApp.master_servicer import MasterServicer
from app.core.config import settings
from app.core.logging import setup_logging
from app.services.node import get_node_service
from app.db.session import get_db_context
from app.utils.constants import HEARTBEAT_TIMEOUT_CHECK_INTERVAL

logger = setup_logging("Master")

def setup_signal_handlers(server: grpc.Server):
    """Set up signal handlers for graceful shutdown."""
    running_loop = asyncio.get_running_loop()
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, starting graceful shutdown...")
        
        # This is a hack to stop wait for the server to stop, without using await because handler shouldn't be async
        asyncio.run_coroutine_threadsafe(server.stop(grace=1), running_loop)  # 30 second grace period
        
        print("Shutdown complete")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

async def heartbeat_loop():
    
    while True:
        await asyncio.sleep(HEARTBEAT_TIMEOUT_CHECK_INTERVAL)
        logger.debug(f"Checking for dead nodes")
        async with get_db_context() as session:
            node_service = get_node_service(session)
            await node_service.update_dead_nodes()

async def main():
    logger = setup_logging("Master")
    logger.info("Starting RunSurge Master")
    asyncio.create_task(heartbeat_loop())
    server = grpc.aio.server()
    master_pb2_grpc.add_MasterServiceServicer_to_server(MasterServicer(), server)
    server.add_insecure_port(f'0.0.0.0:{settings.GRPC_PORT}')
    await server.start()
    logger.info(f"Master server started on port {settings.GRPC_PORT}")
    setup_signal_handlers(server)
    await server.wait_for_termination()
