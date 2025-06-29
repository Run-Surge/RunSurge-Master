import sys
import os

# Add the protos/generated directory to the path to import generated protobuf files
sys.path.append(os.path.join(os.path.dirname(__file__), 'protos'))
from gRPCApp.main import main
import asyncio

if __name__ == "__main__":
    asyncio.run(main())