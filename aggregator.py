import sys
import os

# Add the protos/generated directory to the path to import generated protobuf files
sys.path.append(os.path.join(os.path.dirname(__file__), 'protos'))
import re
import asyncio
import subprocess
from app.db.session import get_db_context
from app.db.repositories.group import GroupRepository
from app.services.group import get_group_service
from app.db.models.scheme import GroupStatus, Group
from app.core.logging import setup_logging
from app.utils.constants import GROUPS_DIRECTORY_PATH

logger = setup_logging(__name__)

async def aggregate_group(group: Group):
    group_dir = f"{GROUPS_DIRECTORY_PATH}/{group.group_id}"
    aggregator_file = f"{group_dir}/{group.aggregator_file_path}"
    result = subprocess.run(["python", aggregator_file], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    if result.returncode != 0:
        logger.error(f"Error aggregating group {group.group_id}: {result.stderr.decode('utf-8')}")
        async with get_db_context() as db:
            group_service = get_group_service(db)
            await group_service.update_group_status(group.group_id, GroupStatus.failed)
        return

    async with get_db_context() as db:
        group_service = get_group_service(db)
        await group_service.update_group_status(group.group_id, GroupStatus.completed)

async def main_loop():
    while True:
        async with get_db_context() as db:
            group_repo = GroupRepository(db)
            group = await group_repo.get_single_group_by_status(GroupStatus.pending_aggregation)
            if not group:
                logger.info("No group to aggregate")
                await asyncio.sleep(5)
                continue

            logger.info(f"Aggregating group {group.group_id}")
            await aggregate_group(group)
            logger.info(f"Group {group.group_id} aggregated")


    
if __name__ == "__main__":
    asyncio.run(main_loop())