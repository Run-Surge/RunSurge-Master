import json
import ast
import re
import math
import csv
import os
import sys
import traceback
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession

# --- 1. Imports and System Path Setup ---
sys.path.append(os.path.join(os.path.dirname(__file__), 'protos'))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.constants import JOBS_DIRECTORY_PATH, GROUPS_DIRECTORY_PATH
from app.utils.utils import convert_nodes_into_Json, get_file_size
from app.services.task import get_task_service
from app.services.data import get_data_service, get_input_data_service
from app.services.node import get_node_service
from app.services.job import get_job_service
from app.db.session import get_db_context, init_db
from app.db.models.scheme import JobStatus, JobType, Node
from app.services.worker_client import WorkerClient
from protos import worker_pb2
from app.core.config import Settings
from group_scheduler import single_task_job_scheduler

# --- 2. Global Settings and Helper Lambdas ---
settings = Settings()
get_lhs_var = lambda s: s.split("=")[0].strip()

# ==============================================================================
# 1. CORE MEMORY CALCULATION LOGIC
# ==============================================================================
def calculate_peak_memory_for_statements(
    statements, live_vars_data, func_footprints_data, stmt_to_idx_map
):
    """Calculates the true peak memory for a list of statements by simulating its execution."""
    peak_memory_for_block = 0
    for stmt in statements:
        live_vars_at_line = live_vars_data.get(stmt, {})
        sum_of_live_vars = sum(
            var_info["size"] for var_info in live_vars_at_line.values()
        )

        func_execution_size = 0
        original_idx = stmt_to_idx_map.get(stmt)
        if original_idx is not None:
            key_prefix_to_find = f"{stmt}"
            found_key = next(
                (k for k in func_footprints_data if k.startswith(key_prefix_to_find)),
                None,
            )
            if found_key:
                func_mem_dict = func_footprints_data[found_key]
                if func_mem_dict:
                    func_execution_size = list(func_mem_dict.values())[-1]

        instantaneous_memory = sum_of_live_vars + func_execution_size
        if instantaneous_memory > peak_memory_for_block:
            peak_memory_for_block = instantaneous_memory
    return peak_memory_for_block

# ==============================================================================
# 2. SCHEDULING ALGORITHMS
# ==============================================================================
def schedule_program_whole(
    initial_blocks,
    program_statements,
    nodes_data,
    live_vars_data,
    func_footprints_data,
    stmt_to_idx_map,
):
    """
    Attempt 1: Tries to schedule the entire program as one single block.
    MODIFIED: Returns a schedule_info list on success, or None on failure.
    """
    print("--- Attempt 1: Scheduling the entire program on a single node ---")

    peak_memory = calculate_peak_memory_for_statements(
        program_statements, live_vars_data, func_footprints_data, stmt_to_idx_map
    )
    print(f"Peak memory requirement for the whole program is: {peak_memory}")

    sorted_nodes = sorted(nodes_data, key=lambda x: x["memory"])
    fitting_node = next(
        (node for node in sorted_nodes if node["memory"] >= peak_memory), None
    )

    if fitting_node:
        print(
            f"SUCCESS: Program fits on '{fitting_node['name']}' (Memory: {fitting_node['memory']})."
        )

        # --- NEW: CONSTRUCT THE SCHEDULE INFO OBJECT ---
        # Find all keys that are initial inputs (i.e., depend on 'none')
        initial_keys = list(
            dict.fromkeys(
                key
                for block in initial_blocks
                for key in block.get("key", [])
                if key.endswith(":none")
            )
        )

        whole_program_schedule_info = [
            {
                "consolidated_block_index": 0,
                "peak_memory": peak_memory,
                "assigned_node": fitting_node,
                "is_schedulable": True,
                "key": initial_keys,
                "statements": program_statements,
            }
        ]
        return whole_program_schedule_info
    else:
        print("FAILURE: No single node has enough memory for the entire program.")
        return None  # Return None on failure

# ==============================================================================
# 1.1. CONTEXT-AWARE MEMORY CALCULATION
# ==============================================================================
def calculate_peak_memory_for_merged_block(
    block_statements, block_keys, live_vars_data, func_footprints_data, stmt_to_idx_map
):
    """
    Calculates the peak memory for a specific block (or a potential merged block),
    considering only its external dependencies (from keys) and internally created variables.
    """
    peak_memory_for_block = 0
    # A simple way to parse the variable being created (e.g., "z" from "z = add1(data)")
    get_lhs_var = lambda s: s.split(" ")[0]

    # These are the input variables the block depends on from the outside.
    external_dependency_vars = {
        k.split(":")[0] for k in block_keys if k.split(":")[0] != "none"
    }

    # This set will track variables created within the block as we iterate through its statements.
    created_internal_vars = set()

    for i, stmt in enumerate(block_statements):
        # 1. Get the memory cost of the function call itself.
        func_execution_size = 0
        original_idx = stmt_to_idx_map.get(stmt)
        if original_idx is not None:
            key_prefix_to_find = f"{stmt}"
            found_key = next(
                (k for k in func_footprints_data if k.startswith(key_prefix_to_find)),
                None,
            )
            if found_key and func_footprints_data[found_key]:
                # The peak memory inside a function is its final memory state
                func_execution_size = list(func_footprints_data[found_key].values())[-1]

        # 2. Identify all variables that should be live at this point *within this block's context*.
        # These are the external dependencies PLUS any variables created in previous statements of this block.
        # We don't need to union the sets on each iteration, just check membership in either.
        relevant_vars = external_dependency_vars.union(created_internal_vars)

        # 3. Sum the sizes of only these relevant live variables.
        # We look up their sizes in the global live_vars_data for the current statement.
        live_vars_at_line = live_vars_data.get(stmt, {})
        sum_of_relevant_live_vars = sum(
            var_info["size"]
            for var_name, var_info in live_vars_at_line.items()
            if var_name in relevant_vars
        )

        # 4. Update the set of internally created variables *after* calculating memory for the current line.
        created_internal_vars.add(get_lhs_var(stmt))

        # 5. Calculate total instantaneous memory and update the peak.
        instantaneous_memory = sum_of_relevant_live_vars + func_execution_size
        if instantaneous_memory > peak_memory_for_block:
            peak_memory_for_block = instantaneous_memory

    return peak_memory_for_block

# ==============================================================================
# 2.1. MERGING EXECUTION BLOCKS
# ==============================================================================
def process_and_merge_blocks(
    blocks_data, nodes_data, func_footprints_data, live_vars_data
):
    """Attempt 2: Merges blocks based on dependencies and memory, then schedules the final blocks."""
    print("\n--- Attempt 2: Merging execution blocks to find a feasible schedule ---")

    if not nodes_data:
        print("Error: No node data provided.")
        return blocks_data, []
    max_node_memory = max(node["memory"] for node in nodes_data)
    print(f"System's Maximum Node Memory for merging: {max_node_memory}")

    stmt_to_idx_map = {
        stmt: i for i, block in enumerate(blocks_data) for stmt in block["statements"]
    }

    blocks = json.loads(json.dumps(blocks_data))
    merged_in_pass, pass_num = True, 1

    while merged_in_pass:
        print(f"\n--- Starting Pass #{pass_num} ---")
        merged_in_pass = False
        source_idx = 0
        while source_idx < len(blocks):
            current_block = blocks[source_idx]
            did_merge_and_restart = False

            # Create a copy of keys to iterate over, as we might modify the original
            for key_str in list(current_block["key"]):
                try:
                    var, index_str = key_str.split(":")
                    if var == "none" or index_str == "none" or not index_str.isdigit():
                        continue

                    ### BUG FIX 1: Convert 1-based key to 0-based index ###
                    # The keys in the file are 1-based, but Python lists are 0-based.
                    target_idx = int(index_str) - 1

                    # A block cannot depend on itself. Also check for invalid indices.
                    if (
                        target_idx == source_idx
                        or target_idx < 0
                        or target_idx >= len(blocks)
                    ):
                        continue

                except (ValueError, IndexError):
                    continue

                target_block = blocks[target_idx]

                # The dependent block's statements must come AFTER the precedent's statements.
                # `current_block` depends on `target_block`.
                temp_merged_statements = (
                    target_block["statements"] + current_block["statements"]
                )

                # Combine keys, removing keys that are now internal to the merged block.
                # The dependency of `current_block` on `target_block` is resolved by the merge.
                temp_merged_keys = [
                    k for k in current_block["key"] if k != key_str
                ] + target_block["key"]
                temp_merged_keys = list(
                    dict.fromkeys(temp_merged_keys)
                )  # Remove duplicates

                merged_footprint = calculate_peak_memory_for_merged_block(
                    temp_merged_statements,
                    temp_merged_keys,
                    live_vars_data,
                    func_footprints_data,
                    stmt_to_idx_map,
                )

                if merged_footprint <= max_node_memory:
                    print(
                        f"  -> Merge PASSED: block {source_idx} (dep) into block {target_idx} (precedent). New Peak: {merged_footprint}"
                    )

                    # Perform the merge: append dependent statements to precedent statements
                    target_block["statements"].extend(current_block["statements"])
                    target_block["key"] = (
                        temp_merged_keys  # Use the already calculated keys
                    )

                    # The block at source_idx is being removed.
                    removed_block_key_val = source_idx + 1
                    blocks.pop(source_idx)

                    # After removing, the target block might have a new index if it was after the source.
                    merge_target_new_idx = (
                        target_idx if target_idx < source_idx else target_idx - 1
                    )
                    merge_target_new_key_val = merge_target_new_idx + 1

                    ### BUG FIX 2: Correctly re-index all keys in the system ###
                    for block_to_update in blocks:
                        new_keys = []
                        for k in block_to_update["key"]:
                            k_var, k_idx_str = k.split(":")
                            if not k_idx_str.isdigit():
                                new_keys.append(k)
                                continue

                            k_idx_val = int(k_idx_str)

                            if k_idx_val == removed_block_key_val:
                                # Dependency pointed to the removed block, so remap it to the merge target.
                                new_keys.append(f"{k_var}:{merge_target_new_key_val}")
                            elif k_idx_val > removed_block_key_val:
                                # Dependency pointed to a block after the removed one, so its index shifts down.
                                new_keys.append(f"{k_var}:{k_idx_val - 1}")
                            else:
                                # Dependency is unaffected.
                                new_keys.append(k)
                        block_to_update["key"] = new_keys

                    merged_in_pass = True
                    did_merge_and_restart = True
                    break  # Exit the keys loop for the current_block

            if did_merge_and_restart:
                # The `blocks` list has changed, so restart the scan from the beginning.
                source_idx = 0
            else:
                # No merge was performed for this block, move to the next one.
                source_idx += 1

        pass_num += 1

    print("\n--- Merging Complete. Calculating final scheduling info. ---")
    block_scheduling_info = []
    sorted_nodes = sorted(nodes_data, key=lambda x: x["memory"])
    for i, block in enumerate(blocks):
        final_peak_memory = calculate_peak_memory_for_merged_block(
            block["statements"],
            block["key"],
            live_vars_data,
            func_footprints_data,
            stmt_to_idx_map,
        )
        fitting_node = next(
            (node for node in sorted_nodes if node["memory"] >= final_peak_memory), None
        )
        block_scheduling_info.append(
            {
                "block_index": i,
                "statements": block["statements"],
                "peak_memory": final_peak_memory,
                "fitting_node": fitting_node,
            }
        )
        print(
            f"Final Block {i}: Peak Memory = {final_peak_memory}, Recommended Node = {fitting_node['name'] if fitting_node else 'None'}"
        )
    return blocks, block_scheduling_info

def consolidate_to_block_format(
    final_blocks,
    scheduling_info,
    nodes_data,
    live_vars_data,
    func_footprints_data,
    stmt_to_idx_map,
):
    """
    Consolidates contiguous schedulable blocks, but ONLY if the resulting
    merged block still fits on an available node.
    """
    print(
        "\n--- Post-Processing: Consolidating and re-mapping keys for final schedule ---"
    )

    if not nodes_data:
        print("Error: No node data provided. Cannot perform consolidation.")
        return final_blocks, scheduling_info

    max_node_memory = max(node["memory"] for node in nodes_data)
    print(f"System's Maximum Node Memory for consolidation: {max_node_memory}")
    consolidated_schedule_info = []
    i = 0
    if not any(info["fitting_node"] for info in scheduling_info):
        print("No schedulable blocks found. No consolidation performed.")
        # If nothing is schedulable, just return the final blocks as is, but with final info
        # (This part of the original code was missing a return, which could be an issue)
        # return final_blocks, scheduling_info
        for i, block in enumerate(scheduling_info):
            block_keys = final_blocks[i]["key"]
            consolidated_schedule_info.append({
                "consolidated_block_index": i+1,
                "peak_memory": block["peak_memory"],
                "assigned_node": block["fitting_node"], # Use the serializable dictionary
                "is_schedulable": block["fitting_node"] is not None,
                "key": block_keys,
                "statements": block["statements"],
            })
        return final_blocks, consolidated_schedule_info

    # --- Phase 1: Group contiguous schedulable blocks CONDITIONALLY ---
    preliminary_blocks = []
    old_to_new_index_map = {}
    i = 0
    while i < len(final_blocks):
        # We need the full info block, not just the fitting_node boolean
        block_info = scheduling_info[i]
        is_schedulable = block_info.get("fitting_node") is not None

        if is_schedulable:
            # Start a new potential group with the current block.
            current_group = {
                "key": list(final_blocks[i]["key"]),
                "statements": list(final_blocks[i]["statements"]),
            }
            new_idx = len(preliminary_blocks)
            old_to_new_index_map[i] = new_idx

            j = i + 1
            while j < len(final_blocks) and (
                scheduling_info[j].get("fitting_node") is not None
            ):
                potential_next_block = final_blocks[j]

                temp_merged_statements = (
                    current_group["statements"] + potential_next_block["statements"]
                )
                temp_merged_keys = list(
                    set(current_group["key"] + potential_next_block["key"])
                )

                simulated_peak = calculate_peak_memory_for_merged_block(
                    temp_merged_statements,
                    temp_merged_keys,
                    live_vars_data,
                    func_footprints_data,
                    stmt_to_idx_map,
                )

                if simulated_peak <= max_node_memory:
                    print(
                        f"  -> Merge SIMULATION PASSED: block {j} into group starting at {i} (New Peak: {simulated_peak})"
                    )
                    current_group["statements"] = temp_merged_statements
                    current_group["key"] = temp_merged_keys
                    old_to_new_index_map[j] = new_idx
                    j += 1
                else:
                    print(
                        f"  -> Merge SIMULATION FAILED: block {j} into group starting at {i} (New Peak: {simulated_peak} > {max_node_memory})"
                    )
                    break

            preliminary_blocks.append(current_group)
            i = j
        else:
            # This is an unschedulable block, add it as-is
            new_idx = len(preliminary_blocks)
            preliminary_blocks.append(final_blocks[i])
            old_to_new_index_map[i] = new_idx
            i += 1

    # --- Phase 2: Iterate through the new structure and rewrite all keys using the map ---
    final_consolidated_blocks = []
    for new_idx, block in enumerate(preliminary_blocks):
        updated_keys = set()
        for key_str in block["key"]:
            try:
                var, index_str = key_str.split(":")
                if not index_str.isdigit():
                    updated_keys.add(key_str)
                    continue

                # --- BUG FIX 1: Convert 1-based key from file to 0-based index for logic ---
                old_dep_index_0_based = int(index_str) - 1

                # Find the new index that the old dependency now maps to
                new_dep_index = old_to_new_index_map.get(old_dep_index_0_based)

                # Only keep the key if it points to a DIFFERENT consolidated block
                if new_dep_index is not None and new_dep_index != new_idx:
                    # --- BUG FIX 2: Convert new 0-based index back to 1-based for consistent output ---
                    updated_keys.add(f"{var}:{new_dep_index + 1}")
            except (ValueError, IndexError):
                updated_keys.add(key_str)

        final_consolidated_blocks.append(
            {"key": sorted(list(updated_keys)), "statements": block["statements"]}
        )

# --- Phase 3: Calculate final info for the newly consolidated blocks ---

    sorted_nodes = sorted(nodes_data, key=lambda x: x["memory"])

    for i, block in enumerate(final_consolidated_blocks):
        if block["statements"]:
            peak_memory = calculate_peak_memory_for_merged_block(
                block["statements"],
                block["key"],
                live_vars_data,
                func_footprints_data,
                stmt_to_idx_map,
            )
            fitting_node_obj = next(
                (node for node in sorted_nodes if node["memory"] >= peak_memory), None
            )
            
            # --- ### THIS IS THE FIX ### ---
            # The 'fitting_node' object from the previous step is a dictionary.
            # This ensures the final result is always a plain, JSON-serializable dictionary.
            fitting_node_dict = dict(fitting_node_obj) if fitting_node_obj else None

            consolidated_schedule_info.append({
                "consolidated_block_index": i+1,
                "peak_memory": peak_memory,
                "assigned_node": fitting_node_dict, # Use the serializable dictionary
                "is_schedulable": fitting_node_dict is not None,
                "key": block["key"],
                "statements": block["statements"],
            })
        else:
            consolidated_schedule_info.append(
                {
                    "consolidated_block_index": i+1,
                    "peak_memory": 0,
                    "assigned_node": None,
                    "is_schedulable": False,
                    "key": block["key"],
                    "statements": block["statements"],
                }
            )
            
    return final_consolidated_blocks, consolidated_schedule_info


# ==============================================================================
# 3. PARALLELIZATION LOGIC (LEVEL 3)
# ==============================================================================

def build_function_definitions(func_footprints_data):
    """
    Parses footprints and builds reconstructed source code using the
    user's file-writing logic, converted to build a string.
    """
    all_functions_code = {}
    processed_function_names = set()
    
    for key, footprint in func_footprints_data.items():
        match = re.search(r':(\w+)\(', key)
        if not match: continue
        
        func_name = match.group(1)
        if func_name in processed_function_names: continue

        # This list will hold the lines of the reconstructed function
        reconstructed_lines = []
        
        # This is your file-writing logic, converted to build a list of strings
        # --------------------------------------------------------------------
        values = [x.replace('\\n', '\n') for x in footprint.keys()]
        is_not_first = False
        for value in values:
            for split in value.split('\n'):
                if split != "aggregation":
                    line_to_add = ""
                    if is_not_first:
                        # This indents every line after the first with 3 spaces
                        line_to_add = '   ' + split 
                    else:
                        line_to_add = split
                    
                    reconstructed_lines.append(line_to_add)
                    is_not_first = True
        # --------------------------------------------------------------------

        # Join the collected lines into a single string
        reconstructed_code = "\n".join(reconstructed_lines)
        
        # Store the final string
        all_functions_code[func_name] = reconstructed_code
        processed_function_names.add(func_name)
        
    return all_functions_code

def plan_data_parallelization(
    consolidated_schedule_info,
    unschedulable_blocks,
    nodes_data, 
    live_vars_data, 
    func_footprints_data
):
    """
    LEVEL 3 BRAIN (ROBUST): Plans chained 1-to-1 parallelism using the 
    "Maximum Required Parallelism" for the entire chain.
    """
    print("\n--- Level 3: Planning Data Splitting and Parallelization (Robust Chaining) ---")
    
    parallelization_plan = {}
    split_instructions = {} 

    if not nodes_data:
        return parallelization_plan, split_instructions

    smallest_node_memory = min(n["memory"] for n in nodes_data) if nodes_data else 1
    
    planned_blocks_indices = set()

    for block_info in unschedulable_blocks:
        current_block_idx = block_info["consolidated_block_index"]
        if current_block_idx in planned_blocks_indices:
            continue

        chain = []
        curr = block_info
        while curr and not curr.get('is_schedulable'): # Use .get() for safety
            chain.insert(0, curr)
            planned_blocks_indices.add(curr['consolidated_block_index'])
            
            producer = None
            for key in curr.get('key', []):
                var, idx_str = key.split(':')
                if idx_str.isdigit():
                    producer_idx = int(idx_str)
                    producer = next((b for b in consolidated_schedule_info if b["consolidated_block_index"] == producer_idx), None)
                    break
            curr = producer

        if not chain:
            continue

        # --- 2. Calculate the required chunks for EACH stage in the chain ---
        required_chunks_per_stage = []
        for i, stage_block in enumerate(chain):
            statement = stage_block['statements'][0]
            
            input_var_name = ""
            if i == 0:
                match = re.match(r".*?\(([^,)]+)", statement)
                if match: input_var_name = match.group(1).strip()
            else:
                prev_stage_output_var = get_lhs_var(chain[i-1]['statements'][0])
                input_var_name = prev_stage_output_var

            if not input_var_name:
                required_chunks_per_stage.append(1)
                continue

            # ### START MODIFICATION (THE FIX) ###
            # Always calculate the required chunks from the TOTAL input size for that stage.
            # This is the robust "from scratch" method.
            total_input_size = live_vars_data.get(statement, {}).get(input_var_name, {}).get("size", stage_block['peak_memory'])
            num_chunks_for_this_stage = math.ceil(total_input_size / smallest_node_memory) if smallest_node_memory > 0 else 2
            required_chunks_per_stage.append(int(num_chunks_for_this_stage))
            # ### END MODIFICATION (THE FIX) ###

        # --- 3. Determine the final, unified chunk count for the entire chain ---
        final_num_chunks = max(required_chunks_per_stage) if required_chunks_per_stage else 2
        if final_num_chunks <= 1:
            final_num_chunks = 2
        
        print(f"\n-> Analyzed chain starting at block {chain[0]['consolidated_block_index']}. Needs per stage: {required_chunks_per_stage}. Final division factor: {final_num_chunks}")

        # --- 4. Create the final plan for each block in the chain ---
        for i, stage_block in enumerate(chain):
            statement = stage_block['statements'][0]
            
            if i == 0:
                is_initial_task = all(k.endswith(":none") for k in stage_block.get("key", []))
                if is_initial_task:
                    match = re.match(r".*?\(([^,)]+)", statement)
                    parallel_arg_name = match.group(1).strip() if match else ""
                    parallelization_plan[statement] = {
                        "status": "Success_Master_Split",
                        "parallel_arg_name": parallel_arg_name,
                        "num_chunks": final_num_chunks,
                    }
                else:
                    large_input_var = ""
                    for key in stage_block.get("key", []):
                        var, idx_str = key.split(":")
                        if idx_str.isdigit(): large_input_var = var; break
                    parallelization_plan[statement] = {
                        "status": "Success_Consumer",
                        "consumes_variable": large_input_var,
                        "num_chunks": final_num_chunks
                    }
            else:
                prev_stage_output_var = get_lhs_var(chain[i-1]['statements'][0])
                parallelization_plan[statement] = {
                    "status": "Success_Chained_Consumer",
                    "consumes_variable": prev_stage_output_var,
                    "num_chunks": final_num_chunks,
                }

    return parallelization_plan, split_instructions

# ==============================================================================
# 4. EXECUTION PLAN GENERATION
# ==============================================================================

async def generate_sequential_assignment(
    info: dict,
    job_id: int,
    job_dir: str,
    node_map: dict,
    node_address_map: dict,
    var_to_data_id_map: dict,
    true_final_block_idx: int, 
    input_file: str,
    all_functions_source: str,
    worker_client: WorkerClient,
    split_instruction: dict,
    session: AsyncSession,
    var_consumers: dict,
    parallelization_plan: dict,
    consolidated_schedule_info: list,
    full_split_instructions: dict 
):
    """
    Handles a single schedulable block, including aggregation of parallel results.
    """
    block_idx = info["consolidated_block_index"]
    node_name = info["assigned_node"]["name"]
    assigned_node_id = node_map.get(node_name)
    ip_address, port = node_address_map.get(node_name)
    valid_keys = [k for k in info.get("key", []) if k != "none:none"]

    required_data_ids = []
    task_python_code = []
    task_service = get_task_service(session)
    data_service = get_data_service(session)

    for key in info.get("key", []):
        var, source_idx_str = key.split(":")
        
        if source_idx_str == 'none':
            if var_to_data_id_map.get(var):
                required_data_ids.append(var_to_data_id_map[var])
                task_python_code.append(f"    {var} = wait_for_data('{input_file}', '{job_id}')")
        elif source_idx_str.isdigit():
            source_block_idx = int(source_idx_str)
            
            # Check if the dependency comes from a parallel chain
            producer_instructions = full_split_instructions.get(source_block_idx)
            if producer_instructions and producer_instructions.get('partial_output_data_ids'):
                num_chunks = producer_instructions['num_chunks']
                output_var_of_source = producer_instructions['variable_to_split']
                
                print(f"  -> Task for block {block_idx} will aggregate '{var}' from {num_chunks} chunks.")
                
                task_python_code.append(f"    # Aggregate partial results for '{var}'")
                task_python_code.append(f"    aggregated_results = []")
                task_python_code.append(f"    for i in range({num_chunks}):")
                task_python_code.append(f"        chunk_filename = f'{output_var_of_source}_partial_output_chunk_{{i}}.csv'")
                task_python_code.append(f"        partial_data = wait_for_data(chunk_filename, '{job_id}')")
                task_python_code.append(f"        if partial_data:")
                task_python_code.append(f"            # Correctly handle CSV header: add it once, then only data rows.")
                task_python_code.append(f"            if not aggregated_results: aggregated_results.extend(partial_data)")
                task_python_code.append(f"            else: aggregated_results.extend(partial_data[1:])")
                task_python_code.append(f"    {var} = aggregated_results")

                required_data_ids.extend(producer_instructions['partial_output_data_ids'].values())
            
            elif var_to_data_id_map.get(var):
                 required_data_ids.append(var_to_data_id_map.get(var))
                 task_python_code.append(f"    {var} = wait_for_data('{var}.csv', '{job_id}')")

    created_task = await task_service.create_task(job_id=job_id, data_ids=required_data_ids, required_ram=int(info["peak_memory"]), node_id=assigned_node_id)
    current_task_id = created_task.task_id
    task_python_code.insert(0, f"# --- Task {current_task_id} for Job {job_id} ---")
    print(f"\nCreated DB Task with ID: {current_task_id} for Sequential Block {block_idx} on node {node_name}")

    output_data_infos = [] 
    
    for stmt in info["statements"]:
        output_var = get_lhs_var(stmt)
        task_python_code.append(f"    print(f'EXECUTING: {stmt}')")
        task_python_code.append(f"    {stmt}")
        
        if split_instruction and split_instruction["variable_to_split"] == output_var:
            num_chunks = split_instruction['num_chunks']
            chunk_filename_to_id_map = {}
            for i in range(num_chunks):
                chunk_filename = f"{output_var}_chunk_{i}_job_{job_id}.csv"
                chunk_data_obj = await data_service.create_data(file_name=chunk_filename, parent_task_id=current_task_id)
                data_id = chunk_data_obj.data_id
                # ### FIX: Use the integer data_id directly ###
                output_data_infos.append({'data_id': data_id, 'data_name': chunk_filename})
                chunk_filename_to_id_map[chunk_filename] = data_id
            split_instruction['chunk_data_ids'] = chunk_filename_to_id_map    
            
            task_python_code.extend([
                f"    if isinstance({output_var}, list) and len({output_var}) > 1:",
                f"        header, data_rows = {output_var}[0], {output_var}[1:]",
                f"        chunk_size = math.ceil(len(data_rows) / {num_chunks}) if len(data_rows) > 0 else 0",
                f"        for i in range({num_chunks}):",
                f"            chunk_rows = data_rows[i * chunk_size:(i + 1) * chunk_size]",
                f"            if not chunk_rows: continue",
                f"            chunk_filename = f'{output_var}_chunk_{{i}}_job_{job_id}.csv'",
                # ### FIX: The worker script must construct the full path ###
                f"            chunk_filepath = os.path.join(chunk_filename)",
                f"            with open(chunk_filepath, 'w', newline='') as f: writer = csv.writer(f); writer.writerow(header); writer.writerows(chunk_rows)",
            ])
        elif output_var in var_consumers:
            output_filename = f"{output_var}.csv"
            if output_var not in var_to_data_id_map:
                db_data_obj = await data_service.create_data(file_name=output_filename, parent_task_id=current_task_id)
                var_to_data_id_map[output_var] = db_data_obj.data_id
            
            output_data_infos.append({'data_id': var_to_data_id_map[output_var], 'data_name': output_filename})
            temp_output_filename = f"temp_{output_var}_{os.urandom(4).hex()}.csv"
            # ### FIX: The worker script must construct the full path ###
            final_filepath_in_script = f"os.path.join('{output_filename}')"
            task_python_code.append(f"    with open(r'{temp_output_filename}', 'w', newline='') as f: csv.writer(f).writerows({output_var})")
            task_python_code.append(f"    os.rename(r'{temp_output_filename}', {final_filepath_in_script})")

    if block_idx == true_final_block_idx:
        final_output_var = get_lhs_var(info["statements"][-1])
        final_output_filename = "output.csv"
        db_data_obj = await data_service.create_data(file_name=final_output_filename, parent_task_id=current_task_id)
        data_id = db_data_obj.data_id
        var_to_data_id_map[final_output_var] = data_id
        await get_job_service(session).update_job_output_data_id(job_id, data_id)
        
        temp_output_filename = f"temp_output_{os.urandom(4).hex()}.csv"
        final_filepath_in_script = f"os.path.join('{final_output_filename}')"
        output_data_infos.append({'data_id': data_id, 'data_name': final_output_filename})
        
        task_python_code.append(f"    with open(r'{temp_output_filename}', 'w', newline='') as f: csv.writer(f).writerows({final_output_var})")
        task_python_code.append(f"    os.rename(r'{temp_output_filename}', {final_filepath_in_script})")

    task_script_name = f"task_{current_task_id}_script.py"
    full_task_script_content = assemble_script(job_id, ip_address, port, all_functions_source, task_python_code)
    
    script_path = os.path.join(job_dir, task_script_name)
    with open(script_path, "w") as f:
        f.write(full_task_script_content)
    print(f"  -> Wrote sequential task script to '{script_path}'")

    task_assignment_message = worker_pb2.TaskAssignment(
        task_id=current_task_id,
        python_file=full_task_script_content.encode('utf-8'),
        python_file_name=task_script_name,
        required_data_ids=required_data_ids,
        output_data_infos=[worker_pb2.OutputDataInfo(**oi) for oi in output_data_infos],
        job_id=job_id
    )
    
    success = await worker_client.assign_task(task_assignment=task_assignment_message, ip_address=ip_address, port=port)

    if success:
        print(f"  -> Successfully assigned Task {current_task_id}.")
        # Correctly notify for all data that is ready now.
        for key in valid_keys:
            if key.split(":")[1] == 'none':
                var = key.split(":")[0]
                notification = worker_pb2.DataNotification(
                    task_id=current_task_id, data_id=var_to_data_id_map[var], data_name=input_file,
                    ip_address=settings.GRPC_IP, port=settings.GRPC_PORT, hash="", is_zipped=False
                )
                notify_success = await worker_client.notify_data(notification, ip_address, port)
                if notify_success:
                    print(f"  -> Notified worker about input data for variable '{var}'.")
    else:
        print(f"  -> FAILED to assign Task {current_task_id}.")
    
    task_summary = {
        "task_id": current_task_id,
        "task_type": "Sequential/Aggregator",
        "assigned_node": info.get("assigned_node", {}).get("name"),
        "script_name": task_script_name,
        "required_data_ids": required_data_ids,
        "output_data": [{'data_id': oi['data_id'], 'data_name': oi['data_name']} for oi in output_data_infos]
    }
    return task_summary

def assemble_script(job_id, ip_address, port, all_functions_source, task_python_code):
    """Helper to assemble the final python script content."""
    python_harness_preamble = f"""
import time, csv, os, sys, json, asyncio, math
# Make sure the script can find project modules
sys.path.append('{os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))}')
# The worker script gets its own global JOB_ID.
JOB_ID = "{job_id}"
JOBS_DIRECTORY_PATH = "{JOBS_DIRECTORY_PATH}"

def infer_type(value):
    try: return int(value)
    except (ValueError, TypeError):
        try: return float(value)
        except (ValueError, TypeError): return str(value).strip()

def wait_for_data(file_name, job_id_str):
    file_path = os.path.join(file_name)
    print(f"--> [WAIT] Waiting for '{{file_path}}'...")
    while not os.path.exists(file_path):
        time.sleep(1)
    print(f"<-- [RECV] Received '{{file_path}}'.")
    try:
        with open(file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            lines = list(reader)
            if not lines: return []
            return [[infer_type(cell) for cell in row] for row in lines]
    except Exception as e:
        print(f"[ERROR] Failed to load file '{{file_name}}': {{e}}")
        return None
"""
    
    joined_task_code = "\n".join(task_python_code)
    
    # FIX: Use simple string concatenation to avoid nested f-string SyntaxError
    main_logic = f"""
def main():
{joined_task_code}

if __name__ == "__main__":
    print("\\n*** Starting Task for Job " + JOB_ID + " ***\\n")
    main()
    print("\\n*** Task for Job " + JOB_ID + " complete. ***\\n")
"""
    return python_harness_preamble + "\n\n" + all_functions_source + "\n\n" + main_logic

async def generate_execution_plan(
    job_id: int,
    job_dir: str,
    node_map: dict,
    nodes_data: list,
    consolidated_schedule_info,
    live_vars_data,
    func_footprints_data,
    input_file: str,
    parallelization_plan,
    split_instructions,
    session: AsyncSession
):
    """High-level orchestrator that now correctly handles all scheduling patterns."""
    print("\n--- Generating Final Execution Plan ---")

    node_address_map = {node["name"]: (node["ip_address"], node["port"]) for node in nodes_data}
    var_to_data_id_map = {}
    execution_summary = []
    
    data_service = get_data_service(session)
    all_initial_vars = {key.split(":")[0] for block in consolidated_schedule_info for key in block.get("key", []) if key.endswith(":none") and key.split(":")[0] != "none"}
    for var_name in all_initial_vars:
        db_data_obj = await data_service.create_data(file_name=input_file, parent_task_id=None)
        var_to_data_id_map[var_name] = db_data_obj.data_id
        
    function_definitions = build_function_definitions(func_footprints_data)
    all_functions_source = "\n\n".join(function_definitions.values())

    true_final_block_idx = -1
    if consolidated_schedule_info:
        true_final_block_idx = consolidated_schedule_info[-1]["consolidated_block_index"]
    
    var_consumers = {}
    for block in consolidated_schedule_info:
        for key in block.get('key', []):
            var, idx_str = key.split(':')
            if idx_str.isdigit():
                if var not in var_consumers: var_consumers[var] = set()
                assigned_node_info = block.get('assigned_node')
                if assigned_node_info:
                    consumer_node_name = assigned_node_info.get('name')
                    if consumer_node_name: var_consumers[var].add(consumer_node_name)

    task_service = get_task_service(session)
    worker_client = WorkerClient()
        
    planned_blocks_indices = set()
    
    # --- PASS 1: Handle all UN-SCHEDULABLE (Parallel) blocks first ---
    print("\n--- Pass 1: Planning and Generating Parallel Fused Tasks ---")
    for info in consolidated_schedule_info:
        if info["is_schedulable"] or info["consolidated_block_index"] in planned_blocks_indices:
            continue

        chain = []
        curr = info
        while curr and not curr.get('is_schedulable'):
            chain.append(curr)
            planned_blocks_indices.add(curr['consolidated_block_index'])
            output_var = get_lhs_var(curr['statements'][-1])
            next_in_chain = next((b for b in consolidated_schedule_info if any(k.startswith(f"{output_var}:{curr['consolidated_block_index']}") for k in b.get('key', []))), None)
            curr = next_in_chain
        
        if not chain: continue

        print(f"\n--- Fusing and generating tasks for parallel chain of {len(chain)} stages ---")
        first_stage_plan = parallelization_plan.get(chain[0]['statements'][0], {})
        num_chunks = first_stage_plan.get('num_chunks', 0)
        if num_chunks == 0: continue
        
        final_output_var_in_chain = get_lhs_var(chain[-1]['statements'][-1])
        last_block_in_chain_idx = chain[-1]['consolidated_block_index']
        split_instructions[last_block_in_chain_idx] = {
            'variable_to_split': final_output_var_in_chain,
            'num_chunks': num_chunks,
            'partial_output_data_ids': {} 
        }
        
        for chunk_id in range(num_chunks):
            worker_node = nodes_data[chunk_id % len(nodes_data)]
            assigned_node_id = node_map[worker_node["name"]]
            ip_address, port = worker_node["ip_address"], worker_node["port"]

            required_data_ids_for_task = []
            task_python_code = []
            notification_to_send = None

            if first_stage_plan.get('status') == 'Success_Master_Split':
                parallel_arg_name = first_stage_plan['parallel_arg_name']
                chunk_filename = f"{parallel_arg_name}_chunk_{chunk_id}_job_{job_id}.csv"
                
                primary_input_path = os.path.join(job_dir, input_file)
                try:
                    with open(primary_input_path, 'r') as f: rows = list(csv.reader(f))
                    header, data_rows = rows[0], rows[1:]
                    chunk_size = math.ceil(len(data_rows) / num_chunks)
                    chunk_data_rows = data_rows[chunk_id * chunk_size:(chunk_id + 1) * chunk_size]
                    if not chunk_data_rows: continue
                    with open(os.path.join(job_dir, chunk_filename), 'w', newline='') as f:
                        w = csv.writer(f); w.writerow(header); w.writerows(chunk_data_rows)
                except (IOError, IndexError) as e:
                    print(f"Error preparing master-split chunk file: {e}")
                    continue
                
                chunk_data_obj = await data_service.create_data(file_name=chunk_filename, parent_task_id=None)
                chunk_data_id = chunk_data_obj.data_id
                required_data_ids_for_task.append(chunk_data_id)
                task_python_code.append(f"    {parallel_arg_name} = wait_for_data('{chunk_filename}', '{job_id}')")
                notification_to_send = {'data_id': chunk_data_id, 'data_name': chunk_filename}
            
            elif first_stage_plan.get('status') == 'Success_Consumer':
                consumes_var = first_stage_plan['consumes_variable']
                producer_block_idx = next((int(key.split(':')[1]) for key in chain[0].get('key', []) if key.startswith(f"{consumes_var}:")), -1)
                producer_split_instruction = split_instructions.get(producer_block_idx, {})
                chunk_filename = f"{consumes_var}_chunk_{chunk_id}_job_{job_id}.csv"
                chunk_data_id = producer_split_instruction.get('chunk_data_ids', {}).get(chunk_filename)
                
                if chunk_data_id:
                    required_data_ids_for_task.append(chunk_data_id)
                    notification_to_send = {'data_id': chunk_data_id, 'data_name': chunk_filename}
                task_python_code.append(f"    {consumes_var} = wait_for_data('{chunk_filename}', '{job_id}')")
            
            for stage_block in chain:
                for stmt in stage_block['statements']:
                    task_python_code.append(f"    print(f'EXECUTING FUSED: {stmt}')")
                    task_python_code.append(f"    {stmt}")

            partial_output_filename = f"{final_output_var_in_chain}_partial_output_chunk_{chunk_id}.csv"
            filepath_in_script = f"os.path.join('{partial_output_filename}')"
            task_python_code.append(f"    with open({filepath_in_script}, 'w', newline='') as f: csv.writer(f).writerows({final_output_var_in_chain})")

            fused_task_ram = int(sum(b['peak_memory'] for b in chain) / num_chunks) if num_chunks > 0 else sum(b['peak_memory'] for b in chain)
            created_task = await task_service.create_task(job_id=job_id, data_ids=required_data_ids_for_task, required_ram=fused_task_ram, node_id=assigned_node_id)
            current_task_id = created_task.task_id
            task_python_code.insert(0, f"# --- Fused Parallel Task {current_task_id} ---")
            
            output_data_obj = await data_service.create_data(file_name=partial_output_filename, parent_task_id=current_task_id)
            output_data_id = output_data_obj.data_id
            
            split_instructions[last_block_in_chain_idx]['partial_output_data_ids'][chunk_id] = output_data_id
            
            task_script_name = f"task_{current_task_id}_fused_script.py"
            full_task_script_content = assemble_script(job_id, ip_address, port, all_functions_source, task_python_code)
            with open(os.path.join(job_dir, task_script_name), "w") as f: f.write(full_task_script_content)
            
            output_data_infos = [{'data_id': output_data_id, 'data_name': partial_output_filename}]
            task_assignment_message = worker_pb2.TaskAssignment(
                task_id=current_task_id, python_file=full_task_script_content.encode('utf-8'),
                python_file_name=task_script_name, job_id=job_id,
                required_data_ids=required_data_ids_for_task,
                output_data_infos=[worker_pb2.OutputDataInfo(**oi) for oi in output_data_infos]
            )
            success = await worker_client.assign_task(task_assignment=task_assignment_message, ip_address=worker_node['ip_address'], port=worker_node['port'])
            
            if success:
                print(f"  -> Successfully assigned Fused Task {current_task_id}.")
                if notification_to_send:
                    notification = worker_pb2.DataNotification(
                        task_id=current_task_id, data_id=notification_to_send['data_id'],
                        data_name=notification_to_send['data_name'], ip_address=settings.GRPC_IP, 
                        port=settings.GRPC_PORT, hash=""
                    )
                    await worker_client.notify_data(notification, worker_node['ip_address'], worker_node['port'])
            else:
                print(f"  -> FAILED to assign Fused Task {current_task_id}.")   
            
            # ### START MODIFICATION: Create and append summary for this fused task ###
            task_summary = {
                "task_id": current_task_id,
                "task_type": "Fused Parallel",
                "chunk_id": chunk_id,
                "fused_stages": [b['consolidated_block_index'] for b in chain],
                "assigned_node": worker_node['name'],
                "script_name": task_script_name,
                "required_data_ids": required_data_ids_for_task,
                "output_data": output_data_infos
            }
            execution_summary.append(task_summary)             
            # ### END MODIFICATION ###

    # --- PASS 2: Handle all SCHEDULABLE (Sequential/Aggregator) blocks ---
    print("\n--- Pass 2: Planning and Generating Sequential/Aggregator Tasks ---")
    for info in consolidated_schedule_info:
        if info["is_schedulable"]:
            block_idx = info["consolidated_block_index"]
            print(var_to_data_id_map)
            task_summary = await generate_sequential_assignment(
                info, job_id, job_dir, node_map, node_address_map, var_to_data_id_map, 
                true_final_block_idx, input_file, all_functions_source, worker_client, 
                split_instructions.get(block_idx), session, var_consumers, 
                parallelization_plan, consolidated_schedule_info,
                split_instructions
            )
            if task_summary:
                execution_summary.append(task_summary)

    print("\nMaster execution plan generation complete.")
    return execution_summary

# ==============================================================================
# 5. MAIN SCHEDULER ORCHESTRATOR
# ==============================================================================
async def scheduler(job_id: int, session: AsyncSession, input_file: str):
    task_service = get_task_service(session)
    node_service = get_node_service(session)
    nodes = await node_service.get_all_nodes()
    
    # Adjust node RAM based on current consumption
    for node in nodes:
        consumed_ram = await task_service.get_total_node_ram(node.node_id)
        node.ram = node.ram - consumed_ram if node.ram > consumed_ram else 0
    
    nodes_data, node_map = convert_nodes_into_Json(nodes)
    job_dir = os.path.join(JOBS_DIRECTORY_PATH, str(job_id))
    print(f"Scheduler using available nodes: {nodes_data}")

    if not nodes_data:
        print(f"FATAL ERROR: No worker nodes have available memory. Cannot schedule job {job_id}.")
        # Optionally update job status to pending/failed
        return

    try:
        with open(os.path.join(job_dir, 'main_lists.json'), 'r') as f: initial_blocks_raw = json.load(f)
        with open(os.path.join(job_dir, 'main_lines_footprint.json'), 'r') as f: live_vars_data = json.load(f)
        with open(os.path.join(job_dir, 'func_lines_footprint.json'), 'r') as f: func_footprints_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error: Could not find required input file: {e.filename}")
        return

    # Assuming keys in main_lists.json are 1-based, convert to 0-based for internal use.
    initial_blocks = []
    for block in initial_blocks_raw:
        new_keys = []
        for key in block['key']:
            var, idx_str = key.split(':')
            if idx_str.isdigit():
                new_keys.append(f"{var}:{int(idx_str)}")
            else:
                new_keys.append(key)
        initial_blocks.append({'key': new_keys, 'statements': block['statements']})

    full_program_statements = [stmt for block in initial_blocks for stmt in block["statements"]]
    stmt_to_original_idx_map = {stmt: i for i, block in enumerate(initial_blocks_raw) for stmt in block["statements"]}
    
    # --- Execute the scheduling workflow (Levels 1, 2, 3) ---
    final_blocks, scheduling_info, consolidated_schedule_info, parallelization_plan, split_instructions = [], [], [], {}, {}

    # --- LEVEL 1 ---
    consolidated_schedule_info = schedule_program_whole(initial_blocks, full_program_statements, nodes_data, live_vars_data, func_footprints_data, stmt_to_original_idx_map)
    
    job_is_fully_schedulable = False
    if consolidated_schedule_info is None:
        # --- LEVEL 2 ---
        final_blocks, scheduling_info = process_and_merge_blocks(initial_blocks, nodes_data, func_footprints_data, live_vars_data)
        # with open(os.path.join(job_dir, "final_blocks_after_merge.json"), "w") as f:
        #     json.dump(final_blocks, f, indent=4)
        # with open(os.path.join(job_dir, "scheduling_info_before_consolidation.json"), "w") as f:
        #     json.dump(scheduling_info, f, indent=4)
        consolidated_schedule, consolidated_schedule_info = consolidate_to_block_format(final_blocks, scheduling_info, nodes_data, live_vars_data, func_footprints_data, stmt_to_original_idx_map)
        # with open(os.path.join(job_dir, "final_consolidated_schedule.json"), "w") as f:
        #     json.dump(consolidated_schedule, f, indent=4)
        with open(os.path.join(job_dir, "final_schedule_info.json"), "w") as f:
            json.dump(consolidated_schedule_info, f, indent=4)

        unschedulable_final_blocks = [info for info in consolidated_schedule_info if not info["is_schedulable"]]
        
        if not unschedulable_final_blocks:
            job_is_fully_schedulable = True
        else:
            # --- LEVEL 3 ---
            parallelization_plan, split_instructions = plan_data_parallelization(
                consolidated_schedule_info, unschedulable_final_blocks, nodes_data,
                live_vars_data, func_footprints_data
            )
            with open(os.path.join(job_dir, "parallelization_plan.json"), "w") as f:
                json.dump(parallelization_plan, f, indent=4)
            # Check if all parallelization plans were successful
            is_any_plan_deferred = any(p.get("status") == "Deferred" for p in parallelization_plan.values())
            if is_any_plan_deferred:
                print("\nHALTING JOB: One or more blocks could not be parallelized and are deferred.")
                # You can update job status to pending_schedule here
                return
            else:
                job_is_fully_schedulable = True
    else:
        job_is_fully_schedulable = True

    if job_is_fully_schedulable:
        # --- FINAL STEP: GENERATE EXECUTION PLAN WITH DB INTEGRATION ---
        final_execution_plan = await generate_execution_plan(
            job_id=job_id, job_dir=job_dir, node_map=node_map,
            nodes_data=nodes_data, consolidated_schedule_info=consolidated_schedule_info,
            func_footprints_data=func_footprints_data,
            input_file=input_file, parallelization_plan=parallelization_plan,
            split_instructions=split_instructions, session=session, live_vars_data=live_vars_data
        )
        job_service = get_job_service(session)
        await job_service.update_job_status(job_id, JobStatus.running)
    
    # --- Write all debugging output files to the job directory ---
    # print(f"\n--- Writing final debug files to '{job_dir}' ---")
    # try:
    #     with open(os.path.join(job_dir, "final_schedule_info.json"), "w") as f: json.dump(consolidated_schedule_info, f, indent=4)
    #     with open(os.path.join(job_dir, "parallelization_plan.json"), "w") as f: json.dump(parallelization_plan, f, indent=4)
    #     with open(os.path.join(job_dir, "split_instructions.json"), "w") as f: json.dump(split_instructions, f, indent=4)
    #     print("Final debug files written successfully.")
    # except Exception as e:
    #     print(f"An error occurred while writing debug files: {e}")
    execution_plan_path = os.path.join(job_dir, "execution_plan.json")
    with open(execution_plan_path, "w") as f:
        json.dump(final_execution_plan, f, indent=4)
    print("\n--- SCHEDULING PROCESS COMPLETE ---")


async def main():
    await init_db()
    while True:
        async with get_db_context() as session:
            try:
                job_service = get_job_service(session)
                jobs = await job_service.get_jobs_not_scheduled()

                if not jobs:
                    await asyncio.sleep(2)
                    continue
                
                print(f"Found {len(jobs)} jobs to schedule. Processing one.")
                job = jobs[0]
                print(f"Processing Job ID: {job.job_id}, Type: {job.job_type}")
                
                input_data_service = get_input_data_service(session)
                input_data = await input_data_service.get_input_data(job.job_id)
                
                if not input_data:
                    print(f"No input data found for job {job.job_id}. Skipping.")
                    # Optionally update job status to failed/error
                    await asyncio.sleep(1)
                    continue
                
                input_file = str(input_data.input_data_id) + ".csv"
                
                if job.job_type == JobType.complex:
                    print("Handling complex job with single_task_job_scheduler.")
                    group_id = str(job.group_id)
                    file_path = os.path.join(GROUPS_DIRECTORY_PATH, group_id, f"{group_id}.py")
                    if os.path.exists(file_path):
                        file_size = get_file_size(file_path)
                        await single_task_job_scheduler(job.group_id, job.job_id, input_data.input_data_id, file_size, job.required_ram, session)                          
                    else:
                        print(f"ERROR: Script file not found for complex job: {file_path}")

                else: # Standard job type
                    print("Handling standard job with main scheduler.")
                    await scheduler(job.job_id, session, input_file)

            except Exception as e:
                print(traceback.format_exc())
                print(f"An unhandled error occurred in the main loop: {e}")
            
            await asyncio.sleep(5) # Wait before polling for new jobs

if __name__ == "__main__":
    asyncio.run(main())