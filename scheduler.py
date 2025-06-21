import json
import argparse
import ast
import re
import math
import csv
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.utils.constants import JOBS_DIRECTORY_PATH
from app.utils.utils import convert_nodes_into_Json
from app.services.task import get_task_service
from app.services.data import get_data_service
from app.services.node import get_node_service
from app.services.job import get_job_service
from app.db.session import  get_db_context
from app.db.models.scheme import JobStatus
import asyncio
global_session = None
input_file = "123456.csv"
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


# ==============================================================================
# 3. CONSOLIDATION AND SCHEDULING INFO
# ==============================================================================
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

    if not any(info["fitting_node"] for info in scheduling_info):
        print("No schedulable blocks found. No consolidation performed.")
        # If nothing is schedulable, just return the final blocks as is, but with final info
        # (This part of the original code was missing a return, which could be an issue)
        return final_blocks, scheduling_info

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
    consolidated_schedule_info = []
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
            fitting_node = next(
                (node for node in sorted_nodes if node["memory"] >= peak_memory), None
            )

            consolidated_schedule_info.append(
                {
                    "consolidated_block_index": i,
                    "peak_memory": peak_memory,
                    "assigned_node": fitting_node,
                    "is_schedulable": fitting_node is not None,
                    "key": block["key"],
                    "statements": block["statements"],
                }
            )
        else:
            consolidated_schedule_info.append(
                {
                    "consolidated_block_index": i,
                    "peak_memory": 0,
                    "assigned_node": None,
                    "is_schedulable": False,
                    "key": block["key"],
                    "statements": block["statements"],
                }
            )

    return final_consolidated_blocks, consolidated_schedule_info


# ==============================================================================
# 4. PARALLELIZATION WITH DEFERRAL LOGIC
# ==============================================================================
def get_iterable_name(node):
    """Safely gets the variable name from a for-loop's iterable node."""
    if isinstance(node, ast.Name):
        return node.id
    return None


def is_infeasible_due_to_nested_loops(py_source_code, arg_names):
    try:
        tree = ast.parse(py_source_code)
    except SyntaxError as e:
        # It hits this exception because of the bad indentation!
        print(f"  Warning: Could not parse reconstructed source code...")
        return True  # It returns True (infeasible) on any syntax error
    for node in ast.walk(tree):
        if isinstance(node, ast.For):
            for inner_node in ast.walk(node):
                if inner_node is node:
                    continue
                if isinstance(inner_node, ast.For):
                    iterable_var = get_iterable_name(inner_node.iter)
                    if iterable_var and iterable_var in arg_names:
                        print(
                            f"  -> Feasibility Check FAILED: Found nested loop where inner loop iterates over argument '{iterable_var}'."
                        )
                        return True
    return False


def reconstruct_source_with_indentation(lines_of_code):
    """
    Reconstructs a Python source string with plausible indentation.
    This version correctly handles keys that are multi-line strings.
    """
    reconstructed_code = []
    indent_level = 0
    DEDENT_KEYWORDS = ("elif", "else:", "except", "finally")

    # First, flatten the list so that multi-line keys become multiple items
    actual_lines = []
    for key in lines_of_code:
        actual_lines.extend(key.split("\n"))

    for line in actual_lines:
        stripped_line = line.strip()
        if not stripped_line:  # Skip empty lines
            continue

        # Check for dedentation before applying current indent
        if stripped_line.startswith(DEDENT_KEYWORDS):
            indent_level = max(0, indent_level - 1)

        reconstructed_code.append(
            ("    " * indent_level) + stripped_line
        )  # Use 4 spaces for clarity

        # Check for indentation for the *next* line
        if stripped_line.endswith(":"):
            indent_level += 1

    return "\n".join(reconstructed_code)

def sanitize_statement_for_filename(statement: str) -> str:
    """Converts a Python statement into a safe string for a filename."""
    # Replace common special characters and spaces with underscores
    sanitized = re.sub(r'[=\s(),.]+', '_', statement)
    # Collapse multiple underscores into one
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    return sanitized.strip('_')

def plan_data_parallelization(
    unschedulable_blocks, nodes_data, live_vars_data, func_footprints_data
):
    """
    For unschedulable blocks, determines if a parallelization plan can be made
    statically or if the decision must be deferred.

    MODIFIED: For deferred blocks, generates a new 'main_lists.json'-style file
    for future rescheduling.
    """
    print("\n--- Level 3: Planning Data Parallelization for Failed Blocks ---")

    if not unschedulable_blocks:
        print("No unschedulable blocks to process at this level.")
        return {}
    if not nodes_data:
        print("Error: No node data available for parallelization planning.")
        return {}

    sorted_nodes = sorted(nodes_data, key=lambda x: x["memory"])
    smallest_node_memory = sorted_nodes[0]["memory"]
    print(
        f"Using smallest node '{sorted_nodes[0]['name']}' with {smallest_node_memory} memory as baseline."
    )

    parallelization_plan = {}
    for block in unschedulable_blocks:
        if not block["statements"]:
            continue

        statement = block["statements"][0]
        print(f"\nAnalyzing: '{statement}'")

        block_keys = block.get("key", [])
        has_real_dependency = any(
            k.split(":")[1].isdigit() for k in block_keys if ":" in k
        )

        # --- MODIFIED LOGIC FOR DEFERRED BLOCKS ---
        if has_real_dependency:
            print(f"  -> DEFERRED: Block depends on preceding blocks {block_keys}. Generating reschedule file.")

            # 1. Transform original keys into 'var:none' for the new standalone program.
            # This treats the previous outputs as the new program's primary inputs.
            new_input_keys = []
            for key in block_keys:
                if key == "none:none":
                    continue # This is not a variable dependency
                var, _ = key.split(":", 1)
                new_input_keys.append(f"{var}:none")

            # Create a clean, unique list of input keys.
            new_input_keys = sorted(list(set(new_input_keys)))

            # 2. Create the data structure for the new main_lists.json file.
            # It's a list containing a single block.
            reschedule_program_data = [
                {
                    "key": new_input_keys,
                    "statements": [statement]
                }
            ]

            # 3. Generate a safe filename and write the file.
            sanitized_name = sanitize_statement_for_filename(statement)
            filename = f"deferred_reschedule_{sanitized_name}.json"
            
            try:
                with open(filename, 'w') as f:
                    json.dump(reschedule_program_data, f, indent=4)
                print(f"  -> Successfully wrote reschedule file: {filename}")
            except IOError as e:
                print(f"  -> ERROR: Could not write reschedule file {filename}: {e}")

            # 4. Update the parallelization plan to include the new file's path.
            parallelization_plan[statement] = {
                "status": "Deferred: Requires Feedback",
                "reason": "This block depends on the output of a preceding block. A reschedule file has been generated.",
                "dependencies": block_keys,
                "reschedule_file": filename  # Add the new key
            }
            continue # Move to the next unschedulable block
        
        # --- If we get here, the block has no preceding dependencies. We can try to plan it. ---
        print("  -> No preceding dependencies. Attempting parallelization plan.")

        match = re.match(r"[\w\s,.]+\s*=\s*([\w.]+)\((.*)\)", statement)
        if not match:
            print(f"  -> Could not parse function and args from statement. Skipping.")
            parallelization_plan[statement] = {
                "status": "Failed",
                "reason": "Could not parse statement.",
            }
            continue

        func_name_or_method, args_str = match.groups()
        arg_names = [arg.strip() for arg in args_str.split(",") if arg.strip()]

        found_key = next(
            (k for k in func_footprints_data if k.startswith(statement)), None
        )
        if not found_key:
            print(
                f"  -> Feasibility Check FAILED: No function footprint data found for '{statement}'."
            )
            parallelization_plan[statement] = {
                "status": "Failed: Infeasible",
                "reason": "No footprint data found to reconstruct source.",
            }
            continue

        lines_from_footprints = [
            line
            for line in func_footprints_data[found_key].keys()
            if line != "aggregation"
        ]
        source_code = reconstruct_source_with_indentation(lines_from_footprints)

        if is_infeasible_due_to_nested_loops(source_code, arg_names):
            parallelization_plan[statement] = {
                "status": "Failed: Infeasible",
                "reason": "Function contains an argument in a nested loop iterable.",
            }
            continue

        print("  -> Feasibility Check PASSED.")

        # --- The rest of the calculation logic proceeds as before ---
        live_vars_at_line = live_vars_data.get(statement, {})
        # ... (and so on) ...

        sum_of_args_size = sum(
            live_vars_at_line[arg]["size"]
            for arg in arg_names
            if arg in live_vars_at_line
        )
        mem_values = [
            v
            for v in func_footprints_data[found_key].values()
            if isinstance(v, (int, float))
        ]
        func_execution_size = mem_values[-1] if mem_values else 0
        total_required_mem = sum_of_args_size + func_execution_size
        num_chunks = math.ceil(total_required_mem / smallest_node_memory)

        if num_chunks <= 1:
            num_chunks = 2

        print(
            f"  -> Required Memory: {total_required_mem}, Smallest Node: {smallest_node_memory} -> Parallelization Factor: {num_chunks}"
        )

        # --- REVISED LOGIC: Define Chunks For Each Argument ---
        chunks_per_argument = {}
        # Iterate through each argument that was parsed from the function call
        for arg_name in arg_names:
            arg_info = live_vars_at_line.get(arg_name)

            # Only create chunks for arguments that have a 'length' attribute
            if not arg_info or "length" not in arg_info:
                print(
                    f"  -> Argument '{arg_name}' is not parallelizable (no length data). Skipping."
                )
                continue

            total_length = arg_info["length"]

            # Cannot create more chunks than there are items in this specific argument
            effective_num_chunks = min(
                num_chunks, total_length if total_length > 0 else 1
            )
            if effective_num_chunks == 0 and total_length > 0:
                effective_num_chunks = 1
            if effective_num_chunks == 0:
                continue

            # Calculate the chunk size specifically for this argument
            chunk_size = math.ceil(total_length / effective_num_chunks)

            arg_chunks_list = []
            for i in range(effective_num_chunks):
                start_index = i * chunk_size
                end_index = min((i + 1) * chunk_size, total_length)
                arg_chunks_list.append(
                    {"chunk_id": i, "start_index": start_index, "end_index": end_index}
                )
                if end_index >= total_length:
                    break  # Stop if we've covered the entire length of this argument

            chunks_per_argument[arg_name] = arg_chunks_list
            print(
                f"  -> Defined {len(arg_chunks_list)} chunks for argument '{arg_name}' (length: {total_length})."
            )

        parallelization_plan[statement] = {
            "status": "Success",
            "parallelization_factor": num_chunks,  # The overall target number of parallel jobs
            "chunks": chunks_per_argument,  # The new, detailed chunking dictionary
        }

    return parallelization_plan


# ==============================================================================
# 5. FINAL EXECUTION PLAN GENERATION (COMPLETE AND CORRECTED)
# ==============================================================================
def build_function_definitions(func_footprints_data):
    """
    Parses the function footprints to build a dictionary of unique,
    reconstructed Python function source code.
    """
    all_functions_code = {}

    # Use a set to track processed function names to avoid duplicates
    processed_function_names = set()

    for key, footprint in func_footprints_data.items():
        # The first line of the footprint is the 'def' statement
        def_line = list(footprint.keys())[0]
        match = re.match(r"def\s+(\w+)\s*\(", def_line)
        if not match:
            continue  # Not a valid function definition start

        func_name = match.groups()[0]
        if func_name in processed_function_names:
            continue  # We've already reconstructed this function

        print(f"Reconstructing function: {func_name}")
        lines_from_footprints = [
            line for line in footprint.keys() if line != "aggregation"
        ]
        reconstructed_code = reconstruct_source_with_indentation(lines_from_footprints)
        all_functions_code[func_name] = reconstructed_code
        processed_function_names.add(func_name)

    return all_functions_code


# Place this helper function before `generate_execution_plan` if you haven't already.
def sanitize_statement_for_filename(statement: str) -> str:
    """Converts a Python statement into a safe string for a filename."""
    sanitized = re.sub(r'[=\s(),.]+', '_', statement)
    sanitized = re.sub(r'_+', '_', sanitized)
    return sanitized.strip('_')


# ==============================================================================
# 5. FINAL EXECUTION PLAN GENERATION (WITH DATA SPLITTING)
# ==============================================================================
async def generate_execution_plan(
    job_id: int,
    node_map: dict, # Pass in the map of {node_name: node_id}
    consolidated_schedule_info,
    parallelization_plan,
    nodes_data,
    live_vars_data,
    func_footprints_data,
    input_header,
    input_data_rows
):
    """
    Generates a master schedule, executable scripts, AND creates data/task
    records in the database.
    """
    print("\n--- Generating Final Execution Plan (DB Integration & Scripting) ---")
    # --- Phase 0: Setup ---
    if not nodes_data:
        return

    get_lhs_var = lambda s: s.split("=")[0].strip()
    
    # This map is crucial for finding the provider node ID for intermediate data
    block_to_node_map = {
        info["consolidated_block_index"]: info["assigned_node"]["name"]
        for info in consolidated_schedule_info
        if info["is_schedulable"]
    }
    
    # This dictionary will store all created data objects from the DB,
    # mapping the variable name to the data object (which includes the ID).
    db_data_map = {}

    # --- DB PASS 1: Identify and create all data records ---
    print("\n--- DB Pass 1: Creating Data Records ---")
    for info in consolidated_schedule_info:
        if not info["is_schedulable"]:
            continue

        # 1a. Identify initial inputs for this block (dependencies on "none")
        for key in info.get("key", []):
            if key == "none:none":
                continue
            if key.endswith(":none"):

                var_name = key.split(":")[0]
                # Only create if we haven't seen this initial input before
                if var_name not in db_data_map:
                    print(f"  Creating DB record for initial input: '{var_name}' (file: {input_file})")
                    # The main program's input file is considered the provider with ID -1
                    data_service = get_data_service(global_session)
                    db_data_obj = await data_service.create_data(file_name=input_file, job_id=job_id, provider_id=-1)
                    print("HERE55555555555555555",db_data_obj.data_id)
                    db_data_map[var_name] = db_data_obj.data_id

        # # 1b. Identify intermediate outputs produced by this block
        # for stmt in info["statements"]:
        #     output_var = get_lhs_var(stmt)
        #     # Only create if this output variable hasn't been defined yet
        #     if output_var not in db_data_map:
        #         producer_node_name = info["assigned_node"]["name"]
        #         producer_node_id = node_map.get(producer_node_name)
        #         print(f"  Creating DB record for intermediate output: '{output_var}' (provider: {producer_node_name} ID: {producer_node_id})")
                
        #         # The filename for an intermediate output is just its variable name for now
        #         db_data_obj = await data_service.create_data(file_name=output_var, job_id=job_id, provider_id=producer_node_id)
        #         db_data_map[output_var] = db_data_obj.data_id

    # --- DB PASS 2: Create Tasks & Generate Scripts ---
    print("\n--- DB Pass 2: Creating Task Records & Generating Scripts ---")
    
    # Setup for script generation
    AGGREGATOR_NAME = "AGGREGATOR_SERVICE"
    WORKER_NODES = [node["name"] for node in nodes_data]
    node_plans = {node["name"]: {"initial_data": {}, "python_code": []} for node in nodes_data}
    master_plan = ["# Master Execution Schedule\n"]
    print("HERE2222222222")
    function_definitions = build_function_definitions(func_footprints_data)
    print("HERE111111111111111111111111111")
    
    for info in consolidated_schedule_info:
        block_idx = info["consolidated_block_index"]

        # --- CASE A: The block is schedulable on a worker node ---
        if info["is_schedulable"]:
            node_name = info["assigned_node"]["name"]
            assigned_node_id = node_map.get(node_name)
            # --- DB Task Creation ---
            
            required_data_ids = []
            for key in info.get("key", []):
                var_name = key.split(":")[0]
                if var_name != "none" and var_name in db_data_map:
                    print("var_name",var_name)
                    print("manga",db_data_map[var_name])
                    required_data_ids.append(db_data_map[var_name])
                    print("HERE444444444444444444")
            
            # Remove duplicates that might arise from multiple statements needing the same input
            required_data_ids = list(set(required_data_ids))
            print(f"\nCreating DB Task for Block {block_idx} on Node '{node_name}' (ID: {assigned_node_id})")
            print(f"  Inputs require Data IDs: {required_data_ids}")
            task_service = get_task_service(global_session)
            print("HERE333333333333333333333333333")
            print("required_data_ids",required_data_ids)
            print("job_id",job_id)
            print("assigned_node_id",assigned_node_id)
            await task_service.create_task(
                job_id=job_id,
                data_ids=required_data_ids,
                required_ram=int(info["peak_memory"]), # Ensure RAM is int
                node_id=assigned_node_id
            )

            # --- Script Generation (Code is the same as before) ---
            master_plan.append(f"\n--- BLOCK {block_idx} (On {node_name}) ---")
            plan = node_plans[node_name]
            plan["python_code"].append(f"\n    # --- Task: Execute Block {block_idx} ---")
            valid_keys = [k for k in info.get("key", []) if k != "none:none"]
            for key in valid_keys:
                var, source_idx_str = key.split(":")
                if source_idx_str.isdigit():
                    source_idx_0_based = int(source_idx_str) - 1
                    source_node = block_to_node_map.get(source_idx_0_based, AGGREGATOR_NAME)
                    plan["python_code"].append(f"    print('--- Loading dependency ---')")
                    plan["python_code"].append(f"    {var} = wait_for_data('{var}', from_node='{source_node}')")
                elif source_idx_str == "none" and info["statements"]:
                    stmt = info["statements"][0]
                    var_info = live_vars_data.get(stmt, {}).get(var)
                    plan["initial_data"][var] = var_info
                    plan["python_code"].append(f"    print('--- Loading initial data ---')")
                    plan["python_code"].append(f"    {var} = initial_data['{var}']")

            for stmt in info["statements"]:
                output_var = get_lhs_var(stmt)
                plan["python_code"].append(f"    print(f'EXECUTING: {stmt}')")
                plan["python_code"].append(f"    {stmt}")
            master_plan.append(f"  - {node_name} executes {len(info['statements'])} statement(s).")

        # --- CASE B: The block is unschedulable ---
        else:
            if info["statements"]:
                statement = info["statements"][0]
                plan_result = parallelization_plan.get(statement, {})
                status = plan_result.get("status", "Unplanned")
                print(f"\nSkipping DB creation for unschedulable block: '{statement}' (Status: {status})")
                # All script/file generation for parallel/deferred tasks happens here as before
                # (This logic is left as-is, since you specified to ignore level 3 for DB integration)
                # ... (Place existing logic for handling parallelization_plan here)
                # ...

    # --- Phase 3: Write all generated files ---
    print("\n--- Phase 3: Writing Execution Files ---")
    python_harness_preamble = """
import json
import time
import csv

def wait_for_data(variable_name, from_node):
    print(f"--> [WAIT] Waiting for '{variable_name}' from {from_node}...")
    time.sleep(1)
    print(f"<-- [RECV] Received '{variable_name}'.")
    return f"data_for_{variable_name}"

def send_data(variable_name, data, consumers):
    print(f"--> [SEND] Sending '{variable_name}' to {consumers}...")
    time.sleep(0.5)
    print(f"<-- [SENT] '{variable_name}'.")
"""

    all_functions_source = "\n\n".join(function_definitions.values())

    try:
        with open("master_schedule.txt", "w") as f:
            f.write("\n".join(master_plan))
        print("Master schedule written to master_schedule.txt")

        for node_name, plan_details in node_plans.items():
            if not plan_details["python_code"]:
                continue

            with open(f"{node_name}_data.json", "w") as f:
                json.dump(plan_details["initial_data"], f, indent=4)
            print(f"Initial data for {node_name} written to {node_name}_data.json")

            scheduled_tasks_code = "\n".join(plan_details["python_code"])

            main_logic = f"""
def main(node_name):
    print(f"\\n*** Starting execution on {{node_name}} ***\\n")
    try:
        with open(f'{{node_name}}_data.json', 'r') as f:
            initial_data = json.load(f)
        print("--- Loaded initial data. ---")
    except FileNotFoundError:
        initial_data = {{}}
        print("--- No initial data file found. ---")
    
    # --- SCHEDULED TASKS ---
{scheduled_tasks_code}
    
    print(f"\\n*** Execution on {{node_name}} complete. ***\\n")

if __name__ == "__main__":
    main("{node_name}")
"""
            full_python_code = (
                python_harness_preamble
                + "\n\n# ==================================================\n"
                + "#          RECONSTRUCTED FUNCTION DEFINITIONS          \n"
                + "# ==================================================\n\n"
                + all_functions_source
                + "\n\n# ==================================================\n"
                + "#               MAIN EXECUTION LOGIC                 \n"
                + "# ==================================================\n"
                + main_logic
            )

            with open(f"{node_name}.py", "w") as f:
                f.write(full_python_code)
            print(f"Executable script for {node_name} written to {node_name}.py")

    except IOError as e:
        print(f"Error writing execution plan files: {e}")
# ==============================================================================
# 6. MAIN EXECUTION BLOCK
# ==============================================================================
async def scheduler(job_id: int):
        node_service = get_node_service(global_session)
        nodes = await node_service.get_all_nodes()
        nodes_data, node_map = convert_nodes_into_Json(nodes)
        print("Scheduler using nodes:", nodes_data)
        # --- Load all job-specific files ---
        try:
            job_dir = os.path.join(JOBS_DIRECTORY_PATH, str(job_id))
            with open(os.path.join(job_dir, 'main_lists.json'), 'r') as f: initial_blocks = json.load(f)
            with open(os.path.join(job_dir, 'main_lines_footprint.json'), 'r') as f: live_vars_data = json.load(f)
            with open(os.path.join(job_dir, 'func_lines_footprint.json'), 'r') as f: func_footprints_data = json.load(f)
        except FileNotFoundError as e:
            print(f"Error: Could not find required input file: {e.filename}")
            return
        
        # --- Load primary input CSV data ---
        input_header = None
        input_data_rows = None
        if input_file: # `input_file` is the global variable you defined
            try:
                primary_input_path = os.path.join(JOBS_DIRECTORY_PATH, str(job_id), input_file)
                with open(primary_input_path, 'r', newline='') as f:
                    reader = csv.reader(f)
                    primary_input_data = list(reader)

                if len(primary_input_data) > 1:
                    input_header = primary_input_data[0]
                    input_data_rows = primary_input_data[1:]
                    print(f"Successfully loaded primary input data from '{primary_input_path}' ({len(input_data_rows)} data rows).")
                else:
                    print(f"Warning: Input CSV file '{primary_input_path}' must have a header and at least one data row.")
            except FileNotFoundError:
                print(f"Error: Primary input CSV file not found at '{primary_input_path}'")
            except Exception as e:
                print(f"An error occurred while reading the CSV file: {e}")

        # --- Prepare statement-to-index map ---
        full_program_statements = [
            stmt for block in initial_blocks for stmt in block["statements"]
        ]
        stmt_to_original_idx_map = {
            stmt: i
            for i, block in enumerate(initial_blocks)
            for stmt in block["statements"]
        }
        
        # --- Execute the scheduling workflow (Levels 1, 2, 3) ---
        consolidated_schedule_info = schedule_program_whole(
            initial_blocks,
            full_program_statements,
            nodes_data,
            live_vars_data,
            func_footprints_data,
            stmt_to_original_idx_map,
        )

        final_blocks = []
        scheduling_info = []
        consolidated_schedule = []
        parallelization_plan = {}

        if consolidated_schedule_info is None:
            # Run merging and parallelization if the whole program doesn't fit
            final_blocks, scheduling_info = process_and_merge_blocks(initial_blocks, nodes_data, func_footprints_data, live_vars_data)
            consolidated_schedule, consolidated_schedule_info = consolidate_to_block_format(final_blocks, scheduling_info, nodes_data, live_vars_data, func_footprints_data, stmt_to_original_idx_map)
            unschedulable_final_blocks = [info for info in consolidated_schedule_info if not info["is_schedulable"]]
            parallelization_plan = plan_data_parallelization(unschedulable_final_blocks, nodes_data, live_vars_data, func_footprints_data)
        else:
            consolidated_schedule = [{"key": info["key"], "statements": info["statements"]} for info in consolidated_schedule_info]

        # --- FINAL STEP: GENERATE EXECUTION PLAN WITH DB INTEGRATION ---
        await generate_execution_plan(
            job_id=job_id,
            node_map=node_map,
            consolidated_schedule_info=consolidated_schedule_info,
            parallelization_plan=parallelization_plan,
            nodes_data=nodes_data,
            live_vars_data=live_vars_data,
            func_footprints_data=func_footprints_data,
            input_header=input_header,
            input_data_rows=input_data_rows
        )

        # --- FINAL OUTPUTS (for debugging) ---
        with open("final.json", "w") as f:
            json.dump(final_blocks, f, indent=4)
        with open("blocks.json", "w") as f:
            json.dump(scheduling_info, f, indent=4)
        with open("final_schedule_info.json", "w") as f:
            json.dump(consolidated_schedule_info, f, indent=4)
        with open("consolidated_schedule.json", "w") as f:
            json.dump(consolidated_schedule, f, indent=4)
        with open("parallelization_plan.json", "w") as f:
            json.dump(parallelization_plan, f, indent=4)

        print(
            f"\nFinal consolidated schedule has been written to 'final_schedule_info.json'"
        )
        print("Data parallelization plan has been written to 'parallelization_plan.json'")
        print("Executable node scripts and master plan have been generated.")



async def main():
    global global_session
    while True:
        async with get_db_context() as session:
            global_session = session
            try:
                job_service = get_job_service(session)
                jobs = await job_service.get_jobs_not_scheduled()
                print("Jobs:", jobs)
                print(f"Found {len(jobs)} jobs to schedule")
                for job in jobs:
                    ## update job status to running after scheduler is done
                    await job_service.update_job_status(job.job_id, JobStatus.running)
                    await scheduler(job.job_id)
            except Exception as e:
                print(f"Error: {e}")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
    
