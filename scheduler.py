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
from app.db.session import  get_db_context, init_db
from app.db.models.scheme import JobStatus
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
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

# The only function you need to change or replace in your file.
def reconstruct_source_with_indentation(lines_of_code):
    """
    Reconstructs a Python source string with plausible indentation.
    Handles multi-line strings correctly and maintains internal block indentation.
    """
    reconstructed_code = []
    indent_level = 0
    DEDENT_KEYWORDS = ("elif", "else:", "except", "finally")

    # First, flatten the keys into lines while preserving block indentation
    actual_lines = []
    for key in lines_of_code:
        # Split into lines and preserve internal indentation
        block_lines = key.split("\n")
        actual_lines.append(block_lines)

    for block in actual_lines:
        for i, line in enumerate(block):
            stripped_line = line.lstrip()
            if not stripped_line:
                continue  # Skip empty lines

            # Apply dedent before this line if needed
            if stripped_line.startswith(DEDENT_KEYWORDS):
                indent_level = max(0, indent_level - 1)

            # Compute the internal indent (relative to first line in block)
            internal_indent = len(line) - len(stripped_line)
            final_indent = ("    " * indent_level) + (" " * internal_indent)
            reconstructed_code.append(final_indent + stripped_line)

        # If the last line in the block ends with ":", indent the next block
        if block[-1].strip().endswith(":"):
            indent_level += 1

    return "\n".join(reconstructed_code)


# This function now works correctly because it uses the fixed helper.
def build_function_definitions(func_footprints_data):
    """
    Parses the function footprints to build a dictionary of unique,
    reconstructed Python function source code.
    """
    all_functions_code = {}
    processed_function_names = set()

    for key, footprint in func_footprints_data.items():
        def_line = list(footprint.keys())[0]
        match = re.match(r"def\s+(\w+)\s*\(", def_line)
        if not match:
            continue
        
        func_name = match.groups()[0]
        if func_name in processed_function_names:
            continue
            
        print(f"Reconstructing function: {func_name}")
        # Get all the code lines from the footprint, excluding the "aggregation" key.
        lines_from_footprints = [line for line in footprint.keys() if line != "aggregation"]
        
        # Use the new, corrected reconstruction function.
        reconstructed_code = reconstruct_source_with_indentation(lines_from_footprints)
        
        all_functions_code[func_name] = reconstructed_code
        processed_function_names.add(func_name)
        
    return all_functions_code

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
async def generate_execution_plan(
    job_id: int,
    job_dir: str, # <-- NEW ARGUMENT for pathing
    node_map: dict,
    consolidated_schedule_info,
    parallelization_plan,
    nodes_data,
    live_vars_data,
    func_footprints_data,
    input_header,
    input_data_rows,
    session: AsyncSession  # <-- NEW ARGUMENT for database session
):
    """
    Generates all artifacts for a job inside the specified job directory.
    Integrates the real wait_for_data logic into the generated scripts.
    """
    print("\n--- Generating Final Execution Plan (DB Integration & Scripting) ---")

    # --- Phase 0: Setup ---
    if not nodes_data: return
    get_lhs_var = lambda s: s.split("=")[0].strip()
    block_to_node_map = { info["consolidated_block_index"]: info["assigned_node"]["name"] for info in consolidated_schedule_info if info["is_schedulable"] }
    var_consumers = {}
    for info in consolidated_schedule_info:
        valid_keys = [k for k in info.get("key", []) if k != "none:none"]
        for key in valid_keys:
            var, source_idx_str = key.split(":")
            if source_idx_str.isdigit():
                source_idx_0_based = int(source_idx_str) - 1
                producer_block = next((p for p in consolidated_schedule_info if p["consolidated_block_index"] == source_idx_0_based), None)
                if producer_block:
                    for stmt in producer_block.get("statements", []):
                        produced_var = get_lhs_var(stmt)
                        if produced_var == var:
                            if produced_var not in var_consumers: var_consumers[produced_var] = set()
                            consuming_node = block_to_node_map.get(info["consolidated_block_index"])
                            if consuming_node: var_consumers[produced_var].add(consuming_node)
                            break
    produced_by_scheduled_blocks = { get_lhs_var(stmt) for info in consolidated_schedule_info if info["is_schedulable"] for stmt in info["statements"] }
    var_to_data_id_map = {}

    # --- Phase 1: Create Initial Input Data Records ---
    print("\n--- Creating Initial Data Records ---")
    all_initial_vars = {key.split(":")[0] for block in consolidated_schedule_info for key in block.get("key", []) if key.endswith(":none") and key.split(":")[0] != "none"}
    data_service = get_data_service(session)
    for var_name in all_initial_vars:
        print(f"  Creating DB record for initial input: '{var_name}'")
        # Correctly pass the main input filename to the DB record
        db_data_obj = await data_service.create_data(file_name=input_file, job_id=job_id, parent_task_id=None)
        var_to_data_id_map[var_name] = db_data_obj.data_id
        
    # --- Phase 2: Sequential Task/Data Creation and Script Generation ---
    print("\n--- Creating Tasks, Output Data, and Generating Scripts ---")
    node_plans = {node["name"]: {"initial_data": {}, "python_code": []} for node in nodes_data}
    master_plan = ["# Master Execution Schedule\n"]
    function_definitions = build_function_definitions(func_footprints_data)
    last_schedulable_block_idx = max((info["consolidated_block_index"] for info in consolidated_schedule_info if info["is_schedulable"]), default=-1)

    for info in consolidated_schedule_info:
        block_idx = info["consolidated_block_index"]
        valid_keys = [k for k in info.get("key", []) if k != "none:none"]

        if info["is_schedulable"]:
            node_name, assigned_node_id = info["assigned_node"]["name"], node_map.get(info["assigned_node"]["name"])
            required_data_ids = list(set([var_to_data_id_map[key.split(":")[0]] for key in valid_keys if key.split(":")[0] in var_to_data_id_map]))
            
            print(f"\nCreating DB Task for Block {block_idx} on Node '{node_name}'")
            task_service = get_task_service(session)
            created_task = await task_service.create_task(job_id=job_id, data_ids=required_data_ids, required_ram=int(info["peak_memory"]), node_id=assigned_node_id)
            print(f"  -> Created Task with ID: {created_task.task_id}")

            master_plan.append(f"\n--- BLOCK {block_idx} (On {node_name}) ---")
            plan = node_plans[node_name]
            plan["python_code"].append(f"\n    # --- Task {created_task.task_id}: Execute Block {block_idx} ---")

            initial_vars_for_block = {key.split(":")[0] for key in valid_keys if key.endswith(":none") and key.split(":")[0] != "none"}
            if initial_vars_for_block and input_header is not None:
                block_input_filename = f"task_{created_task.task_id}_input.csv"
                block_input_filepath = os.path.join(job_dir, block_input_filename)
                print(f"  -> Writing initial data for this task to '{block_input_filepath}'")
                with open(block_input_filepath, 'w', newline='') as f:
                    csv.writer(f).writerows([input_header] + input_data_rows)
                plan['initial_data']['primary_input'] = block_input_filename
                plan["python_code"].append(f"    primary_data = wait_for_data('{block_input_filename}', '{job_id}')")
                for var in initial_vars_for_block:
                    plan["python_code"].append(f"    {var} = primary_data")

            for key in valid_keys:
                if key.split(":")[1].isdigit():
                    var = key.split(":")[0]
                    plan["python_code"].append(f"    {var} = wait_for_data('{var}.csv', '{job_id}')")

            for stmt in info["statements"]:
                output_var = get_lhs_var(stmt)
                plan["python_code"].append(f"    print(f'EXECUTING: {stmt}')")
                plan["python_code"].append(f"    {stmt}")
                if output_var in var_consumers:
                    output_filename = f"{output_var}.csv"
                    output_filepath = os.path.join(job_dir, output_filename)
                    if output_var not in var_to_data_id_map:
                        db_data_obj = await data_service.create_data(file_name=output_filename, job_id=job_id, parent_task_id=created_task.task_id)
                        var_to_data_id_map[output_var] = db_data_obj.data_id
                    plan["python_code"].append(f"    with open('{output_filepath}', 'w', newline='') as f: csv.writer(f).writerows({output_var})")
                    plan["python_code"].append(f"    send_data('{output_var}', consumers={list(var_consumers[output_var])})")
            
            if block_idx == last_schedulable_block_idx:
                final_output_var = get_lhs_var(info["statements"][-1])
                final_output_filepath = os.path.join("output.csv")
                plan["python_code"].append(f"    print(f'--- SAVING FINAL RESULT to {final_output_filepath} ---')")
                plan["python_code"].append(f"    with open('{final_output_filepath}', 'w', newline='') as f: csv.writer(f).writerows({final_output_var})")
        
        else: # CASE B: The block is unschedulable
            if not info["statements"]: continue
            statement = info["statements"][0]
            plan_result = parallelization_plan.get(statement, {})
            
            if not plan_result or plan_result.get("status") != "Success":
                status = plan_result.get("status", "Unplanned")
                master_plan.append(f"\n--- Task '{statement}' (UNPLANNED) ---")
                master_plan.append(f"  - Status: {status}")
            else:
                if input_header is None or input_data_rows is None:
                    master_plan.append(f"\n--- Task '{statement}' (UNPLANNED) ---")
                    master_plan.append(f"  - Status: Failed - Missing primary input data for splitting.")
                    continue
                
                master_plan.append(f"\n--- Task '{statement}' (PARALLEL) ---")
                output_var = get_lhs_var(statement)
                arg_names_match = re.match(r".*?\((.*)\)", statement)
                arg_names = [arg.strip() for arg in arg_names_match.groups()[0].split(',') if arg.strip()] if arg_names_match else []
                func_name_match = re.search(r"=\s*([\w.]+)\(", statement)
                func_name = func_name_match.groups()[0] if func_name_match else "unknown_function"
                parallel_arg_name = next(iter(plan_result.get('chunks', {})), None)
                
                if not parallel_arg_name:
                    master_plan.append(f"  - Status: Failed - No chunkable argument in plan.")
                    continue

                master_plan.append(f"  - Aggregator: AGGREGATOR_SERVICE")
                worker_chunk_assignments = {}
                all_chunk_ids, WORKER_NODES = list(range(plan_result.get("parallelization_factor", 0))), [node["name"] for node in nodes_data]
                for i, chunk_id in enumerate(all_chunk_ids):
                    worker_name = WORKER_NODES[i % len(WORKER_NODES)]
                    worker_chunk_assignments.setdefault(worker_name, []).append(chunk_id)

                for worker_name, assigned_chunks in worker_chunk_assignments.items():
                    worker_plan = node_plans[worker_name]
                    worker_plan["python_code"].append(f"\n    # --- PARALLEL TASK for '{statement}' ---")

                    for chunk_id in assigned_chunks:
                        chunk_info = plan_result['chunks'][parallel_arg_name][chunk_id]
                        start, end = chunk_info['start_index'], chunk_info['end_index']
                        chunk_data = [input_header] + input_data_rows[start:end]
                        chunk_filename = f"{parallel_arg_name}_chunk_{chunk_id}_for_{worker_name}.csv"
                        
                        with open(chunk_filename, 'w', newline='') as f: csv.writer(f).writerows(chunk_data)
                        master_plan.append(f"  - Worker {worker_name} assigned data chunk: {chunk_filename}")
                            
                        chunk_key = f"chunk_file_{sanitize_statement_for_filename(statement)}_{chunk_id}"
                        worker_plan['initial_data'][chunk_key] = chunk_filename
                        
                        worker_plan['python_code'].append(f"\n    # -- Sub-Task for Chunk {chunk_id} --")
                        worker_plan['python_code'].append(f"    with open(initial_data['{chunk_key}'], 'r', newline='') as f: {parallel_arg_name}_chunk_data = list(csv.reader(f))")
                        
                        # --- THIS IS THE CORRECTED AND REWRITTEN ARGUMENT HANDLING LOGIC ---
                        args_for_call = {}
                        for arg in arg_names:
                            if arg == parallel_arg_name:
                                args_for_call[arg] = f"{parallel_arg_name}_chunk_data"
                            elif arg not in produced_by_scheduled_blocks:
                                print(f"  -> Parallel task needs primary input '{arg}'. Adding to manifest.")
                                worker_plan["initial_data"][arg] = live_vars_data.get(statement, {}).get(arg, f"MISSING_DATA_FOR_{arg}")
                                worker_plan["python_code"].append(f"    {arg} = initial_data.get('{arg}')")
                                args_for_call[arg] = arg
                            else: # Argument is an intermediate result from a previous block
                                producer_node = "UNKNOWN"
                                for k in valid_keys:
                                    if k.startswith(arg + ":"):
                                        producer_node = block_to_node_map.get(int(k.split(":")[1]) - 1, "AGGREGATOR_SERVICE")
                                        break
                                worker_plan["python_code"].append(f"    {arg} = wait_for_data('{arg}', from_node='{producer_node}')")
                                args_for_call[arg] = arg
                        
                        call_str = ", ".join([f"{k}={v}" for k, v in args_for_call.items()])
                        worker_plan['python_code'].append(f"    partial_result_{chunk_id} = {func_name}({call_str})")
                        worker_plan['python_code'].append(f"    send_data('partial_result_for_{output_var}', partial_result_{chunk_id}, consumers=['AGGREGATOR_SERVICE'])")

                for worker_name in worker_chunk_assignments.keys():
                    node_plans[worker_name]["python_code"].extend([
                        f"\n    # --- SYNCHRONIZATION BARRIER ---",
                        f"    {output_var} = wait_for_data('{output_var}', from_node='AGGREGATOR_SERVICE')"
                    ])
                master_plan.append(f"  - All workers ({', '.join(worker_chunk_assignments.keys())}) will WAIT for final result '{output_var}'.")

    # --- Phase 3: Write all generated files ---
    print("\n--- Phase 3: Writing Execution Files ---")
    
    # Correctly integrate your new wait_for_data and other helpers
    python_harness_preamble = f"""
import json
import time
import csv
import os

# --- This job's specific directory ---
JOB_ID = "{job_id}"
JOBS_DIRECTORY_PATH = "{JOBS_DIRECTORY_PATH}"
JOB_DIR = os.path.join(JOBS_DIRECTORY_PATH, str(JOB_ID))

def infer_type(value):
    try: return int(value)
    except (ValueError, TypeError):
        try: return float(value)
        except (ValueError, TypeError): return str(value).strip()

def wait_for_data(file_name, job_id_str):
    file_path = os.path.join(file_name)
    print(f"--> [WAIT] Waiting for '{{file_path}}'...")
    while not os.path.exists(file_path):
        time.sleep(2)
    print(f"<-- [RECV] Received '{{file_path}}'.")
    try:
        with open(file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            # Applying infer_type to every cell
            return [[infer_type(cell) for cell in row] for row in reader]
    except Exception as e:
        print(f"[ERROR] Failed to load file '{{file_name}}': {{e}}")
        return None

def send_data(variable_name, consumers):
    # This is now just a notification stub
    print(f"--> [SEND] Notifying {{consumers}} that '{{variable_name}}' is ready.")
    time.sleep(0.5)
    print(f"<-- [SENT] Notification for '{{variable_name}}'.")
"""
    all_functions_source = "\n\n".join(function_definitions.values())

    try:
        master_schedule_path = os.path.join(job_dir, "master_schedule.txt")
        with open(master_schedule_path, "w") as f: f.write("\n".join(master_plan))
        print(f"Master schedule written to {master_schedule_path}")

        for node_name, plan_details in node_plans.items():
            if not plan_details["python_code"]: continue
            
            node_manifest_path = os.path.join(job_dir, f"{node_name}_data.json")
            with open(node_manifest_path, "w") as f: json.dump(plan_details["initial_data"], f, indent=4)
            print(f"Initial data manifest for {node_name} written to {node_manifest_path}")
            
            scheduled_tasks_code = "\n".join(plan_details["python_code"])
            main_logic = f"""
def main(node_name):
    print(f"\\n*** Starting execution on {{node_name}} ***\\n")
    # The manifest file is now inside the job directory
    manifest_path = os.path.join(JOB_DIR, f'{{node_name}}_data.json')
    try:
        with open(manifest_path, 'r') as f:
            initial_data = json.load(f)
        print("--- Loaded initial data manifest. ---")
    except FileNotFoundError:
        initial_data = {{}}
        print("--- No initial data manifest found. ---")
    
    # --- SCHEDULED TASKS ---
{scheduled_tasks_code}
    
    print(f"\\n*** Execution on {{node_name}} complete. ***\\n")

if __name__ == "__main__":
    main("{node_name}")
"""
            full_python_code = (python_harness_preamble + "\n\n# RECONSTRUCTED FUNCTION DEFINITIONS\n" + all_functions_source + "\n\n# MAIN EXECUTION LOGIC\n" + main_logic)
            
            script_path = os.path.join(job_dir, f"{node_name}.py")
            with open(script_path, "w") as f: f.write(full_python_code)
            print(f"Executable script for {node_name} written to {script_path}")

    except IOError as e:
        print(f"Error writing execution plan files: {e}")

# ==============================================================================
# 6. MAIN EXECUTION BLOCK
# ==============================================================================
async def scheduler(job_id: int, session: AsyncSession):
        node_service = get_node_service(session)
        nodes = await node_service.get_all_nodes()
        nodes_data, node_map = convert_nodes_into_Json(nodes)
        job_dir = os.path.join(JOBS_DIRECTORY_PATH, str(job_id))

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
        
        input_header = None
        input_data_rows = None
        if input_file:
            try:
                primary_input_path = os.path.join(job_dir, input_file)
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
                return # Stop if the main input is missing

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
            job_dir = job_dir,
            node_map=node_map,
            consolidated_schedule_info=consolidated_schedule_info,
            parallelization_plan=parallelization_plan,
            nodes_data=nodes_data,
            live_vars_data=live_vars_data,
            func_footprints_data=func_footprints_data,
            input_header=input_header,
            input_data_rows=input_data_rows,
            session=session
        )

        # --- FINAL OUTPUTS (for debugging) ---
        # with open("final.json", "w") as f:
        #     json.dump({job_dir + final_blocks}, f, indent=4)
        # with open("blocks.json", "w") as f:
        #     json.dump({job_dir + scheduling_info}, f, indent=4)
        # with open("final_schedule_info.json", "w") as f:
        #     json.dump({job_dir + consolidated_schedule_info}, f, indent=4)
        # with open("consolidated_schedule.json", "w") as f:
        #     json.dump({job_dir + consolidated_schedule}, f, indent=4)
        # with open("parallelization_plan.json", "w") as f:
        #     json.dump({job_dir + parallelization_plan}, f, indent=4)

        print(
            f"\nFinal consolidated schedule has been written to 'final_schedule_info.json'"
        )
        print("Data parallelization plan has been written to 'parallelization_plan.json'")
        print("Executable node scripts and master plan have been generated.")

async def main():
    await init_db()
    while True:
        async with get_db_context() as session:
            try:
                job_service = get_job_service(session)
                jobs = await job_service.get_jobs_not_scheduled()
                print("Jobs:", jobs)
                print(f"Found {len(jobs)} jobs to schedule")
                for job in jobs:
                    ## update job status to running after scheduler is done
                    await job_service.update_job_status(job.job_id, JobStatus.running)
                    await scheduler(job.job_id, session)
            except Exception as e:
                print(f"Error: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
    
