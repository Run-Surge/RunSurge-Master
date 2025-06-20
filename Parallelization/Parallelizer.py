import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Parallelization.Memory_Estimator import *
from Parallelization.DDG import *
import py_compile
import sys
from collections import defaultdict, deque
from typing import List, Dict, Any
import glob
import json
import copy
import re
from app.utils.constants import JOBS_DIRECTORY_PATH
from collections import defaultdict
from typing import List, Dict, Any


global SAVE_PATH

def group_by_needs_with_wait_index(
    statements: List[Dict[str, Any]],
    dependency_graph: Dict[str, Any]
) -> Dict[tuple, List[str]]:
    
    def get_dependency_dict(statements):
        line_to_stmt = {stmt["code line"]: stmt["statement"] for stmt in statements}
        grouped_statements = defaultdict(list)
        first_no_dependency_handled = False

        for stmt in statements:
            line_num = stmt["code line"]
            line_str = str(line_num)
            info = dependency_graph.get(line_str, {})
            depends_on = info.get("Depends on", [])

            if not depends_on:
                key = ("none:none",) if not first_no_dependency_handled else ("data:none",)
                key = None
                if first_no_dependency_handled:
                    key = ("none:none",)
                else:   
                    if line_to_stmt[line_num].startswith("def"):
                        key = ("args:none",)
                    else:
                        key = ("data:none",)
                    
                first_no_dependency_handled = True
                grouped_statements[key].append(line_to_stmt[line_num])
            else:
                # key_parts = [f"{dep['Dependency'][0]}:{dep['Node']}" for dep in depends_on]
                key_parts = [
                    f"{var}:{dep['Node']}"
                    for dep in depends_on
                    for var in dep['Dependency']
                ]
                key = tuple(sorted(key_parts))
                grouped_statements[key].append(line_to_stmt[line_num])

        # Convert to list of dicts
        result = [{"key": key, "statements": stmts} for key, stmts in grouped_statements.items()]
        return result  
    def get_line_to_statement_map(statements: List[Dict[str, Any]]) -> Dict[int, str]:
        return {stmt["code line"]: stmt["statement"] for stmt in statements}
    def convert_keys_to_dict_indices(grouped_list, line_to_code_mapping):
        def get_statement_index(grouped_list,code):
            for i, item in enumerate(grouped_list):
                if code in item["statements"]:
                    return i
            return None
        for ind,dictionnary in enumerate(grouped_list):
            keys = dictionnary["key"]
            keys = list(keys)
            new_keys = []
            for key in keys:
                lineno = key.split(":")
                new_key = None
                if lineno[1] == "none":
                    new_key = key
                else:
                    code_line = line_to_code_mapping.get(int(lineno[1]), None)
                    index = get_statement_index(grouped_list, code_line)
                    new_key = f'{lineno[0]}:{index}'
                new_keys.append(new_key) 
            new_key_tuple = tuple(new_keys)
            grouped_list[ind]["key"] = new_key_tuple
        return grouped_list
    result = get_dependency_dict(statements)
    line_to_code_mapping = get_line_to_statement_map(statements)
    result = convert_keys_to_dict_indices(result,  line_to_code_mapping)
    
    return result
   

def check_syntax_errors(file_path,error_file="errors.txt"):
    try:
        py_compile.compile(file_path, doraise=True)
        return True
    except py_compile.PyCompileError as e:
        print(f"Syntax error in {file_path}: {e.msg}")
        with open(error_file, 'a') as ef:
            ef.write(f"Syntax error in {file_path}: {e.msg}\n")
        return False
    
def build_ddg(file_path):
    try:
        with open(file_path, 'r') as f:
            code = f.read()

        # Remove all content between any two "#---------------------------------------------------------------------------------------------------------"   
        code = re.sub(
            r"#-+\n.*?\n#-+\n",
            "",
            code,
            flags=re.DOTALL
        )

        tree = ast.parse(code)
        tree = AugAssignToAssignTransformer().visit(tree)
        ast.fix_missing_locations(tree)
        graph=DDG_Wrapper(tree)
        graph.build_ddgs()
        return graph
    except Exception as e:
        print(f"Error building DDG for {file_path}: {e}")
        return None
def dependency_analyzer(folder):
    global SAVE_PATH

    jsons = sorted(glob.glob(f"{folder}/*.json"))
    results = []
    if len(jsons) % 2 != 0:
        raise ValueError("Expected an even number of JSON files (pairs of edges and nodes).")
    for i in range(0, len(jsons), 2):
        with open(jsons[i], 'r') as f1, open(jsons[i + 1], 'r') as f2:
            edges = json.load(f1)
            nodes = json.load(f2)

        print(f"\nProcessing pair: {os.path.basename(jsons[i])}, {os.path.basename(jsons[i+1])}")
        result = group_by_needs_with_wait_index(nodes, edges)
   
        index = int(re.search(r'\d+', os.path.basename(jsons[i])).group(0))
        print("DEBUGGIN ",f"{SAVE_PATH}/main_lists.json")
        if index == 0:
            json.dump(result, open(f"{SAVE_PATH}/main_lists.json", 'w'), indent=4)
        else:
            #! remove aggregation keys from the result
            for item in result:
                item["statements"] = [
                    stmt for stmt in item.get("statements", [])
                    if not stmt.strip().startswith("aggregation =")
                ]
            result = [item for item in result if item["statements"]]

            json.dump(result, open(f"{SAVE_PATH}/function{index}_lists.json", 'w'), indent=4)
        results.append(result)
    return results

def get_memory_foortprint(file_path, entry_point, functions,job_id):
    global SAVE_PATH
    def get_file_name(tree):
        file_name = None
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == 'FILE_NAME':
                        if isinstance(node.value, ast.Constant):
                            file_name = node.value.value
        file_name=f"{JOBS_DIRECTORY_PATH}/{job_id}/123456.csv"

        return file_name
    def get_read_file_block(tree):
        try_node = None
        for node in tree.body:
            if isinstance(node, ast.If):
                # match: if __name__ == '__main__':
                test = node.test
                if (
                    isinstance(test, ast.Compare)
                    and isinstance(test.left, ast.Name)
                    and test.left.id == '__name__'
                    and any(
                        isinstance(c, ast.Constant) and c.value == '__main__'
                        for c in test.comparators
                    )
                ):
                    # look for the Try in its body
                    for inner in node.body:
                        if isinstance(inner, ast.Try):
                            try_node = inner
                            break

        # 4. Unparse (get source for) that Try node
        if try_node is not None:
            try_src = ast.unparse(try_node)
            return try_src
        else:
            print("No try/except block found under __main__")
    def get_func_footprint(func_name, args, functions, func_lines_footprint, global_parser,main_code_line,lineno):
        def find_func_index(func_name, func_list):
            for idx, func_tuple in enumerate(func_list):
                if func_tuple[0] == func_name:
                    return idx
            return -1
        def get_footprint(node,local_parser,func_lines_footprint):
            tree = copy.deepcopy(node.body[0])  # Get the first statement in the function body
            # print(ast.dump(tree, indent=4))  # Debugging: print the AST nodes
            if isinstance(tree, ast.Assign):
                #! for x = len(var) case
                code = ast.unparse(tree)
                pattern = r"len\((\w+)\)"
                match = re.search(pattern, code)
                if match:
                    var_name = match.group(1)
                    if var_name in local_parser.vars:
                        length = local_parser.vars[var_name][0]
                        modified_code = re.sub(pattern, str(length), code)
                        print(f"Modified code: {modified_code}")  # Debugging: print the modified code
                        tree = ast.parse(modified_code).body[0]  
                    else:
                        raise ValueError(f"Variable {var_name} not found in local parser variables.")  
                #! for x = len(var[i])
                pattern = r"len\((\w+\[\d+\])\)" 
                match = re.search(pattern, code)
                if match:
                    full_index_expr = match.group(1)  # e.g., numeric_data[0]
                    var_name = full_index_expr.split('[')[0]  # Extract base variable name, e.g., numeric_data
                    if var_name in local_parser.vars:
                        length = str(local_parser.vars[var_name][0])  # Convert to string
                        # Replace only the matched part
                        modified_code = code[:match.start()] + str(length) + code[match.end():]
                        print(f"Modified code: {modified_code}")  # Debugging: print the modified code
                        tree = ast.parse(modified_code).body[0]
                    else:
                        raise ValueError(f"Variable {var_name} not found in local parser variables.")
                #! skip break and continue statements
                if (ast.unparse(tree).startswith('break') or ast.unparse(tree).startswith('continue')):
                    return
                local_parser._assignmemt_handler(tree)
            elif  isinstance(tree, ast.AugAssign):
                local_parser._insertion_handler(tree)
            elif isinstance(tree, ast.Expr):
                func = tree.value.func.attr 
                if func in ['insert', 'append', 'extend']:
                    local_parser._insertion_handler(tree)
                elif func in ['pop', 'remove','clear']:
                    local_parser._deletion_handler(tree)
            elif  isinstance(tree, ast.Delete):
                local_parser._deletion_handler(tree.body[0])
            elif isinstance(tree, ast.For):
                local_parser._handle_loop_footprint(tree)
            elif isinstance(tree, ast.If):
                local_parser._handle_if_footprint(tree)
            # print(ast.unparse(node))  # Debugging: print the source code of the node
            key = f"{main_code_line}#{lineno}:{func_name}"
            func_lines_footprint[key][ast.unparse(node)] = sum(val[1] for val in local_parser.vars.values())
        local_parser = Memory_Parser()
        lines_footprint = {}
        index = find_func_index(func_name, functions)
        if index != -1:
            fargs = functions[index][1]
            if len(args) != len(fargs):
                raise ValueError(f"Function {func_name} called with incorrect number of arguments.")
            else:
                for i, arg in enumerate(args):
                    local_parser.vars[fargs[i]] = global_parser.vars[arg]
            code = functions[index][2]
            func_def = f"def {func_name}({', '.join(fargs)}):"
            args_total_memory = sum(val[1] for val in local_parser.vars.values())
            key = f"{main_code_line}#{lineno}:{func_name}"
            func_lines_footprint[key][func_def] = args_total_memory 
            tree = ast.parse(code)
            agg = False
            for node in tree.body:
                # print(ast.dump(node, indent=4))  # Debugging: print the AST nodes
                node = AugAssignToAssignTransformer().visit(node)
                ast.fix_missing_locations(node)
                #! assumption deal with multilevel indexing as first level only
                node = ast.parse(re.sub(r'(\[[^\[\]]+\])(?:\[[^\[\]]+\])+', r'\1', ast.unparse(node)))
                #! x[i].append() --> x.append()
                pattern = r'(?:\[\s*[^]]+\s*\])+(?=\.\w+\s*\()'
                node = ast.parse(re.sub(pattern, '', ast.unparse(node)))
                #! if aggreagation then get the aggregation type instead of the footprint
                if ast.unparse(node).startswith('aggregation = '):
                    agg = True
                    match = re.search(r'''["'](.*?)["']''', ast.unparse(node))
                    if match:
                       agg_type = ''
                       agg_target = ''
                       full_value = match.group(1)
                       if full_value != "":
                           agg_type, agg_target = full_value.split(':', 1)
                       letter = match.group(1)
                       if agg_type in ['c', 'a', 's', 'm', 'n', 'l', 'i','']:
                           func_lines_footprint[key]["aggregation"] = f"{agg_type}:{agg_target}"
                           agg = True
                       else:
                           raise ValueError(f"Invalid aggregation type: {letter}")
                else:
                    get_footprint(node, local_parser, func_lines_footprint)
            if not agg:
                raise ValueError(f"Function {func_name} does not have an aggregation type defined.")
           
            return_statement = list(func_lines_footprint[key].keys())[-1]
            return_footprint_size,return_footprint_length = local_parser._get_return_size_length(ast.parse(return_statement))
            return return_footprint_size,return_footprint_length 
                # print(local_parser.vars)
        else:
            raise ValueError(f"Function {func_name} not found in the provided functions list.")
            
    def get_main_footprint(entry_point, functions,global_parser):
        def get_func_attributes(node, functions):
            func = value.func
            if isinstance(func, ast.Name):
                func_name = func.id
            elif isinstance(func, ast.Attribute):
                func_name = func.attr
            args = []
            for arg in value.args:
                if isinstance(arg, ast.Name):
                    args.append(arg.id)
                elif isinstance(arg, ast.Constant):
                    args.append(arg.value)
                else:
                    args.append(ast.dump(arg))  # fallback for complex args

            return func_name, args
        def substitute_outer_keys(func_dict):
            new_dict = defaultdict(dict)
            
            for func_name, inner_dict in func_dict.items():
                # Find the function signature from inner keys (starts with 'def ')
                func_signature = next((k for k in inner_dict if k.startswith('def ')), None)
                if func_signature:
                    # Extract actual signature string inside parentheses
                    old_key = func_name.split(':')[0].strip()  # Get the main code line before the first colon
                    new_key = func_signature[4:].strip(':')  # Remove 'def ' and any trailing ':'
                    new_key = f"{old_key}:{new_key}"  # Combine main code line with function signature
                    new_dict[new_key] = inner_dict
                else:
                    new_dict[func_name] = inner_dict  # fallback if signature not found
            
            return new_dict
           
        tree = ast.parse(entry_point)
        main_lines_footprint = {}
        func_lines_footprint = defaultdict(dict)
        for i,node in enumerate(tree.body):
            node = AugAssignToAssignTransformer().visit(node)
            ast.fix_missing_locations(node)
            if isinstance(node, ast.Assign):
                targets = [target.id for target in node.targets if isinstance(target, ast.Name)]
                value = node.value
                #! assumptions: only 1 return value, return value is of type list
                main_lines_footprint[ast.unparse(node)] = global_parser.vars.copy()
                if isinstance(value, ast.Call):
                    #! handle .copy() method
                    if isinstance(value.func, ast.Attribute):
                        length,memory_footprint = global_parser._list_method_handler(value)
                        global_parser.vars[targets[0]] = (length, memory_footprint, 'list')
                        line_code = ast.unparse(node)
                        key = f"{line_code}#{i}:copy()"
                        value = {f"return {value.func.value.id}.copy()":memory_footprint}
                        func_lines_footprint[key] = value
                    else:
                        func_name, args = get_func_attributes(node, functions)
                        return_footprint_size,return_footprint_length = get_func_footprint(func_name, args, functions,func_lines_footprint,global_parser,ast.unparse(node),i)
                        global_parser.vars[targets[0]] = (return_footprint_length,return_footprint_size,'list')
        main_lines_footprint = {
            outer_key: {
                inner_key: {'length': val[0], 'size': val[1]}
                for inner_key, val in inner_dict.items()
            }
            for outer_key, inner_dict in main_lines_footprint.items()
        }        
        # print("Main Lines Footprint:", main_lines_footprint)
        json.dump(main_lines_footprint, open(f'{SAVE_PATH}/main_lines_footprint.json', 'w'), indent=4)
        func_lines_footprint = substitute_outer_keys(func_lines_footprint)
        # print("Function Lines Footprint:", func_lines_footprint)
        json.dump(func_lines_footprint, open(f'{SAVE_PATH}/func_lines_footprint.json', 'w'), indent=4)                
                        
            
                
    memory_parser = Memory_Parser()
    tree = ast.parse(open(file_path, 'r').read())
    file_name = get_file_name(tree)
    read_file_block = get_read_file_block(tree)
    read_file_block = read_file_block.replace("FILE_NAME", f"'{file_name}'")
    read_file_ast = ast.parse(read_file_block)
    memory_parser._file_handler(read_file_ast)
    memory_parser.vars['data'] = memory_parser.vars['lines']
    del memory_parser.vars['lines']  
    get_main_footprint(entry_point, functions, memory_parser)
    # print(memory_parser.vars)
def Parallelizer(path,job_id):
    global SAVE_PATH
    SAVE_PATH = f"Jobs/{job_id}"
    print(f"Parallelizing {path}")
    error_file = "errors.txt"
    print("I am running")
    filename=path
    if not os.path.exists(filename):
        print(f"File {filename} does not exist.")
        return
    else:
        print(f"File {filename} exists.")
    if check_syntax_errors(filename, error_file):
       print(f"1. Syntax check passed for {filename}.")
    
    graph = build_ddg(filename)
    functions = None
    entry_point = None
    if graph:
        print(f"2. DDG built successfully for {filename}.")
        # graph.visualize_graph_data()
        # create the directory if not exists
        temp_path = f'Jobs/{job_id}'
        if not os.path.exists(temp_path):
            os.makedirs(temp_path)
        graph.save_to_json(temp_path)
        functions = graph.parser.functions
        entry_point= graph.parser.entry_point
    else:
        print(f"2. Failed to build DDG for {filename}. Check {error_file} for details.")   
    
    dep_2d_list = dependency_analyzer(temp_path)
    if dep_2d_list:
        print(f"3. Dependency analysis completed for {filename}.")
    
    get_memory_foortprint(filename,entry_point,functions,job_id)
    print(f"4. Memory footprint analysis completed for {filename}.")