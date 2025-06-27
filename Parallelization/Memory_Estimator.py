import ast
import string
import sys
import math
import enum
import os
from copy import deepcopy
import re
import random
class Primitives_Estimator:
    def __init__(self):
        pass
    def estimate_primitive_size(self, value):
        '''
        Estimate the size of a primitive value in bytes.
        '''
        return sys.getsizeof(value)
    def estimate_list_size(self,length):
        '''
        Estimate the size of a list in bytes based on its length.
        the size is calculated based on the number of elements and a precomputed capacity.
        size = size of #pointers ceiled
        '''
        def get_precomputed_capacity(length):
            if length == 0:
                return 0
            elif length <= 4:
                return 4
            elif length <= 8:
                return 8
            elif length <= 16:
                return 16
            elif length <= 25:
                return 25
            elif length <= 35:
                return 35
            elif length <= 49:
                return 49
            elif length <= 64:
                return 64
            else:
                return int(length * 1.025)
        base_size = sys.getsizeof([]) + 8 * get_precomputed_capacity(length)
        return base_size
class VariableToConstantTransformer(ast.NodeTransformer):
    def visit_Name(self, node):
        return ast.Constant(value=f"${node.id}")

class RemoveUnaryOpWrapper(ast.NodeTransformer):
    def visit_List(self, node):
        # Visit children first
        node = self.generic_visit(node)
        # Replace UnaryOp elements with their operand
        new_elts = []
        for elt in node.elts:
            if isinstance(elt, ast.UnaryOp):
                new_elts.append(elt.operand)  # unwrap
            else:
                new_elts.append(elt)
        node.elts = new_elts
        return node
class ConstantListToNamesTransformer(ast.NodeTransformer):
    def visit_Constant(self, node):
        # Only process string constants like "$var"
        if isinstance(node.value, str) and node.value.startswith("$"):
            return ast.Name(id=node.value[1:], ctx=ast.Load())

        # Handle Constant that wraps a list of strings like ["$x", "$y"]
        if isinstance(node.value, list):
            # Convert list elements recursively if they start with $
            elements = []
            for item in node.value:
                if isinstance(item, str) and item.startswith("$"):
                    elements.append(ast.Name(id=item[1:], ctx=ast.Load()))
                else:
                    elements.append(ast.Constant(value=item))
            return ast.List(elts=elements, ctx=ast.Load())

        return node

class AugAssignToExtend(ast.NodeTransformer):
    def visit_AugAssign(self, node):
        if isinstance(node.op, ast.Add):
            return ast.Expr(
                value=ast.Call(
                    func=ast.Attribute(
                        value=ast.copy_location(
                            ast.Name(id=node.target.id, ctx=ast.Load()),
                            node.target
                        ),
                        attr='extend',
                        ctx=ast.Load()
                    ),
                    args=[node.value],
                    keywords=[]
                )
            )
        return node

class AugAssignToExtend(ast.NodeTransformer):
    def visit_AugAssign(self, node):
        if isinstance(node.op, ast.Add):
            return ast.Expr(
                value=ast.Call(
                    func=ast.Attribute(
                        value=ast.copy_location(
                            ast.Name(id=node.target.id, ctx=ast.Load()),
                            node.target
                        ),
                        attr='extend',
                        ctx=ast.Load()
                    ),
                    args=[node.value],
                    keywords=[]
                )
            )
        return node
def convert_augassign_to_assign(node):
    if isinstance(node, ast.AugAssign):
        return ast.Assign(
            targets=[node.target],
            value=ast.BinOp(
                left=ast.copy_location(ast.Name(id=node.target.id, ctx=ast.Load()), node),
                op=node.op,
                right=node.value
            )
        )
    return node  # return unchanged if not AugAssign
class AugAssignToAssignTransformer(ast.NodeTransformer):
    def visit_AugAssign(self, node):
        return convert_augassign_to_assign(node)
class InsertToAppend(ast.NodeTransformer):
    def visit_Call(self, node):
        self.generic_visit(node)  # Visit children nodes first
        if (
            isinstance(node.func, ast.Attribute) and
            node.func.attr == 'insert' and
            len(node.args) == 2
        ):
            # Change method name to 'append'
            node.func.attr = 'append'
            # Keep only the second argument (the value to insert)
            node.args = [node.args[1]]
        return node
class Memory_Parser:
    transformer = VariableToConstantTransformer()
    transformer2 = ConstantListToNamesTransformer()
    transformer3 = AugAssignToExtend()
    transformer4 = InsertToAppend()
    class AssignTypes(enum.Enum):
        PRIMITIVE = "primitive"
        LIST = "list" 
    def __init__(self):
        self.primitives_estimator = Primitives_Estimator()
        self.vars = {}  #! varibles parsed so far (name: (value, memory, type))
        self.funcs = {'int':('int',0), 'str':('str',0), 'float':('float',0), 'bool':('bool',0), 'bytes':('bytes',0), 'bytearray':('bytearray',0), 'complex':('complex',0)
                      ,'list':('list',0)}  #! functions parsed so far (name: (type, memory))
        self.primitives=['int','str','float','bool','bytes','bytearray','complex','unk']  #! primitive types  
    def _reset(self):
        '''
        resets the memory parser.
        '''
        self.vars = {}
    def conv_len_assignment(self,tree):
        #! for x = len(var) case
        code = ast.unparse(tree)
        pattern = r"len\((\w+)\)"
        match = re.search(pattern, code)
        if match:
            var_name = match.group(1)
            if var_name in self.vars:
                length = self.vars[var_name][0] if self.vars[var_name][2] == 'list' else 420
                modified_code = re.sub(pattern, str(length), code)
                # print(f"Modified code: {modified_code}")  # Debugging: print the modified code
                tree = ast.parse(modified_code).body[0]  
                return tree
            else:
                raise ValueError(f"Variable {var_name} not found in local parser variables.")  
        #! for x = len(var[i])
        pattern = r"len\((\w+\[\d+\])\)" 
        match = re.search(pattern, code)
        if match:
            full_index_expr = match.group(1)  # e.g., numeric_data[0]
            var_name = full_index_expr.split('[')[0]  # Extract base variable name, e.g., numeric_data
            if var_name in self.vars:
                length = str(self.vars[var_name][0] if self.vars[var_name][2] == 'list' else 420)  # Convert to string
                # Replace only the matched part
                modified_code = code[:match.start()] + str(length) + code[match.end():]
                # print(f"Modified code: {modified_code}")  # Debugging: print the modified code
                tree = ast.parse(modified_code).body[0]
                return tree
            else:
                raise ValueError(f"Variable {var_name} not found in local parser variables.")
        return tree  # Return the original tree if no match is found
    def _hande_primitives_type_conversions(self, node):
        if isinstance(node.func, ast.Name) and node.func.id == 'int':  
                if (isinstance(node.args[0], ast.Subscript)):
                    return 100000000000000000
                arg_val = self._evaluate_primtive_expression(node.args[0])
                return int(arg_val)
        elif isinstance(node.func, ast.Name) and node.func.id == 'str':
            if (isinstance(node.args[0], ast.Subscript)): 
                return "this is a place holder for a string"
            return str(self._evaluate_primtive_expression(node.args[0]))
        elif isinstance(node.func, ast.Name) and node.func.id == 'float':
            if (isinstance(node.args[0], ast.Subscript)): 
                return 100000000000000000.0
            return float(self._evaluate_primtive_expression(node.args[0]))
        return None
    def _handle_mathematical_ops(self, node, left_val, right_val):
        if isinstance(node.op, ast.Add):
            return left_val + right_val
        elif isinstance(node.op, ast.Sub):
            return left_val - right_val
        elif isinstance(node.op, ast.Mult):
            return left_val * right_val
        elif isinstance(node.op, ast.Div):
            return left_val / right_val if right_val != 0 else 0
        elif isinstance(node.op, ast.FloorDiv):
            return left_val // right_val if right_val != 0 else 0
        elif isinstance(node.op, ast.Pow):
            return left_val ** right_val
        elif isinstance(node.op, ast.Mod):
            return left_val % right_val
        else:
            raise ValueError(f"Unsupported operation: {type(node.op).__name__}")
        
    def _evaluate_primtive_expression(self, node):
        if isinstance(node, ast.Constant):  
            return node.value
        elif isinstance(node, ast.BinOp):  
            left_val = self._evaluate_primtive_expression(node.left)
            right_val = self. _evaluate_primtive_expression(node.right)
            result = self._handle_mathematical_ops(node, left_val, right_val)
            if result is not None:
                return result    
        elif isinstance(node, ast.Name):  
                if node.id in self.vars:
                    return self.vars[node.id][0]  
                else:
                    raise NameError(f"Variable '{node.id}' is not defined.")
        elif isinstance(node, ast.Call): 
            result = self._hande_primitives_type_conversions(node)
            if result is not None:
                return result
        else:
            raise TypeError(f"Unsupported AST node: {type(node).__name__}")   
           
    def _evaluate_primitive_assignment(self,stmt):
        '''
        
        evaluate primitive assignment statements.
        
        '''
        if isinstance(stmt.targets[0],ast.Subscript):  #! handle x[0] = 5
            return
        var_name = stmt.targets[0].id  
        result = self._evaluate_primtive_expression(stmt.value)
        memory = self.primitives_estimator.estimate_primitive_size(result)
        self.vars[var_name] = (result, memory, type(result).__name__) 
    def _assignment_type(self, node):
        '''
        
        Determine the type of assignment based on the AST node.
        supported types are primitive and list, primitive functions.
        
        '''
        if isinstance(node, ast.Constant):  
            if type(node.value).__name__ in self.primitives: 
                return self.AssignTypes.PRIMITIVE
            else:
                return self.AssignTypes.LIST
        elif isinstance(node, ast.List) or isinstance(node, ast.ListComp):
            return self.AssignTypes.LIST
        elif isinstance(node, ast.Name):
            if self.vars[node.id][2] in self.primitives:
                return self.AssignTypes.PRIMITIVE
            elif self.vars[node.id][2] == 'list':
                return self.AssignTypes.LIST
        elif isinstance(node, ast.Subscript):
            return self.AssignTypes.LIST  # Subscript is typically used for list indexing
        elif isinstance(node, ast.Call):
            if self.funcs[node.func.id][0] in self.primitives:
                return self.AssignTypes.PRIMITIVE
            elif self.funcs[node.func.id][0] == 'list':
                return self.AssignTypes.LIST 
        elif isinstance(node, ast.BinOp):
            left_type = self._assignment_type(node.left)
            right_type = self._assignment_type(node.right)
            if left_type == self.AssignTypes.LIST or right_type == self.AssignTypes.LIST:
                return self.AssignTypes.LIST
            elif left_type == self.AssignTypes.PRIMITIVE and right_type == self.AssignTypes.PRIMITIVE:
                return self.AssignTypes.PRIMITIVE
    def _evaluate_list_assignment(self, stmt,first=True):
        '''
        
        gets size of a list assignment statement.
        
        '''
        def handle_subscript(stmt):
                target = stmt.value.id
                if target not in self.vars:
                    raise NameError(f"Variable '{target}' is not defined syntax error.")
                if self.vars[target][2] != 'list':
                    # raise TypeError(f"Variable '{target}' is not a list syntax error.")
                    return 1111111111, 1
                total_size = self.vars[target][1]
                total_length = self.vars[target][0]
                slice = stmt.slice
                lower = None
                upper = None
                step = 1
                size = None
                length = None
                if isinstance(slice, ast.Slice):
                    lower = slice.lower.value if isinstance(slice.lower, ast.Constant) else None
                    upper = slice.upper.value if isinstance(slice.upper, ast.Constant) else None
                    step = slice.step.value if isinstance(slice.step, ast.Constant) else 1
                elif isinstance(slice, ast.Constant):
                    lower = slice.value
                    upper = None
                    step = 1
                length = 0
                size = 0
                if lower is not None and upper is not None:
                    length = (upper - lower) // step
                    size = total_size//total_length * length + sys.getsizeof([])
                elif lower is not None and  upper is None:
                    if isinstance(slice, ast.Slice):
                        length = (total_length - lower)// step
                        size = (total_size // total_length) * length + sys.getsizeof([])
                    else:
                        size = total_size//total_length 
                        length =1
                        #! Note: this is an over estimation as it takes the size of the pointer into account 
                elif lower is None and upper is not None:
                    length = upper// step
                    size = total_size//total_length * length + sys.getsizeof([])
                return size, length
        def handle_BinOp(stmt):
            op = None
            left = None
            right = None
            if isinstance(stmt, ast.BinOp):
                op = stmt.op
                left = stmt.left
                right = stmt.right
            else:
                op = stmt.value.op
                left = stmt.value.left
                right = stmt.value.right

            left_size,left_length = _parse_list_elements_sizes(left)
            right_size,right_length = _parse_list_elements_sizes(right)
            total_size = left_size + right_size
            total_length = left_length if left_length  is not None else 0
            total_length += right_length if right_length is not None else 0
            total_length = 1 if total_length <= 0 else total_length
            if isinstance(op, ast.Add):
                if isinstance(left, ast.Name) and left.id in self.vars:
                    if self.vars[left.id][2] != 'list':
                        total_length = -1
                        total_size = total_size // total_length   if total_length > 0 else 0
                    else:
                        total_length += self.vars[left.id][0]
                        total_size+=self.primitives_estimator.estimate_list_size(self.vars[left.id][0]) - sys.getsizeof([]) 
                elif isinstance(left, ast.List):
                    total_length += len(left.elts)
                if isinstance(right, ast.Name) and right.id in self.vars:
                    if self.vars[right.id][2] != 'list':
                        total_size = total_size // total_length if total_length > 0 else 0
                        total_length = -1 
                    else:
                        total_length += self.vars[right.id][0]
                        total_size+=self.primitives_estimator.estimate_list_size(self.vars[right.id][0]) 
                elif isinstance(right, ast.List):
                    total_length += len(right.elts)
                return total_size, total_length
            else:
                total_size = total_size // total_length if total_length > 0 else 0
                total_length = -1
                return total_size, total_length
                
                
      
            
        def _parse_list_elements_sizes(node):
            # print(ast.dump(node, indent=4)) 
            node = RemoveUnaryOpWrapper().visit(node)
            ast.fix_missing_locations(node) # Debugging: print the node structure
            if isinstance(node, ast.List):  
                sizes = [_parse_list_elements_sizes(el)[0] for el in node.elts]
                total_size = sum([size for size in sizes])
                total_length = len(node.elts)
                return total_size + self.primitives_estimator.estimate_list_size(total_length),0                     
            elif isinstance(node,ast.Constant):
                return sys.getsizeof(node.value),0
            elif isinstance(node, ast.Name):
                if node.id in self.vars:
                    return self.vars[node.id][1],0            
            elif (isinstance(node, ast.Subscript)):
                size, length = handle_subscript(node)
                return size, length
            elif isinstance(node, ast.BinOp):
                size, length = handle_BinOp(node)
                return size, length
            
        if isinstance(stmt.targets[0], ast.Subscript):
           return   
        var = stmt.targets[0].id
        multiplier = 1
        if (isinstance(stmt.value, ast.Call)):
            stmt=stmt.value.args[0]
        elif (isinstance(stmt.value, ast.Subscript)):
            size, length = handle_subscript(stmt.value)
            self.vars[var] = (length, size//10, 'list')
            return  
                        
        elif (isinstance(stmt.value, ast.BinOp)): 
            #! handle list multiplication
            op = stmt.value.op
            left = stmt.value.left
            right = stmt.value.right
            if isinstance(op, ast.Mult):
                list_val = None
                if isinstance(left,ast.Name) and left.id in self.vars and self.vars[left.id][2] == 'list':
                    multiplier = right.value if isinstance(right, ast.Constant) else self.vars[right.id][0]
                    length = self.vars[left.id][0]
                    list_val = ast.List(
                        elts=[ast.Constant(value=111111111) for _ in range(length)],
                        ctx=ast.Load()
                    )  
                elif isinstance(right,ast.Name) and right.id in self.vars and self.vars[right.id][2] == 'list':
                    multiplier = left.value if isinstance(right, ast.Constant) else self.vars[left.id][0]
                    list_val = ast.List(
                        elts=[ast.Constant(value=111111111) for _ in range(length)],
                        ctx=ast.Load()
                    )  
                #! Inner 1d indexing is assumed to be a primitive not a list
                elif (isinstance(left, ast.Subscript) or isinstance(right,ast.Subscript)):
                    size, length = handle_BinOp(stmt)
                    self.vars[var] = (1111111111, size, 'unk')  
                    return
                else:
                    multiplier = right.value if  isinstance(left, ast.List) else left.value
                    list_val=left if isinstance(left, ast.List) else right
                stmt=list_val
            else:
                size, length = handle_BinOp(stmt)
                if (not isinstance(op, ast.Add)):
                    self.vars[var] = (1111111111, size, 'unk')
                else:    
                    if length == -1:
                        self.vars[var] = (111111111, size, 'unk')
                    else:
                        self.vars[var] = (length, size//10, 'list')               
                return
        elif (isinstance(stmt.value, ast.ListComp)):
            elt = stmt.value.elt
            new_assign = ast.Assign(
                targets=[ast.Name(id=var, ctx=ast.Store())],
                value=elt
            )
            multiplier = (
                stmt.value.generators[0].iter.args[0].value
                if isinstance(stmt.value.generators[0].iter, ast.Call)
                and isinstance(stmt.value.generators[0].iter.args[0], ast.Constant)
                else 1
            )            
            self._evaluate_list_assignment(new_assign)
            length = self.vars[var][0] * multiplier
            size = self.vars[var][1] * multiplier
            self.vars[var] = (length, size//10, 'list')
            return
        else:
            stmt=stmt.value
        list_length = len(stmt.elts)
        list_length = list_length * multiplier
        memory = self.primitives_estimator.estimate_list_size(list_length)
        elements_size = _parse_list_elements_sizes(stmt)[0] * multiplier
        if first:
           self.vars[var] = (list_length,elements_size//2,'list')
        else:
            self.vars[var] = (self.vars[var][0] + list_length, (self.vars[var][1] + elements_size)//2, 'list')
        
          
    def _assignmemt_handler(self,tree):
        stmt = tree
        if self._assignment_type(stmt.value) == self.AssignTypes.PRIMITIVE:
            self._evaluate_primitive_assignment(stmt)
        elif self._assignment_type(stmt.value) == self.AssignTypes.LIST :
            # print("List Assignment")            
            self._evaluate_list_assignment(stmt)
    def _handle_list_insertion(self, var,func,args,in_loop = 1):
        if func == 'append':
            #! start with $ then a variable name
            if isinstance(args[0], str) and args[0].startswith("$"):
               args[0] = args[0][1:]  # Remove the leading '$'
               if args[0] not in self.vars:
                   raise NameError(f"Variable '{args[0]}' is not defined syntax error.")
               args[0] = self.vars[args[0]][0]  # Get the value of the variable
            for i in range(in_loop):
                new_node = ast.Assign(
                    targets=[ast.Name(id=var, ctx=ast.Store())],
                    value=ast.List(elts=[ast.Constant(value=args[0])], ctx=ast.Load())
                )
                new_node = self.transformer2.visit(new_node)
                # print(f"New Node: {ast.dump(new_node)}")
                self._evaluate_list_assignment(new_node, False)
        elif func == 'extend':  
            #! extend treats immutable objects as if they were re-created in memory together with their pointers.
            
            flattened_args = []
            for sublist in args:
                for item in sublist:
                    if isinstance(item, str) and item.startswith("$"):
                        item = item[1:]
                        if item not in self.vars:
                            raise NameError(f"Variable '{item}' is not defined syntax error.")
                        if self.vars[item][2] == 'list':
                            length = self.vars[item][0]
                            size = self.vars[item][1]
                            total_size = size + self.primitives_estimator.estimate_list_size(length) -  sys.getsizeof([]) #! should be 2 one for get the size of the list and one for the pointer in the original list
                            self.vars[var] = (self.vars[var][0] + length, (self.vars[var][1] + total_size)//2, 'list')
                            continue
                    flattened_args.append(item)
            args = flattened_args
            for i in range(in_loop):
                for arg in args:
                    new_node = ast.Assign(
                        targets=[ast.Name(id=var, ctx=ast.Store())],
                        value=ast.List(elts=[ast.Constant(value=arg)], ctx=ast.Load())
                    )
                    new_node = self.transformer2.visit(new_node)
                    # print(f"New Node: {ast.dump(new_node)}")
                    self._evaluate_list_assignment(new_node, False)
    def _insertion_handler(self, tree,in_loop = 1):
        def extract_call_info(tree):
            for node in ast.walk(tree):
                # print(f"Node: {ast.dump(node,indent=4)}")  # Debugging: print the node structure
                if isinstance(node, ast.Call):
                    func_node = node.func
                    if isinstance(func_node, ast.Attribute):
                        var_id = func_node.value.id  # e.g., 'x'
                        func_type = func_node.attr   # e.g., 'append'
                        args = []
                        for arg in node.args:
                            if isinstance(arg, ast.Name):
                                # Capture variable name as a string
                               args.append([f"${arg.id}"])
                            elif isinstance(arg, ast.Call):
                                args.append(111111111111111)
                                #append(data[0])
                            elif isinstance(arg, ast.Subscript):
                                 var_sub_name = arg.value.id
                                #  print(var_sub_name)
                                 if self.vars[var_sub_name][2] == 'list':
                                    target_size = self.vars[var_sub_name][1]//self.vars[var_sub_name][0]
                                 else:
                                    target_size = self.vars[var_sub_name][1]  
                                
                                #  print(target_size)
                                 def generate_string_with_exact_size(target_size):
                                    return ''.join(random.choices(string.ascii_letters, k=target_size))
                                 args.append(generate_string_with_exact_size(min(target_size,10000)))
                            else:
                                node = self.transformer.visit(node)
                                try:
                                    args.append(ast.literal_eval(arg))
                                except (ValueError, SyntaxError):
                                    raise ValueError(f"Unsupported argument type: {type(arg).__name__}")
                        return var_id, func_type, args
         
        if isinstance(tree, ast.AugAssign): #! for handling +=
            tree = self.transformer3.visit(tree)
        else: #1 to replace insert with append
            contains_insert = any(
                isinstance(node, ast.Call) and
                isinstance(node.func, ast.Attribute) and
                node.func.attr == 'insert'
                for node in ast.walk(tree)
            )
            if contains_insert:
                tree = self.transformer4.visit(tree)
        var, func_type, args = extract_call_info(tree)
        if not var in self.vars:
            raise NameError(f"Variable '{var}' is not defined syntax error.")
        if self.vars[var][2] == 'list':
           self._handle_list_insertion(var, func_type,args, in_loop)   
            
        
    def _handle_list_deletion(self, var, func_type,in_loop = 1):
        if func_type == 'pop':
            if self.vars[var][0] == 0:
                raise IndexError(f"pop from empty list syntax error.")
            for i in range(in_loop):
                total_size = self.vars[var][1]
                total_length = self.vars[var][0]
                new_size = total_size - (total_size // total_length)  
                self.vars[var] = (total_length - 1, new_size, 'list')
            return
        elif func_type == 'clear':
            self.vars[var] = (0, sys.getsizeof([]), 'list')  # Reset to empty list
        elif func_type == 'remove':
            for i in range(in_loop):
                new_length = self.vars[var][0] - 1
                new_size = self.vars[var][1] - (self.vars[var][1] // self.vars[var][0])
                self.vars[var] = (new_length, new_size, 'list') 
    def _deletion_handler(self, tree, in_loop = 1):
        def extract_call_info(tree):
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    func_node = node.func
                    if isinstance(func_node, ast.Attribute):
                        var_id = func_node.value.id  # e.g., 'x'
                        func_type = func_node.attr   # e.g., 'pop'
                        return var_id, func_type
        var_id, func_type = None, None
        if  isinstance(tree, ast.Delete): #! handle del statements
            stmt = tree
            subscript = stmt.targets[0]
            var_id = subscript.value.id
            slice = subscript.slice
            lower = None
            upper = None
            step = 1
            if isinstance(slice, ast.Slice):
                lower = slice.lower.value if isinstance(slice.lower, ast.Constant) else None
                upper = slice.upper.value if isinstance(slice.upper, ast.Constant) else None
                step = slice.step.value if isinstance(slice.step, ast.Constant) else 1
            elif isinstance(slice, ast.Constant):
                lower = slice.value
                upper = None
                step = 1 
            total_length = self.vars[var_id][0]
            total_size = self.vars[var_id][1]
            if lower is not None and upper is not None:
                length = (upper - lower) // step
                size = total_size // total_length * length
                self.vars[var_id] = (total_length - length, total_size - size, 'list')
            elif lower is not None and upper is None:
                if isinstance(slice, ast.Slice):
                        length = (total_length - lower)// step
                        size = total_size - (total_size // total_length) * length
                        length = total_length - length 
                else:
                    length = total_length - 1
                    size = total_size - total_size // total_length
                self.vars[var_id] = (length, size//10, 'list')
            elif lower is None and upper is not None:
                length = upper// step
                size = total_size - total_size//total_length * length
                length = total_length - length
                self.vars[var_id] = (length, size//10, 'list')     

                
            return                
        else:
            var_id, func_type = extract_call_info(tree)
        if var_id not in self.vars:
            raise NameError(f"Variable '{var_id}' is not defined syntax error.")
        if self.vars[var_id][2] != 'list':
            raise TypeError(f"Variable '{var_id}' is not a list syntax error.")
        self._handle_list_deletion(var_id, func_type, in_loop)
                    
         
    def _list_method_handler(self, tree):
        def extract_call_info(tree):
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    func_node = node.func
                    if isinstance(func_node, ast.Attribute):
                        var_id = func_node.value.id  # e.g., 'x'
                        func_type = func_node.attr   # e.g., 'pop'
                        return var_id, func_type
        var, func_type = extract_call_info(tree)
        #! returns length,size
        if func_type == ['reverse','sort']:
            if var not in self.vars:
                raise NameError(f"Variable '{var}' is not defined syntax error.")
            if self.vars[var][2] != 'list':
                raise TypeError(f"Variable '{var}' is not a list syntax error.")
            # Reversing a list does not change its size or length
            return None,None
        elif func_type in ['count','index']:
            return 0, sys.getsizeof(10000000)  #! hardcoded maybe changed later (or maybe not :( )
        elif func_type in ['copy']:
            return self.vars[var][0],self.vars[var][1]
            
        
    
        # print(f"Variable ID: {var}, Function Type: {func_type}, Arguments: {args}")
    def _file_handler(self, tree):
        '''
        gets the file metadata and records it in the dictionary.
        '''
        def extract_file_info(tree):
            file_path = None
            list_name = None

            for node in ast.walk(tree):
                # Find open() call and extract file path
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'open':
                    if len(node.args) >= 1 and isinstance(node.args[0], ast.Constant):
                        file_path = node.args[0].value

                # Find assignment to variable from file.readlines()
                if isinstance(node, ast.Assign):
                    if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
                        if node.value.func.attr == 'readlines' and isinstance(node.targets[0], ast.Name):
                            list_name = node.targets[0].id

            return file_path, list_name
        file_path ,var = extract_file_info(tree)
        file_size = os.path.getsize(file_path) + sys.getsizeof([])  #! add the size of the list pointer
        length = 0
        with open(file_path, 'rb') as f:
            length = sum(1 for _ in f)
        file_size += self.primitives_estimator.estimate_list_size(length)
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline()
            ncols = len(first_line.strip().split()) if first_line else 0
        file_size += self.primitives_estimator.estimate_list_size(ncols) * length  #! add the size of the list pointer
        self.vars[var] = (length, file_size, 'list')
    def _get_return_size_length(self,node):
        var_name = None
        lower = None
        upper = None
        step = None
        ret_stmt = node.body[0]
        if not isinstance(ret_stmt, ast.Return):
            return None

        value = ret_stmt.value
   
        if isinstance(value, ast.Name):
            var_name = value.id

        # Case: return x[0] or return x[0:4] or return x[0:8:2]
        if isinstance(value, ast.Subscript) and isinstance(value.value, ast.Name):
            var_name = value.value.id

            # Case: return x[0]
            if isinstance(value.slice, ast.Constant):
                lower = value.slice.value
                upper = lower + 1
                step = 1
    
            # Case: return x[0:4] or x[0:8:2]
            elif isinstance(value.slice, ast.Slice):
                def get_val(val): return val.value if isinstance(val, ast.Constant) else None
                lower = get_val(value.slice.lower)
                upper = get_val(value.slice.upper)
                step = get_val(value.slice.step) if get_val(value.slice.step) is not None else 1
            
        if var_name not in self.vars:
            raise NameError(f"Variable '{var_name}' is not defined syntax error.")
        original_length, original_size, _ = self.vars[var_name]
        if lower is None:
            length = self.vars[var_name][0] if self.vars[var_name][2] == 'list' else 1
            return original_size, length
        else:
            new_length = (upper - lower) // step
            new_size = int(original_size // original_length * new_length)
            return new_size, new_length
    def _handle_loop_footprint(self, node):
        def get_number_outer_iterations(node):
            loop_expr = None
            iter_node = node.iter
            #! no subscript within the loop
            if isinstance(iter_node, ast.Subscript):
                iter_node = ast.Name(id=iter_node.value.id, ctx=ast.Load())
            # Case 1: for i in x:
                
            if isinstance(iter_node, ast.Name):
                loop_expr = f"len({iter_node.id})"
            # Case 2: for i in range(...):
            elif isinstance(iter_node, ast.Call) and isinstance(iter_node.func, ast.Name):
                if iter_node.func.id == 'range':
                    if len(iter_node.args) == 1:
                        # range(N)
                        if isinstance(iter_node.args[0], ast.Constant):
                            loop_expr = str(iter_node.args[0].value)
                        else:
                            loop_expr = ast.unparse(iter_node.args[0])
                            if not loop_expr.startswith("len("):
                                if loop_expr in self.vars:
                                    loop_expr = self.vars[loop_expr][0]
                                else:
                                    raise ValueError(f"Variable '{loop_expr}' is not defined syntax error.") 
                    elif len(iter_node.args) in [2, 3]:
                        start = ast.unparse(iter_node.args[0])
                        stop = ast.unparse(iter_node.args[1])
                        if len(iter_node.args) == 3:
                            step = ast.unparse(iter_node.args[2])
                            loop_expr = f"({stop} - {start}) // {step}"
                        else:
                            loop_expr = f"{stop} - {start}"
                # Case 3: for index, name in enumerate(names):
                elif iter_node.func.id == 'enumerate' and len(iter_node.args) == 1:
                    arg_expr = ast.unparse(iter_node.args[0])
                    loop_expr = f"len({arg_expr})"
                # Case 4: for i, j in zip(a, b):
                elif iter_node.func.id == 'zip':
                    zipped_lens = []
                    for arg in iter_node.args:
                        zipped_lens.append(f"len({ast.unparse(arg)})")
                    loop_expr = f"min({', '.join(zipped_lens)})"
            # Try resolving len(x) from known vars
            if isinstance(loop_expr, int):
                return loop_expr 
            if loop_expr:
                if loop_expr.startswith("len(") and loop_expr.endswith(")"):
                    var = loop_expr[4:-1].strip()
                    if var in self.vars:
                        loop_expr = self.vars[var][0]
                return loop_expr
            else:
                raise ValueError("Unsupported loop iteration expression.")
        def has_inner_for_loop(node):
            if not isinstance(node, ast.For):
                return False

            for child in ast.walk(node):
                if child is not node and isinstance(child, ast.For):
                    return True
            return False
        def get_assignment_targets(assign_node):
            targets = []
            target = assign_node.target
            if isinstance(target, ast.Name):
                targets.append(target.id)
            elif isinstance(target, ast.Tuple):
                targets.extend([elt.id for elt in target.elts if isinstance(elt, ast.Name)])
            return targets
        def handle_nested_loops(node, depth=0,n_outer_iterations=None, n_inner_iterations=None):
            if not isinstance(node, ast.For):
                return
            # print("  " * depth + f"Handling loop at depth {depth}")
            n_iters = n_outer_iterations if depth == 0 else n_inner_iterations #* n_outer_iterations
            if not isinstance(n_iters, int):
                n_iters = 420
            for stmt in node.body:
                # print(ast.dump(stmt, indent=4))
                if isinstance(stmt, ast.For):
                    original_vars = self.vars.copy()
                    targets = get_assignment_targets(stmt)
                    costs = get_iterable_cost_expr_only(stmt, self.vars)
                    for i,target in enumerate(targets):
                        self.vars[target] = (1,costs[i], 'unk')
                    handle_nested_loops(stmt, depth + 1, n_outer_iterations=n_iters, n_inner_iterations=n_inner_iterations)
                    # for key in list(self.vars.keys()):
                    #     if key not in original_vars:
                    #         del self.vars[key]
                elif isinstance(stmt, ast.If):
                    self._handle_if_footprint(stmt,n_iters)
                else:
                    #! if a list it's most likely an augmented assignment
                    if isinstance(stmt, ast.Assign):
                        if isinstance(stmt.targets[0], ast.Subscript):
                           return #! most probably changing a value not inserting anything 
                        stmt = self.conv_len_assignment(deepcopy(stmt))
                        # if stmt.targets[0].id in self.vars: 
                        #     if self.vars[stmt.targets[0].id][2] == 'list':
                        #         size_before_loop = self.vars[stmt.targets[0].id][1]
                        #         len_before_loop = self.vars[stmt.targets[0].id][0]
                        #         self._assignmemt_handler(stmt)
                        #         size_after_loop = self.vars[stmt.targets[0].id][1]
                        #         len_after_loop = self.vars[stmt.targets[0].id][0]
                        #         size_increment = size_after_loop - size_before_loop
                        #         len_increment = len_after_loop - len_before_loop
                        #         new_size = size_before_loop + size_increment * int(n_iters)
                        #         new_length = len_before_loop + len_increment * int(n_iters)
                        #         self.vars[stmt.targets[0].id] = (new_length,new_size, 'list')
                        # else:
                        self._assignmemt_handler(stmt)
                    elif  isinstance(stmt, ast.AugAssign):
                        size_before_loop = self.vars[stmt.target.id][1]
                        len_before_loop = self.vars[stmt.target.id][0]
                        self._insertion_handler(deepcopy(stmt))
                        size_after_loop = self.vars[stmt.target.id][1]
                        len_after_loop = self.vars[stmt.target.id][0]
                        size_increment = size_after_loop - size_before_loop
                        len_increment = len_after_loop - len_before_loop
                        new_size = size_before_loop + size_increment * int(n_iters)
                        new_length = len_before_loop + len_increment * int(n_iters)
                        self.vars[stmt.target.id] = (new_length,new_size//10, 'list')
                    elif isinstance(stmt, ast.Expr):
                        func = stmt.value.func.attr 
                        if func in ['insert', 'append', 'extend']:
                            size_before_loop = self.vars[stmt.value.func.value.id][1]
                            len_before_loop = self.vars[stmt.value.func.value.id][0]
                            self._insertion_handler(deepcopy(stmt))
                            size_after_loop = self.vars[stmt.value.func.value.id][1]
                            len_after_loop = self.vars[stmt.value.func.value.id][0]
                            size_increment = size_after_loop - size_before_loop
                            len_increment = len_after_loop - len_before_loop
                            new_size = size_before_loop + size_increment * int(n_iters)
                            new_length = len_before_loop + len_increment * int(n_iters)
                            self.vars[stmt.value.func.value.id] = (new_length,new_size//10, 'list')
                        elif func in ['pop', 'remove','clear']:
                            size_before_loop = self.vars[stmt.value.func.value.id][1]
                            len_before_loop = self.vars[stmt.value.func.value.id][0]
                            self._deletion_handler(deepcopy(stmt))
                            size_after_loop = self.vars[stmt.value.func.value.id][1]
                            len_after_loop = self.vars[stmt.value.func.value.id][0]
                            size_decrement = size_after_loop - size_before_loop
                            len_decrement = len_after_loop - len_before_loop
                            new_size = size_before_loop + size_decrement * int(n_iters)
                            new_length = len_before_loop + len_decrement * int(n_iters)
                            self.vars[stmt.value.func.value.id] = (new_length,new_size//10, 'list')
                    elif  isinstance(stmt, ast.Delete):
                            for target in stmt.targets:
                                if isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):    
                                    var_name = target.value.id        
                                    size_before_loop = self.vars[var_name][1]
                                    len_before_loop = self.vars[var_name][0]
                                    self._deletion_handler(deepcopy(stmt))
                                    size_after_loop = self.vars[var_name][1]
                                    len_after_loop = self.vars[var_name][0]
                                    size_decrement = size_after_loop - size_before_loop
                                    len_decrement = len_after_loop - len_before_loop
                                    #! max is to avoid negative sizes or lengths
                                    new_size = max(size_before_loop + size_decrement * int(n_iters),0)
                                    new_length = max(len_before_loop + len_decrement * int(n_iters),0)
                                    self.vars[var_name] = (new_length,new_size, 'list')
            # print("  " * (depth + 1) + f"Statement: {ast.dump(stmt)}")
            # print(self.vars)
        def get_iterable_cost_expr_only(for_node, vars_dict):
            iter_expr = for_node.iter
            if isinstance(iter_expr, ast.Subscript):
                iter_expr = ast.Name(id=iter_expr.value.id, ctx=ast.Load())
            cost_exprs = []

            def var_expr(name):
                return self.vars[name][1]//self.vars[name][0]

            if isinstance(iter_expr, ast.Call):
                func_name = getattr(iter_expr.func, 'id', None)

                if func_name == 'enumerate':
                    arg = iter_expr.args[0]
                    if isinstance(arg, ast.Name):
                        name = arg.id
                        cost_exprs.append(sys.getsizeof(100000))
                        cost_exprs.append(var_expr(name))

                elif func_name == 'zip':
                    for arg in iter_expr.args:
                        if isinstance(arg, ast.Name):
                            cost_exprs.append(var_expr(arg.id))

                elif func_name == 'range':
                    cost_exprs.append(sys.getsizeof(100000))

            elif isinstance(iter_expr, ast.Name):
                cost_exprs.append(var_expr(iter_expr.id))

            elif isinstance(iter_expr, ast.Call) and isinstance(iter_expr.func, ast.Name) and iter_expr.func.id == 'len':
                cost_exprs.append("sys.getsizeof(100000)")

            elif isinstance(iter_expr, ast.Constant) and isinstance(iter_expr.value, int):
                cost_exprs.append("sys.getsizeof(100000)")
            return cost_exprs
        
        original_vars = self.vars.copy()
        n_outer_iterations = get_number_outer_iterations(node)
        n_inner_iterations = 420 if has_inner_for_loop(node) else None
        targets = get_assignment_targets(node)
        costs = get_iterable_cost_expr_only(node, self.vars) 
        for i,target in enumerate(targets):
            self.vars[target] = (1,costs[i], 'unk')  # Initialize targets to empty lists

        handle_nested_loops(node, depth=0, n_outer_iterations=n_outer_iterations, n_inner_iterations=n_inner_iterations)
        # for key in list(self.vars.keys()):
        #     if key not in original_vars:
        #         del self.vars[key]
        # print(self.vars)
    def _handle_if_footprint(self, node, n_iterations=1):
        def extract_all_if_blocks(root_if_node):
            blocks = []
            current = root_if_node
            while isinstance(current, ast.If):
                blocks.append(ast.Module(body=deepcopy(current.body), type_ignores=[]))
                if len(current.orelse) == 1 and isinstance(current.orelse[0], ast.If):
                    current = current.orelse[0]  # move to next `elif`
                else:
                    if current.orelse:
                        blocks.append(ast.Module(body=deepcopy(current.orelse), type_ignores=[]))  # final `else` block
                    break
            return blocks
        def handle_if_blocks(blocks,n_iterations = 1):
            vars_footprints_dicts= []
            n_iter = n_iterations 
            for block in blocks:
                original_vars = self.vars.copy()
                for stmt in block.body:
                    if isinstance(stmt, ast.Assign):
                        if isinstance(stmt.targets[0], ast.Subscript):
                           return #! most probably changing a value not inserting anything 
                        stmt = self.conv_len_assignment(deepcopy(stmt))
                        # if stmt.targets[0].id in self.vars: 
                        #     if self.vars[stmt.targets[0].id][2] == 'list':
                        #         size_before_loop = self.vars[stmt.targets[0].id][1]
                        #         len_before_loop = self.vars[stmt.targets[0].id][0]
                        #         self._assignmemt_handler(stmt)
                        #         size_after_loop = self.vars[stmt.targets[0].id][1]
                        #         len_after_loop = self.vars[stmt.targets[0].id][0]
                        #         size_increment = size_after_loop - size_before_loop
                        #         len_increment = len_after_loop - len_before_loop
                        #         new_size = size_before_loop + size_increment * int(n_iter)
                        #         new_length = len_before_loop + len_increment * int(n_iter)
                        #         self.vars[stmt.targets[0].id] = (new_length,new_size, 'list')
                        # else:
                        self._assignmemt_handler(stmt)
                    elif  isinstance(stmt, ast.AugAssign):
                        size_before_loop = self.vars[stmt.target.id][1]
                        len_before_loop = self.vars[stmt.target.id][0]
                        self._insertion_handler(deepcopy(stmt))
                        size_after_loop = self.vars[stmt.target.id][1]
                        len_after_loop = self.vars[stmt.target.id][0]
                        size_increment = size_after_loop - size_before_loop
                        len_increment = len_after_loop - len_before_loop
                        new_size = size_before_loop + size_increment * int(n_iter)
                        new_length = len_before_loop + len_increment * int(n_iter)
                        self.vars[stmt.target.id] = (new_length,new_size//10, 'list')
                    elif isinstance(stmt, ast.Expr):
                        func = stmt.value.func.attr 
                        if func in ['insert', 'append', 'extend']:
                            size_before_loop = self.vars[stmt.value.func.value.id][1]
                            len_before_loop = self.vars[stmt.value.func.value.id][0]
                            self._insertion_handler(deepcopy(stmt))
                            size_after_loop = self.vars[stmt.value.func.value.id][1]
                            len_after_loop = self.vars[stmt.value.func.value.id][0]
                            size_increment = size_after_loop - size_before_loop
                            len_increment = len_after_loop - len_before_loop
                            new_size = size_before_loop + size_increment * int(n_iter)
                            new_length = len_before_loop + len_increment * int(n_iter)
                            self.vars[stmt.value.func.value.id] = (new_length,new_size//10, 'list')
                        elif func in ['pop', 'remove','clear']:
                            size_before_loop = self.vars[stmt.value.func.value.id][1]
                            len_before_loop = self.vars[stmt.value.func.value.id][0]
                            self._deletion_handler(deepcopy(stmt))
                            size_after_loop = self.vars[stmt.value.func.value.id][1]
                            len_after_loop = self.vars[stmt.value.func.value.id][0]
                            size_decrement = size_after_loop - size_before_loop
                            len_decrement = len_after_loop - len_before_loop
                            new_size = size_before_loop + size_decrement * int(n_iter)
                            new_length = len_before_loop + len_decrement * int(n_iter)
                            self.vars[stmt.value.func.value.id] = (new_length,new_size, 'list')
                        elif isinstance(stmt, ast.Delete):
                            for target in stmt.targets:
                                if isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):    
                                    var_name = target.value.id        
                                    size_before_loop = self.vars[var_name][1]
                                    len_before_loop = self.vars[var_name][0]
                                    self._deletion_handler(deepcopy(stmt))
                                    size_after_loop = self.vars[var_name][1]
                                    len_after_loop = self.vars[var_name][0]
                                    size_decrement = size_after_loop - size_before_loop
                                    len_decrement = len_after_loop - len_before_loop
                                    #! max is to avoid negative sizes or lengths
                                    new_size = max(size_before_loop + size_decrement * int(n_iter),0)
                                    new_length = max(len_before_loop + len_decrement * int(n_iter),0)
                                    self.vars[var_name] = (new_length,new_size, 'list')
                    elif isinstance(stmt, ast.If):
                        self._handle_if_footprint(stmt, n_iterations)
                    elif isinstance(stmt, ast.For):
                        self._handle_loop_footprint(stmt)
                vars_footprints_dicts.append(self.vars.copy())
                self.vars = original_vars.copy()  #! Reset to original vars after each block
            footprints = []
            # print(vars_footprints_dicts)
            for item in vars_footprints_dicts:
                total_size = sum(value[1]  for value in item.values())  #! sum the values of the dicts
                footprints.append(total_size)
            # print(footprints)
            max_index = footprints.index(max(footprints))  #! get the index of the max footprint
            # print(max_index)
            self.vars = vars_footprints_dicts[max_index]  #! set the vars to the max footprint dict
            # for key in list(self.vars.keys()):
            #     if key not in original_vars:
            #         del self.vars[key]
                
        # original_vars = self.vars.copy()
        blocks = extract_all_if_blocks(node)
        # for block in blocks:
        #     print(ast.dump(block, indent=4))
        handle_if_blocks(blocks, n_iterations)
       
            
       
        
        
