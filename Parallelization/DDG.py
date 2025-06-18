import networkx as nx
import matplotlib.pyplot as plt
import ast
import os
import textwrap
from tabulate import tabulate
from termcolor import colored
import re
import gc
import json
from collections import defaultdict

class DDG_Node:
    def __init__(self, line_number,statement):
        self.statement = statement
        self.line_number = line_number
        self.has=[]
        self.needs=[]
        
class DDG_Edge:
    def __init__(self, src, dest,dependencies):
        self.src = src
        self.dest = dest
        self.dependencies = dependencies
class Debugger:
    def __init__(self):
        pass
    def print_unparsed_function(self, function):
        print("\n" + "="*40)
        print(f"Function Extracted: {function.name}")
        print("="*40)
        print(textwrap.indent(ast.unparse(function), "    "))  # Indent for readability
        print("="*40)
        print(f"Function Completed: {function.name}")
        print("="*40)
    def print_entry_point(self, entry_point):
        print("\n" + "="*40)
        print(f"Entry Point Extracted")
        print("="*40)
        print(textwrap.indent(entry_point, "    "))
        print("="*40)
        print(f"Entry Point Completed")
        print("="*40)
    def print_DDG_node(self, node):
        print("\n" + "="*40)
        print(f"Line Number: {node.line_number}")
        print("\n" + "="*40)
        print(f"Has: {node.has}")
        print("\n" + "="*40)
        print(f"Needs: {node.needs}")
        print("\n" + "="*40)
    def print_DDG_edge(self, edge):
        print("\n" + "="*40)
        print("\n")
        print(f"Source: {edge.src}")
        print(f"Destination: {edge.dest}")
        print(f"Dependencies: {edge.dependencies}")
        print("\n" + "="*40)
class _Parser:
    def __init__(self,tree):
        self.tree=tree
        self.entry_point = None
        self.functions = []
        self.debugger=Debugger()
    #! extract functiona and entry point
    def extract_snippets(self,debug=False):
       for node in self.tree.body:
        #! Get function source code, name and arguements and put them in a tuple
        if isinstance(node, ast.FunctionDef):
            func_name = node.name
            arguements = [arg.arg for arg in node.args.args]
            func_code = ast.unparse(node)  
            pattern = rf'def\s+{func_name}\s*\(.*?\):\s*\n'
            func_code = re.sub(pattern, '', func_code)
            func_code = textwrap.dedent(func_code)
            self.functions.append((func_name,arguements,func_code))
            if debug:
                self.debugger.print_unparsed_function(node)
        elif isinstance(node, ast.If):
            self.entry_point = ast.unparse(node)
            self.entry_point = self.entry_point.replace("if __name__ == '__main__':", "")
            self.entry_point=textwrap.dedent(self.entry_point)
            if debug:
                self.debugger.print_entry_point(self.entry_point)
#! to replace loop variables with _
class ReplaceForTargetsWithUnderscore(ast.NodeTransformer):
    def visit_For(self, node):
        # Visit children first
        self.generic_visit(node)

        # Collect all variable names bound in target
        bound_names = self._extract_names(node.target)

        # Replace target with matching structure filled with '_'
        node.target = self._underscore_target_like(node.target)

        # Replace all uses of those names in the loop body with '_'
        node.body = [self._replace_names_in_stmt(stmt, bound_names) for stmt in node.body]
        return node

    def _extract_names(self, target):
        if isinstance(target, ast.Name):
            return {target.id}
        elif isinstance(target, (ast.Tuple, ast.List)):
            names = set()
            for elt in target.elts:
                names.update(self._extract_names(elt))
            return names
        return set()

    def _underscore_target_like(self, target):
        if isinstance(target, ast.Name):
            return ast.Name(id='_', ctx=ast.Store())
        elif isinstance(target, ast.Tuple):
            return ast.Tuple(
                elts=[self._underscore_target_like(e) for e in target.elts],
                ctx=ast.Store()
            )
        elif isinstance(target, ast.List):
            return ast.List(
                elts=[self._underscore_target_like(e) for e in target.elts],
                ctx=ast.Store()
            )
        return target  # fallback

    def _replace_names_in_stmt(self, stmt, names_to_replace):
        return ReplaceNamesWithUnderscore(names_to_replace).visit(stmt)

class ReplaceNamesWithUnderscore(ast.NodeTransformer):
    def __init__(self, names_to_replace):
        self.names_to_replace = names_to_replace

    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load) and node.id in self.names_to_replace:
            node.id = '_'
        return node
class DDG:
    def __init__(self):
        self.debugger=Debugger()
        self.edges=[]
        self.nodes=[]      
    #? TODO Handle Loops
    def extract_dependencies(self,snippet,functions,function=False,debug=False):
        #! handle the function tuple
        if function:
            func_name=snippet[0]
            arguements=snippet[1]
            snippet=snippet[2]
            #! add the function definition as a node
            node = DDG_Node(0,'def '+func_name+'('+','.join(arguements)+'):')
            node.has.extend(arguements)
            self.nodes.append(node)
            
        sub_tree=ast.parse(snippet)
        def check_var_is_global(nodes,var):
            for node in nodes:
                if var in node.has:
                    return True 
            return False
        def parse_subscript(node, gnode, not_check_globals=True):
            if isinstance(node, ast.Subscript):
                if isinstance(node.value, ast.Name) and (not_check_globals or check_var_is_global(self.nodes,node.value.id)):
                    gnode.has.append(node.value.id)
        def parse_assign(node, gnode,not_check_globals=True):
            for target in node.targets:
                    if isinstance(target, ast.Name) and(not_check_globals or check_var_is_global(self.nodes,target.id)):
                        gnode.has.append(target.id)
                    elif isinstance(target, ast.Subscript):
                        parse_subscript(target, gnode, not_check_globals)
        def parse_Expr(node, gnode,not_check_globals=True):
            list_funcs = ['pop', 'append', 'sort', 'extend', 'reverse', 'insert', 'remove', 'clear']
                
            call = node.value
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute):
                func_name = call.func.attr  # e.g., 'extend'
                if func_name in list_funcs:
                    target = call.func.value
                    if isinstance(target, ast.Name) and (not_check_globals or check_var_is_global(self.nodes,target.id)):
                        var_name = target.id  # 'y'
                        gnode.has.append(var_name)
        def parse_aug_assign(node, gnode):
            target = node.target
            if isinstance(target, ast.Name):
                gnode.has.append(target.id)
                gnode.needs.append(target.id)
            elif isinstance(target, ast.Subscript):
                parse_subscript(target, gnode)
                
        def parse_if(node, gnode):
            temp = set()
            for subnode in ast.iter_child_nodes(node):
                if isinstance(subnode, ast.Assign):
                    for target in subnode.targets:
                        parse_assign(subnode, gnode, not_check_globals=False)
                #! handle in place operations
                elif isinstance(subnode, ast.Expr):
                    parse_Expr(subnode, gnode, not_check_globals=False)
                elif isinstance(subnode, ast.AugAssign):
                    parse_aug_assign(subnode, gnode)
                elif isinstance(subnode, ast.For):
                    parse_for_loop(subnode, gnode, self.nodes)
                elif isinstance(subnode, ast.If):
                    parse_if(subnode, gnode)
                    
            gnode.has.extend(temp)
            del temp
            gc.collect()
        def parse_for_loop(node, gnode, all_nodes):
            # Replace target(s) with underscores
            ReplaceForTargetsWithUnderscore().visit(node)
            ast.fix_missing_locations(node)

            temp = set()
            list_funcs = ['pop', 'append', 'sort', 'extend', 'reverse', 'insert', 'remove', 'clear']

            for subnode in ast.iter_child_nodes(node):
                    if isinstance(subnode, ast.Assign):
                        for target in subnode.targets:
                            parse_assign(subnode, gnode, not_check_globals=False)
                    #! handle in place operations
                    elif isinstance(subnode, ast.Expr):
                        parse_Expr(subnode, gnode, not_check_globals=False)
                    elif isinstance(subnode, ast.AugAssign):
                        parse_aug_assign(subnode, gnode)
                    elif isinstance(subnode,ast.For) and subnode != node:
                        parse_for_loop(subnode, gnode, self.nodes)
                    elif isinstance(subnode, ast.If):
                        parse_if(subnode, gnode)
            gnode.has.extend(temp)
            del temp
            gc.collect()
        def parse_return(node, gnode):
            if isinstance(node.value, ast.Name):
                gnode.needs.append(node.value.id)
        def parse_node(node,gnode,not_check_globals=True):
            #! get variables being assigned to
            if isinstance(node, ast.Assign):  
                parse_assign(node, gnode,not_check_globals)
            #! handle in place operations
            elif isinstance(node, ast.Expr):
                parse_Expr(node, gnode)
            #! handle if conditions
            elif isinstance(node, ast.If):
                parse_if(node, gnode)
            #! handle the function return statement
            elif isinstance(node, ast.Return):
                parse_return(node, gnode)
            #! handle For loops
            elif isinstance (node,ast.For):
                parse_for_loop(node, gnode, self.nodes)
                
        def visit_has_needs(self, node,number):
            #! create a new node
            gnode = DDG_Node(number,ast.unparse(node))
            parse_node(node, gnode)
            #! Get variables used in the assignment
            used_vars = {name.id for name in ast.walk(node) if isinstance(name, ast.Name) and isinstance(name.ctx, ast.Load) and name.id not in  ['_','enumerate','range','zip']and  check_var_is_global(self.nodes,name.id)}
            func_names = [name for name, _, _ in functions]
            used_vars = used_vars - set(func_names)  # Exclude function names from used_vars
            gnode.needs.extend(used_vars)
            if debug:
                self.debugger.print_DDG_node(gnode)
            self.nodes.append(gnode)
            gnode.has = list(set(gnode.has))  # Remove duplicates
            gnode.has = [var for var in gnode.has if check_var_is_global(self.nodes,var)]  
            gnode.needs = list(set(gnode.needs))  # Remove duplicates
            
            
        for i,node in enumerate(sub_tree.body):
            visit_has_needs(self,node,i+1)
            
    def construct_edges(self,debug=False):
        if not self.nodes:
            raise ValueError("No nodes to construct edges from. Please run 'extract_dependencies' first.")
        #! a dictionary to hold the last occurence OF dependencies
        deps={}
        for node in self.nodes:
            edges_to_construct={}
            for need in node.needs:
                if need in deps:
                    if (deps[need],node.line_number) not in edges_to_construct:
                        edges_to_construct[(deps[need],node.line_number)]=[]
                    edges_to_construct[(deps[need],node.line_number)].append(need)
            for edge in edges_to_construct:
                edge=DDG_Edge(edge[0],edge[1],edges_to_construct[edge])
                self.edges.append(edge)    
                if debug:
                    self.debugger.print_DDG_edge(edge)
            for has in node.has:
                deps[has]=node.line_number
    def visualize_graph(self):
        if not self.nodes:
            raise ValueError("No nodes or edges to visualize. Please run 'extract_dependencies' and 'construct_edges' first.")
        G = nx.DiGraph()
        for node in self.nodes:
            G.add_node(node.line_number)

        edge_labels = {}
        for edge in self.edges:
            G.add_edge(edge.src, edge.dest)
            edge_labels[(edge.src, edge.dest)] = ", ".join(map(str, edge.dependencies)) 

        pos = nx.spring_layout(G, k=5)  #! Adjust k to control spacing

        nx.draw(
            G, pos, with_labels=True, 
            node_size=2000, node_color='darkblue', 
            font_color='white', font_weight='bold', font_size=12
        )

        nx.draw_networkx_edges(G, pos, arrowstyle='-|>', arrowsize=20)
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color='red', font_size=10)
        plt.show()
  

    def visualize_graph_data(self):
        if not self.nodes:
            raise ValueError("No nodes or edges to visualize. Please run 'extract_dependencies' and 'construct_edges' first.")
        node_data = [{'code line': node.line_number,'statement':node.statement, 'has': node.has, 'needs': node.needs} for node in self.nodes]
        edge_data = [{'Node': edge.dest, 'Depends on': edge.src,  'Dependency': edge.dependencies} for edge in self.edges]

        node_table = tabulate(node_data, headers="keys", tablefmt="fancy_grid")
        edge_table = tabulate(edge_data, headers="keys", tablefmt="fancy_grid")

        print(colored("Nodes Table:", "cyan", attrs=["bold"]))
        print(node_table)
        print("\n" + colored("Edges Table:", "yellow", attrs=["bold"]))
        print(edge_table)
    def save_to_json(self):
        if not self.nodes:
            raise ValueError("No nodes or edges to save. Please run 'extract_dependencies' and 'construct_edges' first.")
        node_data = [{'code line': node.line_number,'statement':node.statement, 'has': node.has, 'needs': node.needs} for node in self.nodes]
        edge_dict = defaultdict(lambda: {"Node": None, "Depends on": []})
        for edge in self.edges:
            if edge.dest not in edge_dict:
                edge_dict[edge.dest]["Node"] = edge.dest
            edge_dict[edge.dest]["Depends on"].append({
                "Node": edge.src,
                "Dependency": edge.dependencies
            })
        json1 = json.dumps(node_data, indent=4)
        json2 = json.dumps(edge_dict, indent=4)
        return json1,json2

        
        
class DDG_Wrapper:
    def __init__(self,tree):
        self.parser=_Parser(tree)
        self.parser.extract_snippets()
        #! index 0 is always the entry point else functions
        self.ddgs=[]
    def build_ddgs(self):
        if not self.parser.functions and not self.parser.entry_point:
            raise ValueError("No functions or entry points to extract dependencies from. Please run 'extract_snippets' first.")
        if self.parser.entry_point:
            ddg=DDG()
            ddg.extract_dependencies(self.parser.entry_point,self.parser.functions)
            ddg.construct_edges()
            self.ddgs.append(ddg)
        for function in self.parser.functions:
            ddg=DDG()
            ddg.extract_dependencies(function,self.parser.functions,function=True)
            ddg.construct_edges()
            self.ddgs.append(ddg)
            
    #! Visualize the graph (index=-1 for all graphs)
    def visualize_graph(self,index=-1):
        if index == -1:
            for index in range(len(self.ddgs)):
                self.ddgs[index].visualize_graph()
        else:
            self.ddgs[index].visualize_graph()
    #! Visualize the graph data (index=-1 for all graphs)
    def visualize_graph_data(self,index=-1):
        if index == -1:
            for index in range(len(self.ddgs)):
                print(f"Graph {index+1}")
                self.ddgs[index].visualize_graph_data()
                print('-'*40)
                print('\n')
        else:        
            print(f"Graph {index+1}")
            self.ddgs[index].visualize_graph_data()
            print('\n')
    def save_to_json(self,folder_name):
        for index,ddg in enumerate(self.ddgs):
            node_data,edge_data=ddg.save_to_json()
            with open(f"{folder_name}/graph_{index}_nodes.json", "w") as outfile:
                outfile.write(node_data)
            with open(f"{folder_name}/graph_{index}_edges.json", "w") as outfile:
                outfile.write(edge_data)
# testcases_folder_path="testcases"
# testcase=os.path.join(testcases_folder_path, "t3_loops_parsing.py")
# tree = ast.parse(open(testcase).read())
# graph=DDG_Wrapper(tree)
# graph.build_ddgs()
# graph.visualize_graph()