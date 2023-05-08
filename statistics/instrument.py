from _ast import Call, Constant, If, Name
import ast
from typing import Any

class Instrument(ast.NodeTransformer):
    def visit_If(self, node: If) -> Any:
        name = ast.unparse(node.test)
        test = node.test
        func = ast.Name(id = 'pred_branch', ctx = ast.Load())
        node.test = ast.Call(func = func, args = [test, ast.Constant(value = name), ast.Constant(node.lineno)], keywords = [])
        super().generic_visit(test)
        return node
    
    def visit_Call(self, node: Call) -> Any:
        name = ast.unparse(node)
        super().generic_visit(node)
        return ast.Call(func = ast.Name(id = 'pred_returns', ctx = ast.Load()), args = [node, ast.Constant(value = name), ast.Constant(value = node.lineno)], keywords = [])

def add_predicates(module: ast.Module):
    with open('predicates.py', 'r') as file:
        tree = ast.parse(file.read())
        module.body = tree.body + module.body
        return module

def instrument(file):
    with open(file, 'r') as file:
        tree = ast.parse(file.read())
        Instrument().visit(tree)
        add_predicates(tree)  # add predicates after instrumentation
        ast.fix_missing_locations(tree)
        return ast.unparse(tree)
