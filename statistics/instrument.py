import ast


def predicate(name, value):
    args = [value,
            ast.Constant(ast.unparse(value)),
            ast.Constant(value.lineno)]
    return ast.Call(ast.Name(id=name, ctx=ast.Load()), args, keywords=[])


class Instrument(ast.NodeTransformer):
    def visit_If(self, node: ast.If) -> ast.AST:
        test = node.test
        node.test = predicate('pred_branch', test)
        super().generic_visit(test)
        return node

    def visit_Call(self, node: ast.Call) -> ast.AST:
        super().generic_visit(node)
        return predicate('pred_returns', node)


def add_predicates(module: ast.Module):
    with open('predicates.py', 'r') as file:
        tree = ast.parse(file.read())
        module.body = tree.body + module.body
        return module


def instrument(script):
    tree = ast.parse(script)
    Instrument().visit(tree)
    # add predicates after instrumentation to not instrument predicates
    add_predicates(tree)
    ast.fix_missing_locations(tree)
    return ast.unparse(tree)
