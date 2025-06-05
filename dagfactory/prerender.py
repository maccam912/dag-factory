import ast
from typing import Dict
from datetime import date, datetime

try:
    _UNPARSE = ast.unparse
except AttributeError:  # pragma: no cover - Python <3.9
    try:  # defer optional dependency
        import astor

        def _UNPARSE(node: ast.AST) -> str:
            return astor.to_source(node)

    except Exception as err:  # pragma: no cover - missing astor
        raise RuntimeError("Python < 3.9 requires 'astor' for prerendering") from err


def _make_import(module: str, name: str) -> ast.stmt:
    return ast.ImportFrom(module=module, names=[ast.alias(name=name, asname=None)], level=0)


def _value_to_ast(value):
    return ast.parse(repr(value)).body[0].value


def render_python_dag(dag_params: Dict, tasks: Dict) -> str:
    """Return Python source for DAG defined by parameters."""
    module_body = []

    # Import DAG and each operator used
    module_body.append(_make_import("airflow.models", "DAG"))
    operators = {conf.get("operator") for conf in tasks.values() if conf.get("operator")}
    for operator in sorted(operators):
        mod, cls = operator.rsplit(".", 1)
        module_body.append(_make_import(mod, cls))

    dag_args = {k: v for k, v in dag_params.items() if k != "tasks"}

    if any(isinstance(v, (date, datetime)) for v in dag_args.values()):
        module_body.insert(0, ast.Import(names=[ast.alias(name="datetime", asname=None)]))
    dag_call = ast.Call(
        func=ast.Name(id="DAG", ctx=ast.Load()),
        args=[],
        keywords=[ast.keyword(arg=k, value=_value_to_ast(v)) for k, v in dag_args.items()],
    )

    with_body = []
    task_nodes = {}
    for task_id, conf in tasks.items():
        op = conf.get("operator")
        params = {k: v for k, v in conf.items() if k not in ("operator", "dependencies")}
        call = ast.Call(
            func=ast.Name(id=op.rsplit(".", 1)[1], ctx=ast.Load()),
            args=[],
            keywords=[ast.keyword(arg=k, value=_value_to_ast(v)) for k, v in params.items()],
        )
        assign = ast.Assign(targets=[ast.Name(id=task_id, ctx=ast.Store())], value=call)
        with_body.append(assign)
        task_nodes[task_id] = ast.Name(id=task_id, ctx=ast.Load())

    for task_id, conf in tasks.items():
        for dep in conf.get("dependencies", []):
            expr = ast.Expr(
                value=ast.BinOp(left=task_nodes[dep], op=ast.RShift(), right=task_nodes[task_id])
            )
            with_body.append(expr)

    with_stmt = ast.With(
        items=[ast.withitem(context_expr=dag_call, optional_vars=ast.Name(id="dag", ctx=ast.Store()))],
        body=with_body,
    )
    module_body.append(with_stmt)

    mod = ast.Module(body=module_body, type_ignores=[])
    mod = ast.fix_missing_locations(mod)
    return _UNPARSE(mod)
