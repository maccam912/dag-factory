import ast
import os

from dagfactory.dagfactory import DagFactory

HERE = os.path.dirname(__file__)
CONFIG = os.path.join(HERE, "fixtures/dag_factory_prerender.yml")


def test_prerender_writes_python(tmp_path):
    factory = DagFactory(CONFIG, output_dir=tmp_path)
    factory.build_dags()
    generated = tmp_path / "example_dag.py"
    assert generated.exists()
    source = generated.read_text()
    # ensure valid python
    ast.parse(source)
    assert "EmptyOperator" in source
    assert "task_a" not in source  # ensure this file is for example_dag tasks 'first' etc
