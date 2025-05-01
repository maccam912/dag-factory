"""
Tests for the Translator module.
"""
import pytest

from dagfactory.translator import Translator
from dagfactory.exceptions import DagFactoryConfigException
from dagfactory.ir import DagIR, TaskIR, TaskGroupIR


def test_translator_initialize():
    """Test Translator initialization."""
    dag_name = "test_dag"
    dag_config = {
        "schedule": "0 0 * * *",
        "default_args": {"owner": "airflow"},
    }
    default_config = {
        "default_args": {"retries": 3}
    }
    
    translator = Translator(dag_name, dag_config, default_config)
    
    assert translator.dag_name == dag_name
    assert translator.dag_config == dag_config
    assert translator.default_config == default_config


def test_get_task_operator():
    """Test _get_task_operator method."""
    dag_name = "test_dag"
    dag_config = {}
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    
    task_config = {"operator": "airflow.operators.bash.BashOperator"}
    operator = translator._get_task_operator(task_config)
    assert operator == "airflow.operators.bash.BashOperator"


def test_get_task_operator_missing():
    """Test _get_task_operator method with missing operator."""
    dag_name = "test_dag"
    dag_config = {}
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    
    task_config = {}
    with pytest.raises(DagFactoryConfigException) as excinfo:
        translator._get_task_operator(task_config)
    
    assert "No operator specified in task config" in str(excinfo.value)


def test_get_task_params():
    """Test _get_task_params method."""
    dag_name = "test_dag"
    dag_config = {}
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    
    task_config = {
        "operator": "airflow.operators.bash.BashOperator",
        "bash_command": "echo 'Hello World'",
        "dependencies": ["task1", "task2"],
        "retries": 3
    }
    params = translator._get_task_params(task_config)
    assert params == {
        "bash_command": "echo 'Hello World'",
        "retries": 3
    }


def test_get_task_dependencies():
    """Test _get_task_dependencies method."""
    dag_name = "test_dag"
    dag_config = {}
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    
    task_config = {
        "operator": "airflow.operators.bash.BashOperator",
        "dependencies": ["task1", "task2"]
    }
    dependencies = translator._get_task_dependencies(task_config)
    assert dependencies == ["task1", "task2"]


def test_get_task_dependencies_empty():
    """Test _get_task_dependencies method with no dependencies."""
    dag_name = "test_dag"
    dag_config = {}
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    
    task_config = {
        "operator": "airflow.operators.bash.BashOperator"
    }
    dependencies = translator._get_task_dependencies(task_config)
    assert dependencies == []


def test_get_task_dependencies_invalid():
    """Test _get_task_dependencies method with invalid dependencies."""
    dag_name = "test_dag"
    dag_config = {}
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    
    task_config = {
        "operator": "airflow.operators.bash.BashOperator",
        "dependencies": "task1"  # Not a list
    }
    with pytest.raises(DagFactoryConfigException) as excinfo:
        translator._get_task_dependencies(task_config)
    
    assert "Task dependencies must be a list" in str(excinfo.value)


def test_translate_tasks():
    """Test _translate_tasks method."""
    dag_name = "test_dag"
    dag_config = {
        "tasks": {
            "task1": {
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo 'Task 1'"
            },
            "task2": {
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo 'Task 2'",
                "dependencies": ["task1"]
            }
        }
    }
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    tasks = translator._translate_tasks()
    
    assert len(tasks) == 2
    
    # Check task1
    task1 = next(task for task in tasks if task.task_id == "task1")
    assert task1.operator == "airflow.operators.bash.BashOperator"
    assert task1.params == {"bash_command": "echo 'Task 1'"}
    assert task1.dependencies == []
    
    # Check task2
    task2 = next(task for task in tasks if task.task_id == "task2")
    assert task2.operator == "airflow.operators.bash.BashOperator"
    assert task2.params == {"bash_command": "echo 'Task 2'"}
    assert task2.dependencies == ["task1"]


def test_translate_tasks_empty():
    """Test _translate_tasks method with no tasks."""
    dag_name = "test_dag"
    dag_config = {}
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    tasks = translator._translate_tasks()
    
    assert tasks == []


def test_translate_task_groups():
    """Test _translate_task_groups method."""
    dag_name = "test_dag"
    dag_config = {
        "task_groups": {
            "group1": {
                "prefix_group_id": False,
                "tooltip": "Group 1",
                "ui_color": "#FFFFFF"
            },
            "group2": {
                "parent_group": "group1"
            }
        }
    }
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    task_groups = translator._translate_task_groups()
    
    assert len(task_groups) == 2
    
    # Check group1
    group1 = task_groups["group1"]
    assert group1.group_id == "group1"
    assert group1.prefix_group_id is False
    assert group1.tooltip == "Group 1"
    assert group1.ui_color == "#FFFFFF"
    assert group1.parent_group is None
    
    # Check group2
    group2 = task_groups["group2"]
    assert group2.group_id == "group2"
    assert group2.prefix_group_id is True
    assert group2.tooltip is None
    assert group2.ui_color is None
    assert group2.parent_group == "group1"


def test_translate_task_groups_empty():
    """Test _translate_task_groups method with no task groups."""
    dag_name = "test_dag"
    dag_config = {}
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    task_groups = translator._translate_task_groups()
    
    assert task_groups == {}


def test_translate():
    """Test the translate method."""
    dag_name = "test_dag"
    dag_config = {
        "schedule": "0 0 * * *",
        "default_args": {"owner": "airflow"},
        "catchup": False,
        "tasks": {
            "task1": {
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo 'Task 1'"
            }
        },
        "task_groups": {
            "group1": {
                "tooltip": "Group 1"
            }
        }
    }
    default_config = {
        "default_args": {
            "retries": 3,
            "start_date": "2023-01-01"
        }
    }
    
    translator = Translator(dag_name, dag_config, default_config)
    dag_ir = translator.translate()
    
    # Check DagIR
    assert dag_ir.dag_id == "test_dag"
    assert dag_ir.schedule == "0 0 * * *"
    assert dag_ir.dag_kwargs == {"catchup": False}
    
    # Check default_args (merged)
    assert dag_ir.default_args == {
        "retries": 3, 
        "start_date": "2023-01-01",
        "owner": "airflow"
    }
    
    # Check tasks
    assert len(dag_ir.tasks) == 1
    task = dag_ir.tasks[0]
    assert task.task_id == "task1"
    assert task.operator == "airflow.operators.bash.BashOperator"
    assert task.params == {"bash_command": "echo 'Task 1'"}
    
    # Check task groups
    assert len(dag_ir.task_groups) == 1
    group = dag_ir.task_groups["group1"]
    assert group.group_id == "group1"
    assert group.tooltip == "Group 1"


def test_translate_minimal():
    """Test the translate method with minimal config."""
    dag_name = "test_dag"
    dag_config = {}
    default_config = {}
    
    translator = Translator(dag_name, dag_config, default_config)
    dag_ir = translator.translate()
    
    # Check DagIR
    assert dag_ir.dag_id == "test_dag"
    assert dag_ir.schedule is None
    assert dag_ir.default_args == {}
    assert dag_ir.tasks == []
    assert dag_ir.task_groups == {}
    assert dag_ir.dag_kwargs == {}
