"""
Tests for the Intermediate Representation (IR) module.
"""
import pytest
from dataclasses import asdict

from dagfactory.ir import TaskIR, TaskGroupIR, DagIR


def test_task_ir():
    """Test that TaskIR dataclass works as expected."""
    task_ir = TaskIR(
        task_id="test_task",
        operator="airflow.operators.bash.BashOperator",
        params={"bash_command": "echo 'Hello World'"},
        dependencies=["another_task"]
    )
    
    assert task_ir.task_id == "test_task"
    assert task_ir.operator == "airflow.operators.bash.BashOperator"
    assert task_ir.params == {"bash_command": "echo 'Hello World'"}
    assert task_ir.dependencies == ["another_task"]


def test_task_ir_default_values():
    """Test that TaskIR dataclass default values work as expected."""
    task_ir = TaskIR(
        task_id="test_task",
        operator="airflow.operators.bash.BashOperator",
    )
    
    assert task_ir.task_id == "test_task"
    assert task_ir.operator == "airflow.operators.bash.BashOperator"
    assert task_ir.params == {}
    assert task_ir.dependencies == []


def test_task_group_ir():
    """Test that TaskGroupIR dataclass works as expected."""
    task_group_ir = TaskGroupIR(
        group_id="test_group",
        tasks=[
            TaskIR(
                task_id="task_in_group",
                operator="airflow.operators.bash.BashOperator",
                params={"bash_command": "echo 'In Group'"},
            )
        ],
        prefix_group_id=False,
        tooltip="Test Group Tooltip",
        ui_color="#FFFFFF",
        ui_fgcolor="#000000",
        parent_group="parent_group"
    )
    
    assert task_group_ir.group_id == "test_group"
    assert len(task_group_ir.tasks) == 1
    assert task_group_ir.tasks[0].task_id == "task_in_group"
    assert task_group_ir.prefix_group_id is False
    assert task_group_ir.tooltip == "Test Group Tooltip"
    assert task_group_ir.ui_color == "#FFFFFF"
    assert task_group_ir.ui_fgcolor == "#000000"
    assert task_group_ir.parent_group == "parent_group"


def test_task_group_ir_default_values():
    """Test that TaskGroupIR dataclass default values work as expected."""
    task_group_ir = TaskGroupIR(group_id="test_group")
    
    assert task_group_ir.group_id == "test_group"
    assert task_group_ir.tasks == []
    assert task_group_ir.prefix_group_id is True
    assert task_group_ir.tooltip is None
    assert task_group_ir.ui_color is None
    assert task_group_ir.ui_fgcolor is None
    assert task_group_ir.parent_group is None


def test_dag_ir():
    """Test that DagIR dataclass works as expected."""
    task_ir = TaskIR(
        task_id="test_task",
        operator="airflow.operators.bash.BashOperator",
        params={"bash_command": "echo 'Hello World'"},
    )
    
    task_group_ir = TaskGroupIR(
        group_id="test_group",
        tasks=[
            TaskIR(
                task_id="task_in_group",
                operator="airflow.operators.bash.BashOperator",
            )
        ]
    )
    
    dag_ir = DagIR(
        dag_id="test_dag",
        schedule="0 0 * * *",
        default_args={"start_date": "2023-01-01"},
        tasks=[task_ir],
        task_groups={"test_group": task_group_ir},
        dag_kwargs={"catchup": False}
    )
    
    assert dag_ir.dag_id == "test_dag"
    assert dag_ir.schedule == "0 0 * * *"
    assert dag_ir.default_args == {"start_date": "2023-01-01"}
    assert len(dag_ir.tasks) == 1
    assert dag_ir.tasks[0].task_id == "test_task"
    assert len(dag_ir.task_groups) == 1
    assert "test_group" in dag_ir.task_groups
    assert dag_ir.dag_kwargs == {"catchup": False}


def test_dag_ir_default_values():
    """Test that DagIR dataclass default values work as expected."""
    dag_ir = DagIR(dag_id="test_dag")
    
    assert dag_ir.dag_id == "test_dag"
    assert dag_ir.schedule is None
    assert dag_ir.default_args == {}
    assert dag_ir.tasks == []
    assert dag_ir.task_groups == {}
    assert dag_ir.dag_kwargs == {}
