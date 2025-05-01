"""
Module contains the Intermediate Representation (IR) for DAG Factory.

This module provides dataclasses that serve as an intermediate representation
between the YAML/dict configuration and either runtime DAG objects or generated code.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


@dataclass
class TaskIR:
    """Intermediate representation of a DAG task."""
    
    task_id: str
    operator: str  # e.g. "airflow.operators.bash.BashOperator"
    params: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)  # task_ids it depends on


@dataclass
class TaskGroupIR:
    """Intermediate representation of a task group."""
    
    group_id: str
    tasks: List[TaskIR] = field(default_factory=list)
    prefix_group_id: bool = True
    tooltip: Optional[str] = None
    ui_color: Optional[str] = None
    ui_fgcolor: Optional[str] = None
    parent_group: Optional[str] = None
    

@dataclass
class DagIR:
    """Intermediate representation of a DAG."""
    
    dag_id: str
    schedule: Optional[Union[str, Dict[str, Any]]] = None
    default_args: Dict[str, Any] = field(default_factory=dict)
    tasks: List[TaskIR] = field(default_factory=list)
    task_groups: Dict[str, TaskGroupIR] = field(default_factory=dict)
    dag_kwargs: Dict[str, Any] = field(default_factory=dict)
