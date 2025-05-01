"""
Module contains code for translating a DAG Factory config dictionary to the intermediate representation.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Union

from dagfactory.exceptions import DagFactoryConfigException
from dagfactory.ir import DagIR, TaskIR, TaskGroupIR


class Translator:
    """
    Translates a DAG Factory config dictionary to an intermediate representation.
    
    :param dag_name: Name of the DAG
    :type dag_name: str
    :param dag_config: DAG configuration dictionary
    :type dag_config: dict
    :param default_config: Default configuration dictionary
    :type default_config: dict
    """

    def __init__(
        self, 
        dag_name: str, 
        dag_config: Dict[str, Any], 
        default_config: Dict[str, Any]
    ) -> None:
        """Initialize the translator with DAG configuration."""
        self.dag_name = dag_name
        self.dag_config = dag_config
        self.default_config = default_config
        self.logger = logging.getLogger(f"dagfactory.translator.{dag_name}")
    
    def _get_task_operator(self, task_config: Dict[str, Any]) -> str:
        """
        Get the operator class path for a task.
        
        :param task_config: Task configuration dictionary
        :type task_config: dict
        :returns: Full path to the operator class
        :rtype: str
        """
        if "operator" not in task_config:
            raise DagFactoryConfigException("No operator specified in task config")
        
        return task_config["operator"]
    
    def _get_task_params(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get the parameters for a task.
        
        :param task_config: Task configuration dictionary
        :type task_config: dict
        :returns: Task parameters dictionary
        :rtype: dict
        """
        params = {}
        for key, value in task_config.items():
            if key not in ["operator", "dependencies"]:
                params[key] = value
        return params
    
    def _get_task_dependencies(self, task_config: Dict[str, Any]) -> List[str]:
        """
        Get the dependencies for a task.
        
        :param task_config: Task configuration dictionary
        :type task_config: dict
        :returns: List of task IDs that this task depends on
        :rtype: list
        """
        dependencies = []
        if "dependencies" in task_config:
            if not isinstance(task_config["dependencies"], list):
                raise DagFactoryConfigException(
                    f"Task dependencies must be a list, got {type(task_config['dependencies'])}"
                )
            dependencies = task_config["dependencies"]
        return dependencies
    
    def _translate_tasks(self) -> List[TaskIR]:
        """
        Translate task configurations to TaskIR objects.
        
        :returns: List of TaskIR objects
        :rtype: list
        """
        tasks = []
        if "tasks" not in self.dag_config:
            self.logger.warning(f"No tasks specified for DAG {self.dag_name}")
            return tasks
        
        for task_name, task_config in self.dag_config["tasks"].items():
            operator = self._get_task_operator(task_config)
            params = self._get_task_params(task_config)
            dependencies = self._get_task_dependencies(task_config)
            
            task_ir = TaskIR(
                task_id=task_name,
                operator=operator,
                params=params,
                dependencies=dependencies
            )
            tasks.append(task_ir)
        
        return tasks
    
    def _translate_task_groups(self) -> Dict[str, TaskGroupIR]:
        """
        Translate task group configurations to TaskGroupIR objects.
        
        :returns: Dictionary mapping task group IDs to TaskGroupIR objects
        :rtype: dict
        """
        task_groups = {}
        tg_config = self.dag_config.get("task_groups", {})
        
        for group_name, group_config in tg_config.items():
            # Extract task group parameters
            prefix_group_id = group_config.get("prefix_group_id", True)
            tooltip = group_config.get("tooltip")
            ui_color = group_config.get("ui_color")
            ui_fgcolor = group_config.get("ui_fgcolor")
            parent_group = group_config.get("parent_group")
            
            task_group_ir = TaskGroupIR(
                group_id=group_name,
                prefix_group_id=prefix_group_id,
                tooltip=tooltip,
                ui_color=ui_color,
                ui_fgcolor=ui_fgcolor,
                parent_group=parent_group
            )
            
            task_groups[group_name] = task_group_ir
        
        return task_groups
    
    def translate(self) -> DagIR:
        """
        Translate the DAG configuration to a DagIR object.
        
        :returns: DagIR object
        :rtype: DagIR
        """
        # Get DAG parameters
        dag_kwargs = {}
        for key, value in self.dag_config.items():
            if key not in ["tasks", "task_groups", "schedule", "default_args"]:
                dag_kwargs[key] = value
        
        # Merge default config with dag-specific config
        default_args = self.default_config.get("default_args", {})
        if "default_args" in self.dag_config:
            # Update default args with dag-specific args
            dag_default_args = default_args.copy()
            dag_default_args.update(self.dag_config["default_args"])
        else:
            dag_default_args = default_args
        
        # Create DagIR
        return DagIR(
            dag_id=self.dag_name,
            schedule=self.dag_config.get("schedule"),
            default_args=dag_default_args,
            tasks=self._translate_tasks(),
            task_groups=self._translate_task_groups(),
            dag_kwargs=dag_kwargs
        )
