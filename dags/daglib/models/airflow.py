import re
from typing import Any
from pydantic import BaseModel, field_validator, model_validate
from .base import BaseConfig, Metadata


class TaskFactoryConfig(BaseModel):
    """
        Configure for a single task factory instance

        Attributes:
            id (str): Unique identifier for the task factory
            factory_type (str): Type of the task factory to use
            dependencies (list[str]): List of tasks id this task depends on
            args (dict[str, Any]): Arguments to pass to the task factory
    """
    id: str
    factory_type: str
    dependencies: list[str] = []
    args: dict[str, Any] = {}

    @field_validator("id")
    def validate_id(cls, v: str) -> str:
        """
            Validate id is in right format
        """
        if not re.match(r"^[a-zA-Z0-9_]+$", v):
            raise ValueError(f"Invalid id: {v}")
        return v
   

class DagDefaultArgs(BaseModel):
    """
        Default arguments for all tasks in a DAG

        Attributes:
            owner (str): Owner of the DAG
            retries (int): Number of retries for failed tasks, default 0
            retry_delay (int): Delay between retries, default 0
            on_failure_callback (str): Callback function to run on task failure, default None
    """
    owner: str
    retries: int = 0
    retry_delay: int = 0
    on_failure_callback: str = None


class DagDefinitionSpec(BaseModel):
    """
        Specification for defining an Airflow DAG

        Attributes:
            dag_id (str): Unique identifier for the DAG
            schedule: Cron expression for Airflow schedule interval
            start_date: Start date for the DAG
            task_factories: List of task factory instances to use in the DAG. Must contain at least one task factory.
            catchup: Whether to catch up on missed runs, default False
            default_args: Default arguments for all tasks in the DAG, default None
            tags: List of tags to apply to the DAG, default []
            description: Description of the DAG, default ""
    """
    dag_id: str
    schedule: str
    start_date: str
    task_factories: list[TaskFactoryConfig]
    catchup: bool = False
    default_args: DagDefaultArgs = None
    tags: list[str] = []
    description: str = ""
    max_active_runs: int = 10

    @field_validator("dag_id")
    def validate_dag_id(cls, v: str) -> str:
        """
            Validate dag_id is in right format
        """
        if not re.match(r"^[a-zA-Z0-9_]+$", v):
            raise ValueError(f"Invalid dag_id: {v}")
        return v
    
    @field_validator("schedule")
    def validate_schedule(cls, v: str) -> str:
        """
            Validate schedule is in right format for Airflow
        """
        airfllow_presets = {
            "@once",
            "@hourly",
            "@daily",
            "@weekly",
            "@monthly",
            "@yearly",
        }
        if v in airfllow_presets or v.lower() == "none":
            return v

        cron_parts = v.split()

        if len(cron_parts) not in (5, 6):
            raise ValueError(f"Invalid schedule: {v}")
        return v
    
    @field_validator(mode="after")
    def validate_task_dependencies(self) -> "DagDefinitionSpec":
        """
            Ensure all task dependencies reference existing task 
        """
        if not self.task_factories:
            raise self
        
        task_ids = {task.id for task in self.task_factories}
        for task in self.task_factories:
            if task.dependencies:
                for dep in task.dependencies:
                    if dep not in task_ids:
                        raise ValueError(f"Invalid task dependency: {dep}")
        return self

class DagDefinition(BaseConfig):
    """
        Configuration for a single Airflow DAG
    """
    kind: str = "DagDefinition"
    spec: DagDefinitionSpec
