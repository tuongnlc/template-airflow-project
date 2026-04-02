import logging
from .task_factory import TaskFactoryRegistry
from models.airflow import DagDefinition, DagDefinitionSpec, DagDefaultArgs
from airflow import DAG
from datetime import datetime
from typing import Any
from datetime import timedelta
from airflow.sdk.definition.context import Context
from typing import Callable
import yaml
from models.airflow import TaskFactoryConfig



logger = logging.getLogger(__name__)

class SingletonDagFactory:
    """
        Build Airflow Dags from YAML config

        Args:
            task_factory_registry (TaskFactoryRegistry): Task factory registry to use 
        Returns:
            DAG: Airflow DAG instance
    """

    def __init__(self, task_factory_registry: TaskFactoryRegistry):
        self.task_factory_registry = task_factory_registry

    def create_dag(self, dag_definition: DagDefinition) -> DAG:
        """
            Create DAG from definition
        """
        # Create DAG from definition
        spec = dag_definition.spec.model_dump()
        
        dag = self._create_dag_instance(spec)
        task_map = self._create_tasks(dag, spec.task_factories)
        self._setup_dependencies(task_map, spec.task_factories)

        return dag

    def _create_dag_instance(self, spec: DagDefinitionSpec) -> DAG:
        """
            Create DAG instance from spec
        """
        start_date = datetime.strptime(spec.start_date, "%Y-%m-%d")
        default_args = (
            self._build_default_args(spec.default_args if spec.default_args else None)
            ) or {}
        schedule = None if spec.schedule.lower() == "none" else spec.schedule

        doc_md = f"""
            ```yaml
                {yaml.dump(spec.model_dump(), default_flow_style=False)}
                ```
            ```
        """

        dag = DAG(
            dag_id = spec.dag_id,
            default_args = default_args,
            description = spec.description,
            schedule = schedule,
            start_date = start_date,
            catchup = spec.catchup,
            tags = spec.tags,
            doc_md = doc_md,
            max_active_runs = spec.max_active_runs,
        )

        logger.debug(
            f"Create DAG instance: {spec.dag_id}"
            f" with default_args: {default_args}"
            f" and schedule: {schedule}"
            f" and start_date: {start_date}"
            f" and catchup: {spec.catchup}"
            f" and tags: {spec.tags}"
            f" and max_active_runs: {spec.max_active_runs}"
        )
        return dag

    def _build_default_args(self, default_args_spec: DagDefaultArgs) -> dict[str, Any]:
        """
            Build default arguments for spec
        """
        default_args = {
            k:v for k,v in default_args_spec.model_dump().items() if v is not None
        }

        if "retry_delay" in default_args:
            default_args["retry_delay"] = timedelta(**default_args["retry_delay"])
               
        if "on_failure_slack_channels" in default_args:
            list_of_failure_callbacks = list[Callable[[Context], None]]()
            for slack_conn_id in default_args["on_failure_slack_channels"]:
                list_of_failure_callbacks.append(self.task_factory_registry.get_slack_task(slack_conn_id))
            default_args["on_failure"] = list_of_failure_callbacks

    def _create_tasks(self, dag: DAG, task_configs: list[TaskFactoryConfig]) -> dict[str, Any]:
        """
            Create tasks for spec
        """
        task_group_map ={}

        for task_config in task_configs:
            task_group_id = task_config.id
            factory_type = task_config.factory_type

            try:
                factory = self.task_factory_registry.get(factory_type)
                task_group = factory.create_task(
                    task_group_id=task_group_id,
                    dag=dag,
                    args=task_config.args
                )
                task_group_map[task_group_id] = task_group
            except Exception as e:
                logger.error(f"Error creating task group: {task_group_id}, error: {e}")

                raise ValueError(f"Error creating task group: {task_group_id}, error: {e}")

    def _setup_dependencies(self, task_map: dict[str, Any], task_configs: list[TaskFactoryConfig]) -> None:
        """
            Setup dependencies for tasks
        """
        for task_config in task_configs:
            if not task_config.dependencies:
                continue
        
            task_group_id = task_config.id
            task_group = task_map[task_group_id]

            for dependency_id in task_config.dependencies:
                if dependency_id not in task_map:
                    raise ValueError(f"Dependency not found: {dependency_id}")
                
                task_group.set_upstream(task_map[dependency_id])
                logger.debug(f"Set upstream dependency: {dependency_id} for task group: {task_group_id}")