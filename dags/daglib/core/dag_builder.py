import logging
from .config import ConfigRegistry
from .task_factory import TaskFactoryRegistry
from .dag_factory import SingletonDagFactory
from airflow import DAG
from typing import Sequence
from .config import DagDefinition
from airflow.sdk import get_parsing_context


logger = logging.getLogger(__name__)


class DagBuilder:
    def __init__(self, 
        config_registry = ConfigRegistry(),
        task_factory_registry = TaskFactoryRegistry(),
    ):
        self.config_registry = config_registry
        # self.task_factory_registry = task_factory_registry
        self.dag_factory = SingletonDagFactory(task_factory_registry)

    def _create_failed_dag(self, dag_id: str, error_msg: str) -> None:
        """
            Create a failed DAG with given ID
        """
        failed_dag = DAG(
            dag_id=dag_id,
            doc_md=error_msg,
            tags=["dag_creation_error"],
        )
        return failed_dag
        
    def build_all(self):
        dag_definitions = Sequence[DagDefinition] = (
            self.config_registry.get_all_config_by_kind("DagDefinition")
        )

        logger.info(f"Build {len(dag_definition)} DAGs")

        dags = {}

        failed_count = 0

        current_dag_id = get_parsing_context().dag_id

        for dag_definition in dag_definitions: 
            dag_id = dag_definition.spec.dag_id

            if current_dag_id and dag_id != current_dag_id:
                continue
            try:
                dags[dag_id] = self.dag_factory.create_dag(dag_definition)
                logger.debug(f"Create DAG: {dag_id}")
            except Exception as e:
                failed_count += 1
                logger.error(f"Error creating DAG: {dag_id}, error: {e}")
                error_dag_id = f"{dag_id}_error"
                dags[error_dag_id] = self._create_failed_dag(error_dag_id, str(e))
                continue
        
        return dags
       
