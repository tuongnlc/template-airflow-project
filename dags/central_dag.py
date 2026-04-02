from typing import Any
from daglib.utils.config_loader import load_config
from daglib.core.config import ConfigRegistry
from daglib.core.task_factory import TaskFactoryRegistry
from daglib.core.dummy import DummyTaskFactory
from daglib.core.dag_builder import DagBuilder



configs_path = "configs/ingestion_job"


raw_configs: list[dict[str, Any]] = load_config(path=configs_path)
config_registry = ConfigRegistry()
config_registry.populate(raw_configs)

task_factory_registry: TaskFactoryRegistry = TaskFactoryRegistry()
task_factory_registry.register(DummyTaskFactory())

dag_builder = DagBuilder(config_registry=config_registry, task_factory_registry=task_factory_registry)

try:
    all_dags = dag_builder.build_all()
except Exception as e:
    raise e

for dag_id, dag_obj in all_dags.items():
    globals()[dag_id] = dag_obj
   