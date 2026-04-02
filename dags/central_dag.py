from typing import Any
from daglib.utils.config_loader import load_config
from daglib.core.config import ConfigRegistry

configs_path = "configs/ingestion_job"


raw_configs: list[dict[str, Any]] = load_config(path=configs_path)
config_registry = ConfigRegistry()
config_registry.populate(raw_configs)

task_factory_registry: TaskFactoryRegistry = TaskFactoryRegistry()
