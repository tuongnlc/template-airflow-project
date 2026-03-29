from typing import Any
from daglib.utils.config_loader import load_config


configs_path = "configs/ingestion_job"


raw_configs: list[dict[str, Any]] = load_config(path=configs_path)
config_registry = ConfigRegistry()
