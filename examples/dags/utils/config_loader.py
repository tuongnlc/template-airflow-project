"""
    Implement config loader as example
"""
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(project_root, 'dags'))

from typing import Any
from daglib.utils.config_loader import load_config


configs_path = "local_recursive://configs/ingestion_job"


raw_configs: list[dict[str, Any]] = load_config(path=configs_path)

print(raw_configs)
