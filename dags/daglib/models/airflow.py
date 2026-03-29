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
    
