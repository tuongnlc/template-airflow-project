# ConfigRegistry

## Overview

The `ConfigRegistry` is a centralized system designed to manage, validate, and retrieve various types of configuration objects within the application. It acts as a single source of truth for configurations, ensuring that they are well-structured, valid, and easily accessible.

It is particularly useful in systems where configurations are loaded from external sources (like YAML or JSON files) and need to be parsed into strongly-typed Python objects.

## Core Concepts

### `kind` Field
Every configuration object is expected to have a `kind` field. This string identifier determines the type of the configuration (e.g., `"DagDefinition"`, `"TaskFactoryConfig"`). The registry uses this field to map raw dictionary data to the appropriate Pydantic model for validation and object creation.

### `MODEL_MAPPING`
This is a dictionary that serves as the central mapping between a `kind` string and its corresponding Pydantic model class. To support a new configuration type, you simply add an entry to this mapping.

```python
# dags/daglib/core/config.py

MODEL_MAPPING = {
    "DagDefinition": DagDefinition,
    # Add new mappings here
    # "TaskFactoryConfig": TaskFactoryConfig,
}
```

### Validation
The registry leverages Pydantic models for robust data validation. When raw configuration data is loaded via the `populate` method, it finds the correct model using the `kind` field and then uses `model_validate()` to parse and validate the data. This ensures that all configurations in the registry are complete and type-safe.

## API Reference

### `populate(raw_configs: list[dict[str, Any]])`
This method loads and validates a list of raw configuration dictionaries.

- It iterates through the list.
- For each dictionary, it checks for a `kind` key.
- It looks up the corresponding model in `MODEL_MAPPING`.
- It validates the data and creates a model instance.
- The validated config object is stored in the registry, indexed by its `metadata.name`.
- It logs warnings for invalid `kind`s and errors for validation failures.

### `get_all_config_by_kind(kind: str) -> Sequence[BaseConfig]`
Returns a sequence of all configuration objects that match the specified `kind`.

**Note:** The original implementation has a minor bug. The corrected logic is:
```python
def get_all_config_by_kind(self, kind: str) -> Sequence[BaseConfig]:
    """
        Get all config with given kind
    """
    return [config for config in self.configs.values() if config.kind == kind]
```

### `get_by_name(name: str) -> BaseConfig`
Retrieves a single configuration object from the registry by its unique name (`metadata.name`). Raises a `ValueError` if the name is not found.

### `register(config_obj: BaseConfig)`
Manually adds a pre-validated configuration object to the registry. This is useful for programmatically adding configs that are not from a raw data source. Raises a `ValueError` if a config with the same name already exists.

### `get_model_for_kind(kind: str) -> type[BaseConfig]`
Returns the Pydantic model class associated with a given `kind`. Raises a `ValueError` if the `kind` is not found in `MODEL_MAPPING`.

## Example Usage

Let's assume we have the following Pydantic models defined:

```python
# models.py
from pydantic import BaseModel
from typing import Literal, Optional

class Metadata(BaseModel):
    name: str

class BaseConfig(BaseModel):
    kind: str
    metadata: Metadata

class DagDefinition(BaseConfig):
    kind: Literal["DagDefinition"] = "DagDefinition"
    schedule: Optional[str] = None
    description: Optional[str] = None
```

Now, let's use the `ConfigRegistry` to manage our configurations.

```python
# main.py
from daglib.core.config import ConfigRegistry

# 1. Define raw configuration data (e.g., from a YAML file)
raw_configs = [
    {
        "kind": "DagDefinition",
        "metadata": {
            "name": "daily_sales_report"
        },
        "schedule": "0 1 * * *",
        "description": "A DAG to generate daily sales reports."
    },
    {
        "kind": "DagDefinition",
        "metadata": {
            "name": "user_data_processing"
        },
        "schedule": "@hourly"
    },
    {
        "kind": "UnknownKind",  # This will be ignored
        "metadata": {
            "name": "some_other_config"
        }
    }
]

# 2. Initialize the registry
registry = ConfigRegistry()

# 3. Populate the registry with raw data
# You will see INFO and WARNING logs during this step
registry.populate(raw_configs)

# 4. Retrieve configurations
# Get all configs of a specific kind
all_dags = registry.get_all_config_by_kind("DagDefinition")
print(f"Found {len(all_dags)} DAG definitions.")
# Expected output: Found 2 DAG definitions.

# Get a specific config by name
daily_report_dag = registry.get_by_name("daily_sales_report")
print(f"Schedule for '{daily_report_dag.metadata.name}': {daily_report_dag.schedule}")
# Expected output: Schedule for 'daily_sales_report': 0 1 * * *

# Accessing a non-existent config will raise an error
try:
    registry.get_by_name("non_existent_dag")
except ValueError as e:
    print(e)
    # Expected output: Config not found: non_existent_dag
```

## Extending the Registry

To add support for a new configuration type, follow these two steps:

1.  **Define the Pydantic Model**: Create a new class that inherits from `BaseConfig`.

    ```python
    # models.py
    from typing import Any

    class TaskFactoryConfig(BaseConfig):
        kind: Literal["TaskFactoryConfig"] = "TaskFactoryConfig"
        task_type: str
        parameters: dict[str, Any]
    ```

2.  **Update `MODEL_MAPPING`**: Add the new model to the mapping in `dags/daglib/core/config.py`.

    ```python
    # dags/daglib/core/config.py
    from daglib.models.airflow import DagDefinition
    from .models import TaskFactoryConfig # Import the new model

    MODEL_MAPPING = {
        "DagDefinition": DagDefinition,
        "TaskFactoryConfig": TaskFactoryConfig, # Add the new entry
    }
    ```

The registry can now validate and store configurations with `kind: "TaskFactoryConfig"`.
