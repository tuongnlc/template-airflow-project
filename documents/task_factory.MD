# Task Factory

## Overview

The `Task Factory` system is a core component in the dynamic DAG generation architecture, designed based on the **Factory Pattern** and **Registry Pattern**. Its main purpose is to decouple the logic of configuring a DAG (defined in a YAML file) from the specific implementation logic of each task within that DAG.

This architecture allows the system to be highly modular, easily extensible, and maintainable. Developers can add new task types without modifying the DAG building code (`DagBuilder` or `DagFactory`).

## Core Components

The system consists of two main components: `TaskFactoryBase` and `TaskFactoryRegistry`.

### 1. `TaskFactoryBase` (Abstract Base Class)

- **File:** `dags/daglib/core/task_factory.py`

`TaskFactoryBase` is an Abstract Base Class (ABC) that serves as an **interface** for all concrete factory classes. It defines a common structure that every factory must adhere to.

Its most important component is the `create_task` abstract method:

```python
@abstractmethod
def create_task(self, task_group_id: str, dag: DAG, args: dict[str, Any]) -> TaskGroup:
    """
    Creates a TaskGroup for a given DAG.
    """
    ...
```

Any class that inherits from `TaskFactoryBase` is **required** to implement this method. This ensures consistency and allows the `DagFactory` to call `create_task` on any factory without knowing its internal details.

### 2. `TaskFactoryRegistry` (Registry/Service Locator)

- **File:** `dags/daglib/core/task_factory.py`

`TaskFactoryRegistry` is a class that implements the **Registry Pattern**. It acts as a central Service Locator for all `TaskFactory` instances.

**Functionality:**

- **Storage and Mapping:** It maintains an internal dictionary to map a string identifier (`factory_type`) to a corresponding `TaskFactory` instance.
- **Provides an Interface:**
    - `register(factory: TaskFactoryBase)`: Used to register a factory instance with the registry. Each factory is associated with a `factory_type`, which is its class name.
    - `get(factory_type: str)`: Allows a client (i.e., `SingletonDagFactory`) to retrieve a registered factory by its `factory_type`.

Using a Registry helps to **decouple** the `SingletonDagFactory` from concrete factories, adhering to the Inversion of Control (IoC) principle.

## Workflow

1.  **Initialization & Registration:**
    - When the application starts (in `central_dag.py`), an instance of `TaskFactoryRegistry` is created.
    - Instances of concrete `TaskFactory` classes (e.g., `DummyTaskFactory`, `JobTaskFactory`) are instantiated and registered with the `TaskFactoryRegistry`.

2.  **DAG Building:**
    - The `DagBuilder` receives the populated `TaskFactoryRegistry`.
    - The `DagBuilder` delegates the DAG creation to the `SingletonDagFactory`, passing the `TaskFactoryRegistry` to it.

3.  **Task Creation:**
    - The `SingletonDagFactory` reads the `task_factories` configuration from the DAG's YAML file.
    - For each task entry, it retrieves the `factory_type` (e.g., `"JobTaskFactory"`).
    - It uses this `factory_type` to call `task_factory_registry.get("JobTaskFactory")` and receives the corresponding factory instance.
    - Finally, it calls the `factory.create_task(...)` method on the retrieved instance to create the actual `TaskGroup`.

## How to Add a New Task Factory

This is one of the biggest advantages of this architecture. To add a new task type (e.g., to run a Databricks job), you just need to follow these steps:

1.  **Create a New Factory Class:**
    Create a new file, for example `dags/daglib/factories/databricks_factory.py`, and define your factory class. This class **must** inherit from `TaskFactoryBase`.

    ```python
    # dags/daglib/factories/databricks_factory.py
    from typing import Any
    from airflow.sdk import DAG
    from airflow.utils.task_group import TaskGroup
    from daglib.core.task_factory import TaskFactoryBase

    class DatabricksTaskFactory(TaskFactoryBase):
        def create_task(self, task_group_id: str, dag: DAG, args: dict[str, Any]) -> TaskGroup:
            # Validate parameters in `args`
            self.validate_args(args)

            with TaskGroup(group_id=task_group_id, dag=dag) as tg:
                # Logic to create Databricks tasks goes here
                # e.g., DatabricksSubmitRunOperator(...)
                pass
            
            return tg

        def validate_args(self, args: dict[str, Any]) -> None:
            if "job_id" not in args:
                raise ValueError("`job_id` is required for DatabricksTaskFactory")
    ```

2.  **Register the New Factory:**
    In the `dags/central_dag.py` file, import and register your new factory with the registry.

    ```python
    # dags/central_dag.py
    from daglib.core.task_factory import TaskFactoryRegistry
    from daglib.core.dummy import DummyTaskFactory
    from daglib.factories.databricks_factory import DatabricksTaskFactory # <-- Import

    # ...

    task_factory_registry: TaskFactoryRegistry = TaskFactoryRegistry()
    task_factory_registry.register(DummyTaskFactory())
    task_factory_registry.register(DatabricksTaskFactory()) # <-- Register
    ```

3.  **Use it in YAML:**
    You can now use this new factory in your DAG configuration files.

    ```yaml
    # configs/some_dag.yaml
    spec:
      # ...
      task_factories:
        - id: my_databricks_job
          factory_type: DatabricksTaskFactory # <-- Use the class name
          args:
            job_id: 12345
    ```
