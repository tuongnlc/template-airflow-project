# SingletonDagFactory

## Overview

The `SingletonDagFactory` class plays a central role in dynamically building Airflow DAGs (Directed Acyclic Graphs) from YAML configuration files. It acts as an **Orchestrator** or a "chief engineer," taking a blueprint (`DagDefinition`) and assembling it into a complete `DAG` object.

This class does not create specific tasks itself. Instead, it delegates task creation to specialized `TaskFactory` classes via the `TaskFactoryRegistry`.

## Key Roles and Responsibilities

- **DAG Creation Orchestration:** Serves as the main entry point for converting a `DagDefinition` configuration object into an executable `DAG` object.
- **Configuration Parsing:** Reads and processes DAG attributes such as `dag_id`, `schedule`, `start_date`, and `default_args` from the configuration.
- **Root DAG Object Creation:** Initializes the `airflow.DAG` object with the necessary metadata.
- **Task Creation Delegation:** Uses the `TaskFactoryRegistry` to find and invoke the correct `TaskFactory` for each task defined in the configuration file.
- **Dependency Setup:** Reads the `dependencies` configuration and establishes the upstream/downstream relationships between tasks (or more accurately, `TaskGroup`s).

## Detailed Workflow

The process of creating a DAG is orchestrated by the `create_dag` method and proceeds in three sequential steps:

1.  **`_create_dag_instance(spec)`**
    - **Purpose:** To create the "skeleton" or "shell" of the DAG.
    - **Actions:**
        - Reads high-level metadata from the `spec` (parsed from YAML) such as `dag_id`, `description`, `schedule`, `start_date`, and `tags`.
        - Calls `_build_default_args` to process default parameters.
        - Initializes an `airflow.DAG` object.
        - Embeds the entire YAML configuration content into the DAG's `doc_md` attribute, which makes debugging and observation in the Airflow UI extremely convenient.

2.  **`_create_tasks(dag, task_configs)`**
    - **Purpose:** To assemble the "parts" (tasks) into the DAG skeleton.
    - **Actions:**
        - Iterates through the `task_factories` list in the configuration.
        - For each task, it retrieves the `factory_type`.
        - It uses the `factory_type` to request the corresponding `TaskFactory` instance from the `TaskFactoryRegistry`: `self.task_factory_registry.get(factory_type)`.
        - It **delegates** task creation by calling the `create_task_group()` method on the retrieved factory instance.
        - It stores all created `TaskGroup`s in a dictionary (`task_group_map`) for use in the next step.

3.  **`_setup_dependencies(task_map, task_configs)`**
    - **Purpose:** To connect the "parts" together.
    - **Actions:**
        - Iterates again through the `task_factories` list.
        - Checks if a task has a `dependencies` attribute.
        - If so, it finds the corresponding `TaskGroup` objects in the `task_map`.
        - It calls the `task_group.set_upstream(dependency_task_group)` method to establish the dependency relationship between them.

## Interaction with Other Components

- **`DagDefinition`:** This is the Pydantic object containing the validated configuration data from the YAML file. `SingletonDagFactory` uses it as its input "blueprint."
- **`TaskFactoryRegistry`:** `SingletonDagFactory` depends on this registry (via Dependency Injection in the `__init__` method) to retrieve the necessary `TaskFactory` instances without having direct knowledge of them.
- **`TaskFactory` (concrete):** `SingletonDagFactory` is a client of the `TaskFactory` classes. It calls their `create_task_group` method to perform the task creation.
