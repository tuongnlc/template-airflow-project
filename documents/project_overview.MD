# Project Overview: Dynamic DAG Generation Framework for Airflow

## Introduction

This project provides a powerful and flexible framework for automatically generating and managing Apache Airflow DAGs (Directed Acyclic Graphs) from YAML configuration files. The primary goal is to completely decouple the workflow definition logic from the implementation source code, allowing Data Engineers and Analysts to create and modify DAGs quickly without deep intervention into Python code.

## Core Architecture

The system is built upon classic design patterns like the **Factory Pattern** and **Registry Pattern** to achieve high modularity and extensibility.

Overall architecture diagram:

```
[YAML Configs] -> [DagBuilder] -> [SingletonDagFactory] -> [TaskFactoryRegistry] -> [Concrete TaskFactory]
```

The main components include:

1.  **YAML Configuration (`configs/`)**:
    *   This is the "blueprint" for each DAG.
    *   Each YAML file defines a single DAG, including metadata (schedule, start date, etc.) and a list of tasks to be executed.
    *   Each task is defined by an `id`, a `factory_type` (specifying the task type), and `args` (parameters) specific to that task.

2.  **`DagBuilder` (`daglib/core/dag_builder.py`)**:
    *   This is the starting point for the entire process.
    *   It reads all YAML configuration files and then delegates the construction of each DAG to the `SingletonDagFactory`.

3.  **`SingletonDagFactory` (`daglib/core/dag_factory.py`)**:
    *   Acts as the "chief engineer" or "Orchestrator".
    *   It receives a DAG configuration (`DagDefinition`) and is responsible for assembling it into a complete `DAG` object.
    *   It does not create tasks itself but delegates this work to specialized `TaskFactory` classes.

4.  **`TaskFactoryRegistry` (`daglib/core/task_factory.py`)**:
    *   Acts as a "directory" or a Service Locator.
    *   It maintains a mapping between a `factory_type` (a string identifier) and the corresponding `TaskFactory` instances.
    *   It helps the `SingletonDagFactory` find the right "team" to build a specific type of task without knowing about the existence of that class.

5.  **`TaskFactoryBase` and Concrete `TaskFactory` classes**:
    *   `TaskFactoryBase` is an abstract class (interface) that defines the common structure for all factories.
    *   Concrete `TaskFactory` classes (e.g., `JobTaskFactory`, `DummyTaskFactory`) inherit from `TaskFactoryBase` and contain the logic to create a specific type of `TaskGroup` in Airflow. These are the actual "specialized workers".

## Workflow

1.  **Load Configuration**: The `DagBuilder` loads all `.yaml` files from the `configs/` directory.
2.  **Register Factories**: On application startup, all concrete `TaskFactory` classes are registered with the `TaskFactoryRegistry`.
3.  **Build DAGs**:
    *   The `DagBuilder` iterates through each loaded configuration.
    *   For each configuration, it calls `SingletonDagFactory.create_dag()`.
4.  **Assemble Tasks**:
    *   The `SingletonDagFactory` creates the DAG "shell".
    *   It iterates through the `task_factories` list in the configuration.
    *   For each task, it asks the `TaskFactoryRegistry` for the appropriate `TaskFactory` based on the `factory_type`.
    *   It calls the `create_task()` method on that factory to create the `TaskGroup`.
5.  **Set Dependencies**: After all tasks are created, the `SingletonDagFactory` sets up the upstream/downstream relationships between them based on the `dependencies` attribute in the YAML.
6.  **Finalize**: The `DagBuilder` collects all the completed `DAG` objects and injects them into Airflow's `globals()` to be recognized.

## Usage Guide

### Defining a New DAG

1.  Create a new YAML file in the `configs/` directory (e.g., `configs/my_new_dag.yaml`).
2.  Define the DAG according to the structure, specifying the `dag_id`, `schedule`, and the list of `task_factories`.

    ```yaml
    kind: DagDefinition
    metadata:
      name: my_new_dag
    spec:
      dag_id: my_new_dag
      schedule: "0 1 * * *"
      start_date: "2024-01-01"
      catchup: False
      tags: ["new_feature"]
      task_factories:
        - id: first_task
          factory_type: DummyTaskFactory # Use an existing factory
          args: {}
        - id: second_task
          factory_type: JobTaskFactory # Use another factory
          dependencies:
            - first_task # This task runs after first_task
          args:
            # ... parameters for JobTaskFactory
    ```

### Creating a New Task Type

1.  **Create Factory Class**: Create a new Python class that inherits from `TaskFactoryBase` and implements the `create_task` method.
2.  **Register Factory**: In `dags/central_dag.py`, import and register an instance of the new factory class with the `TaskFactoryRegistry`.
3.  **Use It**: You can now use the new `factory_type` (the factory's class name) in your YAML files.

## Benefits

*   **Clear Separation of Concerns**: Business logic (in YAML) is separated from implementation logic (in Python).
*   **Easy Extensibility**: Adding a new task type only requires creating a new Python file without affecting existing code.
*   **Increased Development Speed**: Users can self-serve the creation and modification of DAGs without deep knowledge of the Airflow API.
*   **High Maintainability**: The logic for each task type is neatly encapsulated within its own factory.
