# `config_loader` Utility

This document provides a guide for using the `config_loader.py` module, a flexible tool for loading and parsing YAML configuration files for the project.

## Purpose

The main purpose of `config_loader` is to offer a single interface for reading configuration data from various sources, helping to decouple the configuration loading logic from the application's business logic.

## Core Concepts

### 1. Protocols

A "protocol" is a keyword at the beginning of a path that tells `config_loader` **where** to find and **how** to load a configuration file. This design makes the module extremely extensible.

Currently supported protocols:

*   `local://`: Reads YAML files from a specified directory on the local filesystem (non-recursively).
*   `local_recursive://`: Reads YAML files from a specified directory and **all of its subdirectories** (recursively).

### 2. Path Format

The path provided to `config_loader` must follow the format:
`protocol://path/to/your/resource`

Examples:
*   `local://configs/jobs`
*   `local_recursive://configs`

If the path does not contain `://`, the program will raise a `ConfigLoaderError`.

## Main Function: `load_config(path)`

This is the only function you need to interact with.

```python
load_config(path: str) -> list[dict[str, Any]]
```

*   **Parameter:**
    *   `path` (str): The full path to the configuration resource, including the protocol.
*   **Returns:**
    *   A list of dictionaries. Each dictionary corresponds to a document found in the YAML files. The module supports multi-document YAML files separated by `---`.
*   **Possible Exceptions:**
    *   `ConfigLoaderError`: Invalid path format or unsupported protocol.
    *   `ResourceNotFoundError`: The path does not point to a valid file or directory.
    *   `NoConfigsFoundError`: The directory contains no configuration files.
    *   `ConfigReadError`: An error occurred while reading or parsing a YAML file.

## Usage Example

Here is a complete example of how to use `config_loader`.

### 1. Directory and File Structure

Assume you have the following project structure:

```
.
├── configs/
│   └── ingestion_job/
│       ├── job1.yaml
│       └── job2.yaml
└── dags/
    ├── central_dag.py
    └── daglib/
        └── utils/
            └── config_loader.py
```

Contents of `job1.yaml`:
```yaml
job_name: first_ingestion_job
source: database
```

Contents of `job2.yaml`:
```yaml
job_name: second_ingestion_job
source: api
---
job_name: third_ingestion_job
source: file_system
```

### 2. Python Code to Load Configurations

In your `central_dag.py` file, you could write:

```python
import sys
import os
from typing import Any

# Add the project root to sys.path so Python can find `daglib`
# This code allows the script to be run from anywhere
try:
    from daglib.utils.config_loader import load_config, ConfigLoaderError
except ImportError:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    # Assuming daglib is in dags/
    sys.path.insert(0, os.path.join(project_root, 'dags'))
    from daglib.utils.config_loader import load_config, ConfigLoaderError


# Path to the config directory, using the recursive protocol
configs_path = "local_recursive://configs/ingestion_job"

try:
    # Load all configurations
    raw_configs: list[dict[str, Any]] = load_config(path=configs_path)

    # Print the result
    print(f"Successfully loaded {len(raw_configs)} configurations.")
    for config in raw_configs:
        print(config)

except ConfigLoaderError as e:
    print(f"Error loading configurations: {e}")

```

### 3. Expected Output

When you run the script, the output will be:

```
Successfully loaded 3 configurations.
{'job_name': 'first_ingestion_job', 'source': 'database'}
{'job_name': 'second_ingestion_job', 'source': 'api'}
{'job_name': 'third_ingestion_job', 'source': 'file_system'}
```
*(Note: The order of configurations may vary depending on the operating system)*

