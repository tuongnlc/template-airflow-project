from abc import ABC, abstractmethod
import os
import yaml
from typing import Any



class ConfigLoaderError(Exception):
    """
        Base exception for config loader error
    """
    pass


class NoConfigsFoundError(ConfigLoaderError):
    """
        Exception for when no config found
    """
    pass


class ResourceNotFoundError(ConfigLoaderError):
    """
        Exception for when resource not found
    """
    pass


class ConfigReadError(ConfigLoaderError):
    """
        Exception for when config reader error
    """
    pass


class ProtocolHandler(ABC):
    """
        Abstract base class for protocol handlers (local, redis, database, http, etc...)
    """
    @abstractmethod
    def is_single_resource(self, path: str) -> bool:
        """
            Check if the path is point to single resource
        """
        pass

    @abstractmethod
    def is_collection(self, path: str) -> bool:
        """
            Check if the path is point to collection of config resources
        """
        pass

    @abstractmethod
    def read_resource(self, path: str) -> str:
        """
            Read config content from single resource.
        """
        pass

    @abstractmethod
    def list_resource(self, collection_path: str) -> list[str]:
        """
            List all config resources in collection 
        """
        pass

class LocalProtocolHandler(ProtocolHandler):
    """
        Hanlde for local:// protocol
    """
    def is_single_resource(self, path: str) -> bool:
        """
            Check if the path is point to single resource
        """
        return os.path.isfile(path)

    def is_collection(self, path: str) -> bool:
        """
            Check if the path is point to collection of config resources
        """
        return os.path.isdir(path)

    def read_resource(self, path: str) -> str:
        """
            Read config content from single resource.
        """
        with open(file=path, mode="r", encoding="utf-8") as f:
            return f.read()

    def list_resource(self, collection_path: str) -> list[str]:
        """
            List all config resources in collection 
        """
        yaml_files = []
        for file in os.listdir(collection_path):
            file_path = os.path.join(collection_path, file)
            if os.path.isfile(file_path) and file.endswith((".yaml", ".yml")):
                yaml_files.append(file_path)
        return yaml_files
        

class LocalRecursiveProtocolHandler(LocalProtocolHandler):
    """
        Hanlde for local_recursive:// protocol with recursive search
    """
    def is_single_resource(self, path: str) -> bool:
        """
            Check if the path is point to single resource
        """
        return os.path.isfile(path)

    def is_collection(self, path: str) -> bool:
        """
            Check if the path is point to collection of config resources
        """
        return os.path.isdir(path)

    def read_resource(self, path: str) -> str:
        """
            Read config content from single resource.
        """
        with open(file=path, mode="r", encoding="utf-8") as f:
            return f.read()

    def list_resource(self, collection_path: str) -> list[str]:
        """
            List all config resources in collection 
        """
        yaml_files = []
        for root, dirs, files in os.walk(collection_path):
            for file in files:
                if file.endswith((".yaml", ".yml")):
                    file_path = os.path.join(root, file)
                    yaml_files.append(file_path)
        return yaml_files

def parse_path(path: str) -> tuple[str, str]:
    """
        Parse path with protocol prefix

        Args:
            path (str): Path with protocol prefix (e.g. local://path/to/config.yaml, redis://ey-prefix)
        Returns:
            tuple[str, str]: (protocol, actual_path)
    """
    if "://" not in path:
        raise ConfigLoaderError(f"Invalid path format: {path}")

    protocol, actual_path = path.split("://", 1)
    return protocol, actual_path

def get_protocol_handler(protocol: str) -> ProtocolHandler:
    """
        Get protocol handler for given protocol
    """
    handlers = {
        "local": LocalProtocolHandler(),
        "local_recursive": LocalRecursiveProtocolHandler(),
        # "s3": S3ProtocolHandler(),
    }

    if protocol not in handlers:
        raise ConfigLoaderError(f"Unsupported protocol: {protocol}")
    return handlers[protocol]

def load_config(path: str) -> list[dict[str, Any]]:
    """
        Load and parse config from given path
        Supports TAML files with multiple documents (seperated by --)

        Args:
            path (str): Path with protocol prefix (e.g. local://path/to/config.yaml, redis://ey-prefix)

        Returns:
            list[dict[str, Any]]: List of dictionaries containing all parsed YAML configs

        Raises:
            ConfigLoaderError: If the path is invalid or the protocol is not supported
            NoConfigsFoundError: If no config files are found in the collection
            ResourceNotFoundError: If the resource is not found or is inaccessible
            ConfigReadError: If the config file cannot be read

        Examples:
            >>> configs = load_config("local://path/to/configs")
            >>> configs = load_config("local_recursive://path/to/config.yaml")
            >>> configs = load_config("redis://ey-prefix")
    """
    protocol, actual_path = parse_path(path)
    handler: ProtocolHandler = get_protocol_handler(protocol)
    
    is_single: bool = handler.is_single_resource(path=actual_path)
    is_collection: bool = handler.is_collection(path=actual_path)

    if not is_single and not is_collection:
        raise ResourceNotFoundError(f"Path {actual_path} is not a single resource or collection of resources")

    # Collect resources paths
    resources: list[Any] = []
    if is_single:
        resources = [actual_path]
    else:
        resources = handler.list_resource(collection_path=actual_path)
        if not resources:
            raise NoConfigsFoundError(f"Collection {actual_path} is empty")

    # Parse yaml configs
    configs: list[dict[str, Any]] = []
    for resource in resources:
        try:
            content = handler.read_resource(path=resource) # Open file and read content

            # Skip empty content
            if not content.strip():
                continue
            
            documents = yaml.safe_load_all(content) # Parse yaml documents
            for document in documents:
                if not document:
                    continue

                if isinstance(document, dict):
                    configs.append(document)
        except yaml.YAMLLError as e:
            raise ConfigReadError(f"Error reading config file {resource}: {e}")
        except Exception as e:
            raise ConfigReadError(f"Error reading config file {resource}: {e}")

    return configs
    
   