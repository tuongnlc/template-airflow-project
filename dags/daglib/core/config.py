from typing import Type
from daglib.core.config import BaseConfig
from daglib.models.airflow import DagDefinition
from typing import Any
import logging
from typing import Literal, Sequence



MODEL_MAPPING = dict[str, Type[BaseConfig]] = {
    "DagDefinition": DagDefinition,
}


class ConfigRegistry:
    """
        Config registry to store and manage all config objects

        Steps:
            1. Load raw config data from file
            2. Go through each config and transform it to model object base on kind
            3. Register model object to registry
    """
    def __init__(self):
        self.configs: dict[str, BaseConfig] = {}

    def populate(self, raw_configs: list[dict[str, Any]]) -> None:
        """
            Populate config registry with raw config data
        """
        for data in raw_configs:
            kind: Any | None = data.get("kind")
            if not kind:
                continue

            try:
                model_class: type[BaseConfig] = MODEL_MAPPING[kind]
                config_model: BaseConfig = model_class.model_validate(obj=data)
                name: str = config_model.metadata.name
                if name in self.configs:
                    raise ValueError(f"Duplicate config name: {name}")
                logging.info(f"Populate config: {name}")
                self.configs[name] = config_model
            except KeyError:
                logging.warning(f"Invalid config kind: {kind}")
            except Exception as e:
                logging.error(f"Error populating config: {name}, error: {e}")

    @overload
    def get_all_config_by_kind(
        self, kind: Literal["DagDefinition"]
    ) -> Sequence[DagDefinition]: ...

    # @overload Update here
    # def get_all_config_by_kind(
        # self, kind: Literal["TaskFactoryConfig"]
    # ) -> Sequence[TaskFactoryConfig]: ...

    def get_all_config_by_kind(self, kind: str) -> Sequence[BaseConfig]:
        """
            Get all config with given kind
        """
        return [config for config in self.configs.items() if config.kind == kind]

    def get_model_for_kind(self, kind: str) -> type[BaseConfig]:
        """
            Get model class for given kind
        """
        model: type[BaseConfig] = MODEL_MAPPING.get(kind, None)
        
        if not model:
            raise ValueError(f"Invalid config kind: {kind}")
        return model

    def register(self, config_obj: BaseConfig) -> None:
        """
            Register config object to registry
        """
        name: str = config_obj.metadata.name
        if name in self.configs:
            raise ValueError(f"Duplicate config name: {name}")
        logging.info(f"Register config: {name}")
        self.configs[name] = config_obj

    def get_by_name(self, name: str) -> BaseConfig:
        if name not in self.configs:
            raise ValueError(f"Config not found: {name}")
        return self.configs[name]
       
