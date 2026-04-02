import logging
from .config import ConfigRegistry
from .task_factory import TaskFactoryRegistry

logger = logging.getLogger(__name__)


class DagBuilder:
    def __init__(self, 
        config_registry = ConfigRegistry(),
        task_factory_registry = TaskFactoryRegistry(),
    ):
        self.config_registry = config_registry
        # self.task_factory_registry = task_factory_registry
        self.dag_factory = SingletonDagFactory(task_factory_registry)

    
       
