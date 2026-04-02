import logging
from daglib.core.task_factory import TaskFactoryBase
from typing import Any
from airflow import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.sdk import Asset


logger = logging.getLogger(__name__)


class DummyTaskFactory(TaskFactoryBase):
    """
        Dummy task factory to be used for testing.
    """
    def validate_args(self, args: dict[str, Any]) -> None:
        """
            Validate the arguments passed to the task factory.
        """
        if "custom_dummy_arg1" in args:
            arg1 = args["custom_dummy_arg1"]
            if arg1 not in self.VALID_ARG1_VALUES:
                raise ValueError(f"Invalid custom_dummy_arg1 value: {arg1}")
        
        known_args = ["custom_dummy_arg1", "custom_dummy_arg2"]
        unknown_args = set(args.keys()) - known_args
        if unknown_args:
            logger.warning(f"Unknown arguments: {unknown_args}")

    def _create_task_impl(
        self,
        task_group_id: str, dag: Any, args: dict[str, Any]
    ):
        """
            Create a task group for the given task group ID
        """
        logger.info(f"Create task group: {task_group_id}")

        if "custom_dummy_arg1" in args:
            logger.debug(f"custom_dummy_arg1: {args['custom_dummy_arg1']}")
        if "custom_dummy_arg2" in args:
            logger.debug(f"custom_dummy_arg2: {args['custom_dummy_arg2']}")
        with TaskGroup(dag=dag, task_group_id=task_group_id) as task_group:
            task_1 = EmptyOperator(
                dag=dag,
                task_id="task_1",
                outlets=Asset("dummy://dummy_task_1"),
                doc_yaml="""
                    a: dummy_task_1
                    b: 2
                """,
            )

            task_2 = EmptyOperator(
                dag=dag,
                task_id="task_2",
                outlets=Asset("dummy://dummy_task_2"),
                doc_yaml="""
                    a: dummy_task_2
                    b: 2
                """
            )
            task_1 >> task_2
            return task_group
           
