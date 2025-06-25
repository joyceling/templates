"""
Custom Airflow Operator to clone (copy) data into Snowflake using a provided SQL query.

This operator uses Airflow's SnowflakeHook to execute arbitrary SQL queries, enabling safe credential
managment via Airflow connections. Best practice: use parametrized SQL or external template files.

Example Usage in a DAG:

    from include.snowflake_clone_operator import SnowflakeCloneOperator

    copy_task = SnowflakeCloneOperator(
        task_id="clone_data",
        sql="CREATE TABLE my_clone AS SELECT * FROM my_source_table;",
        snowflake_conn_id="my_snowflake_conn",
    )

"""

from airflow.sdk import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.context import Context
import logging


class SnowflakeCloneOperator(BaseOperator):
    """
    Executes a cloning (copy) SQL statement in Snowflake.

    :param sql: The SQL query to be executed in Snowflake (typically CREATE TABLE ... AS SELECT ...)
    :type sql: str
    :param snowflake_conn_id: The Airflow connection ID for Snowflake
    :type snowflake_conn_id: str
    :param parameters: (optional) Query parameters to interpolate
    :type parameters: dict or None
    """

    template_fields = ("sql",)

    def __init__(
        self,
        sql: str,
        snowflake_conn_id: str = "snowflake_default",
        parameters: dict = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.snowflake_conn_id = snowflake_conn_id
        self.parameters = parameters

    def execute(self, context: Context):
        self.log.info("Executing Snowflake clone/copy query:")
        self.log.info(self.sql)
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        try:
            result = hook.run(self.sql, parameters=self.parameters)
            self.log.info("Snowflake query ran successfully.")
            return result  # (typically None, or a status)
        except Exception as e:
            logging.error(f"Failed to execute SQL in Snowflake: {e}")
            raise
