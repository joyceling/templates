"""
## Galaxies ETL example DAG (with Task Groups)

This example demonstrates an ETL pipeline using Airflow and organizes related tasks using @task_group for clarity and maintainability.
The pipeline mocks data extraction for data about galaxies using a modularized
function, filters the data based on the distance from the Milky Way, and loads the
filtered data into a DuckDB database.
"""

from airflow.sdk import Asset, Param, dag, task, task_group
from pendulum import datetime, duration
from tabulate import tabulate
import pandas as pd
import duckdb
import logging
import os

from include.custom_functions.galaxy_functions import get_galaxy_data

t_log = logging.getLogger("airflow.task")

_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "galaxy_data")
_DUCKDB_TABLE_URI = f"duckdb://{_DUCKDB_INSTANCE_NAME}/{_DUCKDB_TABLE_NAME}"
_CLOSENESS_THRESHOLD_LY_DEFAULT = os.getenv("CLOSENESS_THRESHOLD_LY_DEFAULT", 500000)
_CLOSENESS_THRESHOLD_LY_PARAMETER_NAME = "closeness_threshold_light_years"
_NUM_GALAXIES_TOTAL = os.getenv("NUM_GALAXIES_TOTAL", 20)


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["example", "ETL"],
    params={
        _CLOSENESS_THRESHOLD_LY_PARAMETER_NAME: Param(
            _CLOSENESS_THRESHOLD_LY_DEFAULT,
            type="number",
            title="Galaxy Closeness Threshold",
            description="Set how close galaxies need ot be to the milkyway in order to be loaded to DuckDB.",
        )
    },
    is_paused_upon_creation=False,
)
def example_etl_galaxies():
    """
    Example ETL DAG using Task Groups to organize the Extract, Transform, and Load phases.
    """
    # ------------- #
    # Task Groups   #
    # ------------- #

    @task_group(group_id="extract_phase")
    def extract_phase():
        """Extracts raw galaxy data."""

        @task
        def extract_galaxy_data(
            num_galaxies: int = _NUM_GALAXIES_TOTAL,
        ) -> pd.DataFrame:
            galaxy_df = get_galaxy_data(num_galaxies)
            return galaxy_df

        return extract_galaxy_data()

    @task_group(group_id="transform_phase")
    def transform_phase(galaxy_df):
        """Filters/cleans the extracted galaxy data."""

        @task
        def transform_galaxy_data(galaxy_df: pd.DataFrame, **context):
            closeness_threshold_light_years = context["params"][
                _CLOSENESS_THRESHOLD_LY_PARAMETER_NAME
            ]
            t_log.info(
                f"Filtering for galaxies closer than {closeness_threshold_light_years} light years."
            )
            filtered_galaxy_df = galaxy_df[
                galaxy_df["distance_from_milkyway"] < closeness_threshold_light_years
            ]
            return filtered_galaxy_df

        return transform_galaxy_data(galaxy_df)

    @task_group(group_id="load_phase")
    def load_phase(filtered_galaxy_df):
        """Creates table (if needed) and loads filtered data into DuckDB."""

        @task(retries=2)
        def create_galaxy_table_in_duckdb(
            duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
            table_name: str = _DUCKDB_TABLE_NAME,
        ) -> None:
            t_log.info("Creating galaxy table in DuckDB.")
            os.makedirs(os.path.dirname(duckdb_instance_name), exist_ok=True)
            cursor = duckdb.connect(duckdb_instance_name)
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    name STRING PRIMARY KEY,
                    distance_from_milkyway INT,
                    distance_from_solarsystem INT,
                    type_of_galaxy STRING,
                    characteristics STRING
                )"""
            )
            cursor.close()
            t_log.info(f"Table {table_name} created in DuckDB.")

        @task(outlets=[Asset(_DUCKDB_TABLE_URI)])
        def load_galaxy_data(
            filtered_galaxy_df: pd.DataFrame,
            duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
            table_name: str = _DUCKDB_TABLE_NAME,
        ):
            t_log.info("Loading galaxy data into DuckDB.")
            cursor = duckdb.connect(duckdb_instance_name)
            cursor.sql(
                f"INSERT OR IGNORE INTO {table_name} BY NAME SELECT * FROM filtered_galaxy_df;"
            )
            t_log.info("Galaxy data loaded into DuckDB.")

        create = create_galaxy_table_in_duckdb()
        load = load_galaxy_data(filtered_galaxy_df)
        create >> load
        return load

    @task
    def print_loaded_galaxies(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ):
        cursor = duckdb.connect(duckdb_instance_name)
        near_galaxies_df = cursor.sql(f"SELECT * FROM {table_name};").df()
        near_galaxies_df = near_galaxies_df.sort_values(
            by="distance_from_milkyway", ascending=True
        )
        t_log.info(tabulate(near_galaxies_df, headers="keys", tablefmt="pretty"))

    # ------------- #
    # DAG Structure #
    # ------------- #

    raw_galaxy_df = extract_phase()
    filtered_galaxy_df = transform_phase(raw_galaxy_df)
    loaded = load_phase(filtered_galaxy_df)
    loaded >> print_loaded_galaxies()


example_etl_galaxies()
