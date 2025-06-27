"""
Astronaut ETL Example DAG (Beginner Edition)
============================================

This DAG is designed as an educational example for anyone learning Airflow via the TaskFlow API. It demonstrates:
- How to build and document a DAG using modern Airflow APIs
- Making an API call within a task to fetch the list of astronauts currently in space
- Data quality validation as a separate task
- Airflow's dynamic task mapping to process each astronaut in parallel
- Use of Airflow Assets (Datasets) to enable data-driven scheduling (Airflow 2.6+)

For more information, visit the Astronomer and Airflow documentation links throughout the code.

Further learning:
- Astronomer: https://docs.astronomer.io/learn/get-started-with-airflow
- Airflow TaskFlow API: https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
- Dynamic Task Mapping: https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html#dynamic-task-mapping
- Asset (Dataset) Scheduling: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/datasets.html

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow.sdk import Asset, dag, task
from pendulum import datetime, duration
import requests

# -------------- #
# DAG Definition #
# -------------- #


@dag(
    start_date=datetime(2025, 4, 1),  # When can this DAG be scheduled?
    schedule="@daily",  # How often? '@daily' means once per day.
    max_consecutive_failed_dag_runs=5,  # Auto-pause if this DAG fails 5 times in a row
    doc_md="""
    # Astronaut ETL Example DAG
    This beginner-friendly DAG demonstrates:
    - Use of the TaskFlow API (`@dag`, `@task` decorators)
    - Calling an external API to fetch data
    - Data quality validation in a separate task
    - Dynamic task mapping to process results in parallel
    - Asset-aware outputs for cross-DAG scheduling
    Explore the code comments to learn how each Airflow feature works!
    [Learn more about writing clean Airflow DAGs](https://docs.astronomer.io/learn/airflow-best-practices)
    """,
    default_args={
        "owner": "Astro",  # For Airflow UI owners field
        "retries": 3,  # Retry each task up to 3 times on failure
        "retry_delay": duration(seconds=5),  # Wait 5 seconds between retries
    },
    tags=["example", "space"],
    is_paused_upon_creation=False,  # Start immediately after being parsed
)
def example_astronauts():
    """
    This DAG fetches a list of astronauts currently in space and prints a message for each.
    Steps:
      1. Get the astronaut data from an open API
      2. Validate the data structure (data quality step)
      3. Print each astronaut and their respective craft using dynamic task mapping
    """

    @task(
        outlets=[
            Asset("current_astronauts")
        ],  # Declares this task outputs a Dataset/Asset
    )
    def get_astronauts(**context) -> list[dict]:
        """
        Fetches the current list of astronauts in space from the Open Notify API.
        Returns a list of dictionaries (one per astronaut) or fallback demo data on error.
        - Demonstrates making HTTP requests in a task
        - Shows use of XCom for passing extra metadata downstream
        - Outputs an Airflow Asset to enable asset-driven scheduling
        """
        try:
            response = requests.get("http://api.open-notify.org/astros.json")
            response.raise_for_status()
            data = response.json()
            astronauts = data["people"]
            n = data["number"]
        except Exception as e:
            print(f"API not available: {e} -- using demo astronauts.")
            astronauts = [
                {"craft": "ISS", "name": "Demo Astronaut A"},
                {"craft": "ISS", "name": "Demo Astronaut B"},
            ]
            n = len(astronauts)
        # Store count in XCom for possible later use
        context["ti"].xcom_push(key="number_of_people_in_space", value=n)
        return astronauts

    @task
    def validate_astronauts(astronauts: list[dict]) -> list[dict]:
        """
        Data Quality Check: Ensures astronaut data is a non-empty list of dicts with
        'name' and 'craft' keys. If validation fails, the DAG run fails immediately.
        """
        if not isinstance(astronauts, list):
            raise ValueError("Returned data is not a list.")
        if not astronauts:
            raise ValueError("Received empty astronaut data.")
        for i, record in enumerate(astronauts):
            if not isinstance(record, dict):
                raise ValueError(f"Astronaut #{i} is not a dict: {record}")
            for field in ("name", "craft"):
                if field not in record:
                    raise ValueError(f"Astronaut #{i} missing '{field}' field.")
        # (Educational note: You could do more checks here.)
        return astronauts

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        Print the name and craft for a person in space.
        This is dynamically mapped over all astronauts, so runs once per person.
        Args:
          greeting: Static greeting
          person_in_space: Dict with keys 'name', 'craft'
        """
        name = person_in_space["name"]
        craft = person_in_space["craft"]
        print(f"{name} is in space aboard the {craft}! {greeting}")

    # ----- Pipeline Structure ----- #
    # Step 1: Download astronauts
    raw_astronauts = get_astronauts()
    # Step 2: Validate
    valid_astronauts = validate_astronauts(raw_astronauts)
    # Step 3: Use dynamic task mapping to print info for each astronaut concurrently
    print_astronaut_craft.partial(greeting="Hello from Earth!").expand(
        person_in_space=valid_astronauts
    )
    # (Educational note: .partial() sets static params, .expand() runs task for each item in list)


# Instantiate your DAG (for Airflow to discover it)
example_astronauts()
