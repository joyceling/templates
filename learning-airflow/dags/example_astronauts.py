"""
# Astronaut ETL Example DAG (with Data Quality Checks)

This DAG retrieves the current list of astronauts in space from the Open Notify API, validates the result, and prints each astronaut’s name and craft.

**Author**: Astro
**Support**: support@astro.io
**Tutorial**: https://docs.astronomer.io/learn/get-started-with-airflow
**Related Assets**: current_astronauts (produced)

---

#### DAG Workflow:
1. **Extract** astronaut data from the API (fallback to sample data if unavailable).
2. **Validate** the list for structure and expected content.
3. **Print** each astronaut and their craft using dynamic task mapping.

> This example demonstrates: TaskFlow API usage, XCom, dynamic task mapping, asset lineage, and data quality validation in Airflow 3.0.0.

![Space Station](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow.sdk import Asset, dag, task
from pendulum import datetime, duration
import requests
import logging

# --------------------- #
# DAG Definition
# --------------------- #


@dag(
    dag_id="example_astronauts",
    description=(
        "Queries live astronaut data from Open Notify API. Performs data validation, "
        "dynamically maps a print task per astronaut, and tracks an asset for lineage. "
        "See doc_md for full walkthrough and support information."
    ),
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_consecutive_failed_dag_runs=3,
    concurrency=10,
    default_args={
        "owner": "Astro",
        "email": ["support@astro.io"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": duration(seconds=10),
        # Add SLA of 10 minutes for demo purposes
        "sla": duration(minutes=10),
    },
    tags=["example", "space", "etl", "demo", "asset", "data-quality"],
    doc_md=__doc__,
    is_paused_upon_creation=False,
)
def example_astronauts():
    """
    ## Astronaut ETL (Extract/Validate/Load)

    This Airflow DAG retrieves astronaut info from a public API, validates it, and logs summaries.

    - **Asset Produced**: current_astronauts
    - **Failover**: Falls back to static test data if API down.
    - **Data Quality**: Fails DAG early if invalid data structure detected.
    - **Dynamic Mapping Example**: Demonstrates Airflow 3.x dynamic task mapping on print tasks.

    For more, see: [Astronomer Quickstart](https://docs.astronomer.io/learn/get-started-with-airflow)
    """

    # ------------------- #
    # Tasks
    # ------------------- #

    @task(
        outlets=[Asset("current_astronauts")],
        task_id="extract_astronaut_data",
        doc_md="""
          ### Extract Astronaut Data

          Connects to the Open Notify API to fetch the list of astronauts in space. If unreachable, uses fallback test data.

          - **Pushes**: astronaut count to XCom (key: number_of_people_in_space)
          - **Produces**: Asset `current_astronauts`
          """,
    )
    def extract_astronaut_data(**context) -> list[dict]:
        try:
            r = requests.get("http://api.open-notify.org/astros.json", timeout=10)
            r.raise_for_status()
            astronaut_api_response = r.json()
            number_of_people_in_space = astronaut_api_response["number"]
            list_of_people_in_space = astronaut_api_response["people"]
        except Exception as e:
            logging.warning(
                f"API unavailable, using fallback astronaut data. Error: {e}"
            )
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Marco Alain Sieber"},
                {"craft": "ISS", "name": "Claude Nicollier"},
            ]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task(
        task_id="validate_astronaut_data",
        doc_md="""
          ### Validate Astronaut Data

          Checks that astronaut data is non-empty, has valid structure, and each entry contains a name and craft.

          - **Fails DAG** if invalid structure or missing fields detected.
          - **Returns** the clean data for downstream dynamic mapping.
          """,
    )
    def validate_astronaut_data(astronaut_data: list[dict]) -> list[dict]:
        logging.info(
            f"Validating astronaut data quality. Data received: {astronaut_data}"
        )
        if not isinstance(astronaut_data, list) or not astronaut_data:
            raise ValueError("Astronaut data is missing or not a non-empty list.")
        for idx, astronaut in enumerate(astronaut_data):
            if not isinstance(astronaut, dict):
                raise TypeError(
                    f"Entry at index {idx} is not a dictionary: {astronaut}"
                )
            if (
                "name" not in astronaut
                or not isinstance(astronaut["name"], str)
                or not astronaut["name"].strip()
            ):
                raise ValueError(
                    f"Missing or invalid 'name' at index {idx}: {astronaut}"
                )
            if (
                "craft" not in astronaut
                or not isinstance(astronaut["craft"], str)
                or not astronaut["craft"].strip()
            ):
                raise ValueError(
                    f"Missing or invalid 'craft' at index {idx}: {astronaut}"
                )
        logging.info("Astronaut data quality validated successfully.")
        return astronaut_data

    @task(
        task_id="print_astronaut_and_craft",
        doc_md="""
          ### Print Astronaut and Craft Info (Mapped)

          Logs each astronaut's craft and name with a greeting. Runs in parallel using Airflow 3.0 task mapping.
          """,
    )
    def print_astronaut_and_craft(
        person_in_space: dict, greeting: str = "Hello! :)"
    ) -> None:
        craft = person_in_space["craft"]
        name = person_in_space["name"]
        logging.info(f"{name} is in space flying on the {craft}! {greeting}")

    # --------------
    # Set up workflow
    # --------------

    astronaut_data = extract_astronaut_data()
    validated_data = validate_astronaut_data(astronaut_data)
    print_astronaut_and_craft.partial(greeting="Hello! :)").expand(
        person_in_space=validated_data
    )


# Instantiate the DAG
example_astronauts()
