"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow.sdk import Asset, dag, task
from pendulum import datetime, duration
import requests

# -------------- #
# DAG Definition #
# -------------- #


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(seconds=5),
    },
    tags=["example", "space"],
    is_paused_upon_creation=False,
)
def example_astronauts():
    """
    DAG that extracts the list of astronauts currently in space using the Open Notify API, and prints their names and spacecraft.

    - Schedule: Daily, starting April 1, 2025
    - Purpose: Demonstration of the Airflow TaskFlow API, including dynamic task mapping for parallel operations.
    - Workflow:
      1. Extract astronaut data (with fallback to hardcoded if API is unavailable),
      2. Print a greeting for each astronaut and their craft in a mapped parallel task.

    Each run reflects the current number of astronauts, with parallel printing using Airflow's dynamic task mapping. This is a realistic data integration (extract & report) pattern and showcases robust error fallback, documentation, and dataset tracking.
    """
    # ---------------- #
    # Task Definitions #
    # ---------------- #

    @task(outlets=[Asset("current_astronauts")])
    def get_astronauts(**context) -> list[dict]:
        """
        Retrieves a list of astronauts currently in space (from Open Notify API).
        - Returns: List of dictionaries with astronaut names and crafts.
        - XCom: Pushes the count of astronauts as XCom for downstream use.
        - Fallback: If the API is not available, returns simulated astronaut data.
        - Asset: Tracks the produced dataset for use by downstream scheduled assets/DAGs.
        """
        try:
            r = requests.get("http://api.open-notify.org/astros.json")
            r.raise_for_status()
            number_of_people_in_space = r.json()["number"]
            list_of_people_in_space = r.json()["people"]
        except Exception as e:
            print(f"API unavailable, using hardcoded astronaut data. Error: {e}")
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Marco Alain Sieber"},
                {"craft": "ISS", "name": "Claude Nicollier"},
            ]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        Prints the name and spacecraft of a single astronaut, using the info returned from get_astronauts.
        Prints a custom greeting. This task is dynamically mapped to run once for each astronaut found.
        - Inputs:
            greeting (str): Greeting message to prepend
            person_in_space (dict): Dictionary with astronaut info (craft and name)
        - Output: None; only logs output to standard out
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]
        print(f"{name} is in space flying on the {craft}! {greeting}")

    # ------------------------------------ #
    # Calling tasks + Setting dependencies #
    # ------------------------------------ #

    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=get_astronauts()
    )


# Instantiate the DAG
example_astronauts()
