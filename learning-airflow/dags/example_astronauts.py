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
    doc_md="""
    ## Astronaut ETL (Extract/Validate/Load): Example Airflow 3.0+ DAG

    This DAG demonstrates a typical data pipeline pattern:
    - **Extracts** a list of astronauts from a public API
    - **Validates** the data structure/schema immediately
    - **Prints** information for each astronaut in parallel (using Airflow 3.0 Task Mapping)
    - **Produces an asset** ([see Airflow asset lineage](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/datasets.html)) named `current_astronauts`

    **How this DAG works:**
    1. `extract_astronaut_data` fetches live astronaut info from http://api.open-notify.org/astros.json (falls back to static data if the API is unreachable).
    2. `validate_astronaut_data` checks the input to ensure it’s a list of dicts containing `name` and `craft` keys.
    3. `print_astronaut_and_craft` is dynamically mapped to run once for each astronaut. Each mapped task logs an individual astronaut and their craft.

    **Sample Run Example (Log output):**
    - "Sergey Prokopyev is in space flying on the ISS! Hello! :)"
    - "Jeanette Epps is in space flying on the SpaceX Dragon! Hello! :)"

    **Asset Output Example (`current_astronauts`):**
    ```python
    [{'craft': 'ISS', 'name': 'Sergey Prokopyev'}, ...]
    ```
    The output is tracked as a data asset in Airflow for lineage and downstream pipelines.

    **Failure Handling:**
    If the API is down or data is missing fields, the DAG will:
    - Fall back to test data (extract task)
    - Raise an error and fail quickly with descriptive logs (validation task)

    ---

    **Tip:** You can view XCom push/pull data on each task instance in the Airflow UI under the 'XCom' tab. Each mapped task run for `print_astronaut_and_craft` will appear as a separate line in the Grid view.

    **See also:** https://docs.astronomer.io/learn/get-started-with-airflow for a step-by-step tutorial.
    """,
    is_paused_upon_creation=False,
)
def example_astronauts():
    """
    ## Astronaut ETL (Extract/Validate/Load): Example Airflow 3.0+ DAG

    This DAG demonstrates a typical data pipeline pattern:
    - Extract list of astronauts from a public API
    - Validate the data structure/schema
    - Print info for each astronaut in parallel using Airflow 3.0 dynamic task mapping
    - Produces an asset: `current_astronauts` (see Airflow asset lineage docs)

    Usage Example:
        $ airflow dags trigger example_astronauts

    Expected Output (Logs):
        Sergei Prokopyev is in space flying on the ISS! Hello! :)
        Jeanette Epps is in space flying on the SpaceX Dragon! Hello! :)

    Asset Output (for downstream DAGs):
        [ {'craft': 'ISS', 'name': 'Sergey Prokopyev'}, ... ]

    Failure cases:
    - API unreachable: static fallback data is used
    - Data fails validation: DAG fails early with clear message

    XCom:
    - extract_astronaut_data pushes key: number_of_people_in_space (viewable in Airflow UI)
    """

    # ------------------- #
    # Tasks
    # ------------------- #

    @task(
        outlets=[Asset("current_astronauts")],
        task_id="extract_astronaut_data",
        doc_md="""
        ### Extract Astronaut Data

        Fetches the current list of astronauts in space from the Open Notify API.
        If the API request fails (network issue, non-2xx code), returns static fallback data instead.

        **Returns:**
        A list of astronaut dictionaries in the format:
        ```python
        [{'craft': 'ISS', 'name': 'Sergey Prokopyev'}, ...]
        ```

        **XCom Pushes (usage):**
        - Key: `number_of_people_in_space`
        - Value: Integer (the number of astronauts, e.g. 10)

        **API Example Response:**
        ```json
        {
          "people": [
            {"craft": "ISS", "name": "Sergey Prokopyev"},
            {"craft": "ISS", "name": "Jeanette Epps"}
          ],
          "number": 2,
          "message": "success"
        }
        ```

        **Fallback Data Used Example:**
        ```python
        [
          {"craft": "ISS", "name": "Marco Alain Sieber"},
          {"craft": "ISS", "name": "Claude Nicollier"}
        ]
        ```

        > **UI Tip:** To see the value pushed to XCom: Open this task’s latest run in the UI, select the 'XCom' tab, look for key `number_of_people_in_space`.
        """,
    )
    def extract_astronaut_data(**context) -> list[dict]:
        # Try real API, fallback on error -- this makes the DAG robust to connection problems
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

        # Push astronaut count to XCom for easy inspection in Airflow UI
        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task(
        task_id="validate_astronaut_data",
        doc_md="""
        ### Validate Astronaut Data

        Checks the astronaut data structure, ensuring:
        - The input is a non-empty list
        - Each element is a dict with non-empty string fields `name` and `craft`

        **Typical Input Example:**
        ```python
        [{'craft': 'ISS', 'name': 'Jeanette Epps'}, {'craft': 'ISS', 'name': 'Oleg Artemyev'}]
        ```

        **Failure Cases:**
        - Input is `None`, not a list, or empty list
        - An element is not a dict
        - 'name' or 'craft' missing, or empty string

        **On failure:** Raises an Exception to stop the DAG early and make debugging easier in the UI.
        **Returns:** Cleaned/validated list of astronaut dictionaries (same as input if valid).

        > **Best practice:** Validate data as early as possible in your pipeline. You can expand this function to check for duplicates, forbidden names, etc.
        """,
    )
    def validate_astronaut_data(astronaut_data: list[dict]) -> list[dict]:
        # Validate structure as soon as possible in pipeline
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
        ### Print Astronaut and Craft Info (Mapped Task)

        Prints/logs each astronaut in the input list using Airflow 3.x dynamic task mapping.
        Runs one mapped task per dictionary in `person_in_space`.

        **Arguments:**
        - `person_in_space`: dict, e.g. `{'craft': 'ISS', 'name': 'Jeanette Epps'}`
        - `greeting`: str, default "Hello! :)"

        **Example log output:**
        ```
        Jeanette Epps is in space flying on the ISS! Hello! :)
        ```

        **Dynamic Task Mapping Example:**
        If three astronauts are passed:
        ```python
        input = [{'craft': 'ISS', 'name': 'A'}, {'craft': 'Soyuz', 'name': 'B'}, {'craft': 'ISS', 'name': 'C'}]
        ```
        Airflow will create three mapped task instances; each will process one dict from the input list.

        > **UI Tip:** Each mapped task instance appears as a numbered row in the Grid View.
        """,
    )
    def print_astronaut_and_craft(
        person_in_space: dict, greeting: str = "Hello! :)"
    ) -> None:
        # Mapped task: Airflow creates one instance per astronaut, running in parallel
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
