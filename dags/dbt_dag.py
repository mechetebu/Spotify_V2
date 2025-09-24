from airflow.decorators import dag, task
from datetime import datetime
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.profiles import DuckDBUserPasswordProfileMapping


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "cosmos", "taskflow"],
)
def cosmos_with_taskflow():

    @task
    def pre_process():
        print("Do something before running dbt...")

    @task
    def post_process():
        print("Do something after running dbt...")

    dbt_project = "/opt/airflow/dbt/Spotify_V2/"

    # Define the dbt task group using Cosmos
    dbt_group = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(dbt_project_path="/opt/airflow/dbt/Spotify_V2/"),
        profile_config=ProfileConfig(
            profile_name="Spotify_V2",
            profiles_yml_filepath=dbt_project + "profiles.yml",
            target_name="dev",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"
        ),
    )

    # TaskFlow-style dependency chaining
    pre_process() >> dbt_group >> post_process()


cosmos_with_taskflow()
