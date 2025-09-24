# dbt_project_env.py
from dotenv import load_dotenv, find_dotenv
import os
import subprocess


def run_dbt():
    dotenv_path = find_dotenv()  # searches upward from current dir
    if dotenv_path:
        load_dotenv(dotenv_path)
        AWS_USER = os.environ.get("AIRFLOW_UID")
        print(AWS_USER)
    subprocess.run(["dbt"] + os.sys.argv[1:])


if __name__ == "__main__":
    run_dbt()
