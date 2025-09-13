FROM apache/airflow:3.0.3


RUN python -m venv dbt_venv && source dbt_venv/bin/activate &&\
pip install --no-cache-dir dbt-duckdb dbt-snowflake && deactivate
COPY requirements.txt .

#install requirements
RUN pip install -r requirements.txt

# Add dbt project
COPY dbt/ /opt/airflow/dbt/
