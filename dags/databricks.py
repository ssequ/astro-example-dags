from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

dag = DAG(
  dag_id="databricks_helloworld_dag",
  schedule_interval="@daily",
  start_date=days_ago(1),
  catchup=False,
  default_args={"owner": "airflow"}
)

databricks_job = DataBricksRunNowOperator(
  task_id="helloworld_job",
  job_name="helloworld",
  dag=dag
)

databricks_job
