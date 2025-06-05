import datetime
import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    default_args={"start_date": datetime.datetime(2024, 1, 1)},
    description="Example DAG for prerendering",
    dag_id="simple_prerender_dag",
) as dag:
    task_a = EmptyOperator(task_id="task_a")
    task_b = EmptyOperator(task_id="task_b")
    task_a >> task_b
