from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
with DAG(default_args={'start_date': datetime.date(2024, 1, 1)}, description='Example DAG for prerendering', dag_id='simple_prerender_dag') as dag:
    task_a = EmptyOperator()
    task_b = EmptyOperator()
    task_a >> task_b
