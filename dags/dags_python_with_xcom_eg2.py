from airflow import DAG
import pendulum
from airflow.decorators import task


with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id = 'python_xcom_push_by_return')
    def xcom_push(**kwargs):
        transaction_value = 'status Good'
        return transaction_value

    @task(task_id = 'python_xcom_pull_by_return')
    def xcom_pull_return(**kwargs):
        ti = kwargs['ti']
        pull_value = ti.xcom_pull(key='return_value', task_ids = 'python_xcom_push_by_return')
        print(pull_value)

    xcom_push() >> xcom_pull_return()
