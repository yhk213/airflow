from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum

with DAG(
    dag_id = 'dags_bash_with_variable',
    schedule = "10 9 * * *",
    start_date = pendulum.datetime(2024,1,20, tz="Asia/Seoul"),
    catchup = False
) as dag:
    var_value = Variable.get('sample_key')

    bash_1 = BashOperator(
        task_id = 'bash_1',
        bash_command= f"echo variable {var_value}"
    )

    bash_2 = BashOperator(
        task_id = "bash_2",
        bash_command="echo variable : {{var.value.sample_key}}"
    )

    bash_1 >> bash_2