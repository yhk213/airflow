from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator 

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BashOperator(
        task_id="t1_oragnge",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )
    
    bash_t1 >> bash_t2