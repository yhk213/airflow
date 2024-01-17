from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator 
from common.common_func import regist2


with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag: 
    
    regist_t2 = PythonOperator(
        task_id='regist_t2',
        python_callable=regist2,
        op_args=['yhk', 'male', 'Korea', 'Seoul'],
        op_kwargs={'email' : 'abc@gmail.net', 'phone': '010-000-0000'}
    )

    regist_t2