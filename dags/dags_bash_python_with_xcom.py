from airflow import DAG
from airflow.decorators import task
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    dag_id='dags_bash_python_with_xcom',
    schedule = "0, 0, *, *, *",
    start_date = pendulum.datetime(2024, 1, 20, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status' : 'Good', 'data': [1,2,3]}
        return result_dict
    
    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env= {
            'STATUS' : '{{ ti.xcom_pull(task_ids = "python_push")["status"] }}',
            'DATA' : '{{ ti.xcom_pull(task_ids = "python_push")["data"] }}'
        },
        bash_command = 'echo $STATUS && echo $DATA'
    )
    
    python_push_xcom() >> bash_pull


    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command='echo PUSH_START'
                     '{{ ti.xcom_push(key="bash_pushed", value = 200) }} &&'
                     'echo PUSH_Complete'
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']
        status_value = ti.xcom_pull(key = 'bash_pushed')
        return_value = ti.xcom_pull(task_ids = 'bash_push')
        print(f'status value : ', {status_value})
        print(f'return_value : ', return_value)

  