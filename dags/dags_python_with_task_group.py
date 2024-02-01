from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id = 'dags_python_with_task_group',
    start_date=pendulum.datetime(2024,1,30, tz='Asia/Seoul'),
    schedule = None,
    catchup= False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        '''  task_group 데커레이터를 이용한 첫번째 그룹 '''

        @task(task_id = 'inner_function1')
        def inner_function1(**kwargs):
            print('첫번째 task_group 내 1st task')

        inner_function2 = PythonOperator(
            task_id = 'inner_function2',
            python_callable = inner_func,
            op_kwargs={'msg' : '1st taskgroup 내 2nd task' }
        )

        inner_function1() >> inner_function2

    with TaskGroup(group_id = 'second_group', tooltip = '2nd Group !') as group_2:
        ''' 여기에 적은 docstring은 표시되지 않습니다'''

        @task(task_id='inner_func1')
        def inner_ftn1(**kwargs):
            print('2nd task_group, 1st-task')
        
        inner_ftn2 = PythonOperator(
            task_id = 'inner_func2',
            python_callable=inner_func,
            op_kwargs={'msg' : '2nd task_group, 2nd-task'}
        )

        inner_ftn1() >> inner_ftn2

    group_1() >> group_2


