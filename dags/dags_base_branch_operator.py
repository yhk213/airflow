from typing import Iterable
from airflow import DAG
from airflow.decorators import task
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator
import datetime
from airflow.utils.context import Context
import pendulum

with DAG (
    dag_id = 'dags_base_branch_operator',
    schedule= None,
    start_date = pendulum.datetime(2024,1,13, tz='Asia/Seoul'),
    catchup=False
) as dag: 
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random
            print(context)

            item_list = ['A', 'B', 'C']
            selected_item = random.choice(item_list)

            if selected_item == 'A':
                return 'task_a'
            else:
                return ['task_b', 'task_c']
            
    custom_branch_operator = CustomBranchOperator(task_id = 'python_branch_task')

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable=common_func,
        op_kwargs={'selected' : 'A'}
    )

    task_b = PythonOperator(
        task_id = 'task_b',
        python_callable=common_func,
        op_kwargs={'selected' : 'B'}
    )

    task_c = PythonOperator(
        task_id = 'task_c',
        python_callable=common_func,
        op_kwargs={'selected' : 'C'}
    )

    custom_branch_operator >> [task_a, task_b, task_c]
