from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import json
import os
from datetime import timedelta, datetime
from airflow.utils.trigger_rule import TriggerRule as TriggerRule

path = os.environ["AIRFLOW_HOME"]
dag_name = "bees_ingestion_dag"
users_email_list = ["rodasecalotas@gmail.com"]

Variable.set("users_email_list", json.dumps(users_email_list))   #saving as a json variable so it is easier to use as a list in later steps
Variable.set("dag_name", dag_name)

default_args = {
                "owner": "admin",
                "depends_on_past": False,
                "email": users_email_list,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": timedelta(seconds=5)
                }

# Define the DAG, its ID and when should it run.
dag = DAG(
            dag_id = dag_name,
            start_date=datetime(year=2024, month=12, day=15, hour=10),
            schedule_interval="0 23 * * *",
            default_args=default_args,
            catchup=False
            )

# Define Task 1 (insert the data into the database)
task1 = BashOperator(
                     task_id="api_to_landing_data",
                     bash_command=f"python {path}/dags/src/api_to_landing_data.py",
                     dag=dag,
                    )

# Define Task 2 (run initial tests on the extracted data)
task2 = BashOperator(
                     task_id="landing_tests",
                     bash_command=f"python {path}/dags/tests/landing_tests.py",
                     dag=dag,
                    )

# Define Task 3 (ingest data from landing to bronze)
task3 = BashOperator(
                     task_id="landing_to_bronze",
                     bash_command=f"python {path}/dags/src/landing_to_bronze_data.py",
                     dag=dag,
                    )

# Define Task 4 (ingest data from bronze to silver)
task4 = BashOperator(
                     task_id="bronze_to_silver",
                     bash_command=f"python {path}/dags/src/bronze_to_silver_data.py",
                     dag=dag,
                    )

# Define Task 5 (send email to users alerting about CRUD events)
task5 = BashOperator(
                     task_id="send_email_crud",
                     bash_command=f"python {path}/dags/utils/email_sender_crud.py",
                     dag=dag,
                    )

# Define Task 6 (create agg view - as table - in gold layer)
task6 = BashOperator(
                     task_id="silver_to_gold",
                     bash_command=f"python {path}/dags/src/silver_to_gold_data.py",
                     dag=dag,
                    )

# Define Task 7 (send email to users in case of failure on previous steps)
task7 = BashOperator(
                     task_id="send_email_error",
                     bash_command=f"python {path}/dags/utils/email_sender_error.py",
                     trigger_rule=TriggerRule.ONE_FAILED,
                     dag=dag,
                    )



task1 >> task2 >> task3 >> [task4, task5]
task4 >> task6 >> task7