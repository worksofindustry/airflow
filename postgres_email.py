from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.hooks import postgres_default


args = {
    'owner': 'linker',
    'retries': 1,
    'email_on_failure': True,
    'start_date': datetime(2019, 6, 20),
    'depends_on_past': False,
    'max_active_runs': 1,
    'email': ['worksofindustry@gmail.com'],
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('test-email',
          default_args=args,
          schedule_interval="* * * *"
          )

t1 = BashOperator(
    task_id='copy_document',
    bash_command='cp /media/T/Deployment/MSSQL/CrewWorkPlanning.xlsx /media/R/FTP/CrewWorkPlanning.xlsx',
    retries=1,
    dag=dag
)

t2 = send_email(to=["worksofindustry@gmail.com"],
                subject="Testing DAG Run Confirmation",
                html_content="<h3>Welcome to Airflow</h3>",
                dag=dag
                )
t1 >> t2
