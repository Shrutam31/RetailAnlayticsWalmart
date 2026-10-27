from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag()
def orchestrate():

    @task
    def ingest_cdc():
        return " CDC data ingested"

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"
    
    @task.bash
    def source_freshness():
        # Manually setting working directory to walmart_project as dbt source freshness command needs to be run from the project directory
        return "cd /opt/airflow/walmart_project && dbt source freshness"


    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_t"
    )    

    silver_technical_tests = BashOperator(
            task_id="silver_technical_tests",
            cwd="/opt/airflow/walmart_project",
            bash_command="dbt test --select silver_t"
        )   

    silver_business = BashOperator(
            task_id="silver_business",
            cwd="/opt/airflow/walmart_project",
            bash_command="dbt run --select silver_b"
        )   

    silver_business_tests = BashOperator(
            task_id="silver_business_tests",
            cwd="/opt/airflow/walmart_project",
            bash_command="dbt test --select silver_b"
        )   

    gold_epehemeral = BashOperator(
        task_id="gold_epehemeral",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/ephemeral"
    )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt snapshot"
    )

    gold_fact = BashOperator(
        task_id="gold_fact",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/fact"
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_epehemeral >> gold_dimensions >> gold_fact

orchestrate_dag = orchestrate()