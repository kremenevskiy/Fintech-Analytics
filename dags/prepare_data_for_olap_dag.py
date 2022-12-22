import random
from datetime import datetime
from dateutil.relativedelta import relativedelta

from faker import Faker

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

import psycopg2
import numpy as np

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from query_for_join_transactions_tables import get_query_for_analytic_unloading
import logging

def get_last_transaction_in_analytic_table():
    with psycopg2.connect("host=localhost dbname=fintech user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * from analytic_runs order by last_unloaded_trans_id desc')
            analytic_data = np.array(cur.fetchall())
            if len(analytic_data) == 0:
                return None
            analytic_last_trans_id_unloaded = analytic_data[0, 0]
            return analytic_last_trans_id_unloaded


def get_last_transaction_id_transactions_table():
    with psycopg2.connect("host=localhost dbname=fintech user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * from transactions order by transaction_id desc')
            transactions_data = np.array(cur.fetchall())
            if len(transactions_data) == 0:
                return None
            transactions_data_last_trans_id = transactions_data[0, 0]
            return transactions_data_last_trans_id


with DAG(
    dag_id="data_to_analytic_format",
    start_date=datetime(2020, 2, 2),
    schedule_interval="*/20 * * * *",
    catchup=False,
) as dag:

    def branch_fn(**kwargs):
        last_unloaded_trans_id = get_last_transaction_in_analytic_table()
        transactions_data_last_trans_id = get_last_transaction_id_transactions_table()

        if transactions_data_last_trans_id is None:
            return 'transactions_data_empty'
        elif transactions_data_last_trans_id == last_unloaded_trans_id:
            return 'no_new_transactions'
        elif last_unloaded_trans_id is None:
            return 'unload_all_transactions'
        else:
            ti = kwargs['ti']
            ti.xcom_push(value=last_unloaded_trans_id, key='last_unloaded_trans_id')
            return 'unload_latest_transactions'


    def unload_transactions(**kwargs):

        path_to_jar_file = "/home/kremenevskij/postgres/postgresql-42.5.1.jar"
        spark = SparkSession \
            .builder \
            .appName("Spark streaming job") \
            .config("spark.jars", path_to_jar_file) \
            .getOrCreate()

        url = 'jdbc:postgresql://localhost:5432/fintech'
        properties = {'user': 'postgres', 'password': '1234', 'driver': "org.postgresql.Driver"}
        spark.read.jdbc(url=url, table='loans', properties=properties).createOrReplaceTempView("loans")
        spark.read.jdbc(url=url, table='Loan_Payment', properties=properties).createOrReplaceTempView("Loan_Payment")
        spark.read.jdbc(url=url, table='Deposits', properties=properties).createOrReplaceTempView("Deposits")
        spark.read.jdbc(url=url, table='Deposit_Replenishment', properties=properties).createOrReplaceTempView(
            "Deposit_Replenishment")
        spark.read.jdbc(url=url, table='Clients', properties=properties).createOrReplaceTempView("Clients")
        spark.read.jdbc(url=url, table='Client_Logs', properties=properties).createOrReplaceTempView("Client_Logs")
        spark.read.jdbc(url=url, table='ACTIONS', properties=properties).createOrReplaceTempView("ACTIONS")
        spark.read.jdbc(url=url, table='Client_Address', properties=properties).createOrReplaceTempView(
            "Client_Address")
        spark.read.jdbc(url=url, table='Client_Identify_Document', properties=properties).createOrReplaceTempView(
            "Client_Identify_Document")
        spark.read.jdbc(url=url, table='Client_PersonalInfo', properties=properties).createOrReplaceTempView(
            "Client_PersonalInfo")
        spark.read.jdbc(url=url, table='Client_Cards', properties=properties).createOrReplaceTempView("Client_Cards")
        spark.read.jdbc(url=url, table='Transactions', properties=properties).createOrReplaceTempView("Transactions")
        spark.read.jdbc(url=url, table='Trans_Amounts', properties=properties).createOrReplaceTempView("Trans_Amounts")
        spark.read.jdbc(url=url, table='Trans_Forwarding', properties=properties).createOrReplaceTempView(
            "Trans_Forwarding")
        spark.read.jdbc(url=url, table='Commission', properties=properties).createOrReplaceTempView("Commission")
        spark.read.jdbc(url=url, table='Exchange_Rate', properties=properties).createOrReplaceTempView("Exchange_Rate")
        spark.read.jdbc(url=url, table='Trans_Type', properties=properties).createOrReplaceTempView("Trans_Type")
        spark.read.jdbc(url=url, table='P2P_Trans', properties=properties).createOrReplaceTempView("P2P_Trans")
        spark.read.jdbc(url=url, table='P2B_Trans', properties=properties).createOrReplaceTempView("P2B_Trans")
        last_trans_id = kwargs['last_trans_id']
        analytic_data = spark.sql(get_query_for_analytic_unloading(last_trans_id=last_trans_id))
        analytic_data.write.jdbc(url=url, table="analytic_table", mode='append', properties=properties)
        cnt_pushed_trans = analytic_data.count()
        last_pushed_trans = analytic_data.sort(F.col('transaction_id').desc()).collect()[0][1]
        return cnt_pushed_trans, last_pushed_trans


    def unload_all_transactions(**kwargs):
        cnt_pushed_trans, last_pushed_trans = unload_transactions(last_trans_id=-1)
        ti = kwargs['ti']
        ti.xcom_push(value=cnt_pushed_trans, key='cnt_pushed_trans')
        ti.xcom_push(value=last_pushed_trans, key='last_pushed_trans')


    def unload_latest_transactions(**kwargs):
        last_unloaded_trans_id = kwargs['ti'].xcom_pull(task_ids='check_transactions_table', key='last_unloaded_trans_id')
        logging.info(f'last unloaded transaction id: {last_unloaded_trans_id}')
        cnt_pushed_trans, last_pushed_trans = unload_transactions(last_trans_id=last_unloaded_trans_id)
        ti = kwargs['ti']
        ti.xcom_push(value=cnt_pushed_trans, key='cnt_pushed_trans')
        ti.xcom_push(value=last_pushed_trans, key='last_pushed_trans')


    def generate_query_for_insert_to_analytic_table(**kwargs):
        ti = kwargs['ti']
        cnt_pushed_trans = ti.xcom_pull(task_ids='unload_latest_transactions', key='cnt_pushed_trans')
        last_pushed_trans = ti.xcom_pull(task_ids='unload_latest_transactions', key='last_pushed_trans')
        logging.info(f'unload_unload_latest_transactions cnt: {cnt_pushed_trans}')
        logging.info(f'unload_unload_latest_transactions last: {last_pushed_trans}')
        if cnt_pushed_trans is None:
            cnt_pushed_trans = ti.xcom_pull(task_ids='unload_all_transactions', key='cnt_pushed_trans')
            last_pushed_trans = ti.xcom_pull(task_ids='unload_all_transactions', key='last_pushed_trans')
            logging.info(f'unload_all_transactions cnt: {cnt_pushed_trans}')
            logging.info(f'unload_all_transactions last: {last_pushed_trans}')
        time_now = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        unload_results_query = f"""
            INSERT INTO analytic_runs (last_unloaded_trans_id, date_unloading_run, cnt_transactions_pushed)
            VALUES ({last_pushed_trans}, '{time_now}', {cnt_pushed_trans})
        """
        return unload_results_query



    on_check_transactions_table = BranchPythonOperator(
        task_id='check_transactions_table',
        python_callable=branch_fn
    )

    on_transactions_data_empty = BashOperator(
        task_id="transactions_data_empty",
        bash_command="echo TRANSACTIONS TABLE IS EMPTY",
    )

    on_no_new_transactions = BashOperator(
        task_id="no_new_transactions",
        bash_command="echo NO NEW TRANSACTIONS TO LOAD",
    )

    on_unload_all_transactions = PythonOperator(
        task_id='unload_all_transactions',
        python_callable=unload_all_transactions,
    )

    on_unload_latest_transactions = PythonOperator(
        task_id='unload_latest_transactions',
        python_callable=unload_latest_transactions,
    )

    make_sql_for_analytic_table = PythonOperator(
        task_id='make_sql_for_analytic_table',
        python_callable=generate_query_for_insert_to_analytic_table,
        do_xcom_push=True,
        trigger_rule='one_success',
        provide_context=True
    )

    query_for_analytic_runs = """
            {{ ti.xcom_pull(task_ids='make_sql_for_analytic_table') }}
        """
    push_results_to_analytic_table = PostgresOperator(
        task_id='push_results_to_analytic_table',
        postgres_conn_id="postgres_connection",
        sql=f"""{query_for_analytic_runs}""",
    )

    end_dag = BashOperator(
        task_id="end_dag",
        bash_command="echo OLAP PIPELINE ENDED",
        trigger_rule='one_success',
    )

    on_check_transactions_table >> on_transactions_data_empty >> end_dag
    on_check_transactions_table >> on_no_new_transactions >> end_dag
    on_check_transactions_table >> on_unload_all_transactions
    on_check_transactions_table >> on_unload_latest_transactions
    on_unload_all_transactions >> make_sql_for_analytic_table
    on_unload_latest_transactions >> make_sql_for_analytic_table
    make_sql_for_analytic_table >> push_results_to_analytic_table >> end_dag





