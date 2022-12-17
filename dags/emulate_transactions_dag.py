import random
from datetime import datetime
from dateutil.relativedelta import relativedelta

from faker import Faker

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

import psycopg2
import numpy as np
from emulate_clients_dag import make_noisy_date

import logging
fake = Faker()
logger = logging.getLogger("airflow.task")


def get_verified_clients_ids(p=0.01):
    with psycopg2.connect("host=localhost dbname=fintech user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * from client_identify_document')
            clients_ids = np.array(cur.fetchall())[:, 0]
            clients_ids = np.random.choice(clients_ids, size=int(len(clients_ids) * p))
    return clients_ids


def get_available_trans_types():
    with psycopg2.connect("host=localhost dbname=fintech user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * from trans_type')
            available_trans_types_ids = np.array(cur.fetchall())[:, 0]
    return available_trans_types_ids


def make_transaction(client_id, trans_type_id):
    transaction = {}
    transaction['client_id'] = client_id
    transaction['trans_type_id'] = trans_type_id
    transaction['payee_phone'] = phone = fake.msisdn()[1:]
    transaction['payee_fullname'] = fake.name()
    transaction['is_received'] = False
    transaction['trans_time_created'] = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    transaction['country_from'] = fake.country_code('alpha-3')
    transaction['country_to'] = fake.country_code('alpha-3')
    transaction['currency_from'] = fake.currency_code()
    transaction['currency_to'] = fake.currency_code()
    transaction['trans_amount'] = abs(int(np.random.normal(loc=150000, scale=250000)))
    return transaction


def handle_transaction(transaction):
    # add comission and exchange_rate from tables
    transaction['commission'] = 0
    transaction['exchange_rate'] = 1
    transaction['trans_amount_rubles'] = transaction['trans_amount'] * transaction['exchange_rate']
    return transaction



def get_unreceived_transactions(p=0.01):
    with psycopg2.connect("host=localhost dbname=fintech user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * from transactions where is_received = False')
            trans_ids = np.array(cur.fetchall())
            if len(trans_ids) > 0:
                trans_ids = trans_ids[:, 0]
                trans_ids = np.random.choice(trans_ids, size=int(len(trans_ids) * p))
                return trans_ids
            else:
                return []


def make_transaction_p2p_received(trans_id):
    p2p_info = {}
    p2p_info['transaction_id'] = trans_id
    p2p_info['payee_card_hash'] = fake.credit_card_number(card_type='mastercard')
    p2p_info['time_received'] = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    return p2p_info


def make_transaction_p2b_received(trans_id):
    p2b_info = {}
    p2b_info['transaction_id'] = trans_id
    p2b_info['time_received'] = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    p2b_info['bank'] = fake.company()
    p2b_info['payee_real_fullname'] = fake.name()
    p2b_info['payee_real_phone'] = fake.msisdn()[1:]
    return p2b_info



with DAG(
    dag_id="emulate_transactions",
    start_date=datetime(2020, 2, 2),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    verified_clients_ids = get_verified_clients_ids()
    available_trans_types_ids = get_available_trans_types()

    generated_trans_types = random.choices(available_trans_types_ids, k=len(verified_clients_ids))
    transactions = [handle_transaction(make_transaction(client_id, trans_type)) for client_id, trans_type in zip(verified_clients_ids, generated_trans_types)]

    transactions_sql = "".join([
        f"INSERT INTO Transaction_extended (client_id, type_id, payee_phone, payee_fullname, is_received, trans_time_created, trans_amount, trans_amount_rubles, commission, currency_from, currency_to, exchange_rate, country_from, country_to)"
        f" VALUES ({t['client_id']}, '{t['trans_type_id']}', '{t['payee_phone']}', '{t['payee_fullname']}', {t['is_received']}, '{t['trans_time_created']}', {t['trans_amount']}, {t['trans_amount_rubles']}, {t['commission']}, '{t['currency_from']}', '{t['currency_to']}', {t['exchange_rate']}, '{t['country_from']}', '{t['country_to']}');"
        for t in transactions])


    # generate p2p clients
    trans_unreceived_ids = get_unreceived_transactions()
    if len(trans_unreceived_ids) > 0:
        p2p_received = [make_transaction_p2p_received(trans_id) for trans_id in trans_unreceived_ids]
        p2p_received_sqls = "".join([
            f"INSERT INTO p2p_trans (transaction_id, payee_card_hash, time_received)"
            f" VALUES ({p2p_t['transaction_id']}, '{p2p_t['payee_card_hash']}', '{p2p_t['time_received']}');"
            for p2p_t in p2p_received])
    else:
        p2p_received_sqls = ''

    # generate p2b clients
    trans_unreceived_ids = list(set(get_unreceived_transactions()) - set(trans_unreceived_ids))
    if len(trans_unreceived_ids) > 0:
        p2b_received = [make_transaction_p2b_received(trans_id) for trans_id in trans_unreceived_ids]
        p2b_received_sqls = "".join([
            f"INSERT INTO p2b_trans (transaction_id, time_received, bank, payee_real_fullname, payee_real_phone)"
            f" VALUES ({p2b_t['transaction_id']}, '{p2b_t['time_received']}', '{p2b_t['bank']}', '{p2b_t['payee_real_fullname']}', '{p2b_t['payee_real_phone']}');"
            for p2b_t in p2b_received])
    else:
        p2b_received_sqls = ''


    emulate_transaction = PostgresOperator(
        task_id='emulate_transaction',
        postgres_conn_id="postgres_connection",
        sql=f"""
            {transactions_sql}
          """,
    )

    emulate_p2p_trans_received = PostgresOperator(
        task_id='emulate_p2p_trans_received',
        postgres_conn_id="postgres_connection",
        sql=f"""
                {p2p_received_sqls}
              """,
    )

    emulate_p2b_trans_received = PostgresOperator(
        task_id='emulate_p2b_trans_received',
        postgres_conn_id="postgres_connection",
        sql=f"""
                   {p2b_received_sqls}
                 """,
    )

    emulate_transaction >> emulate_p2p_trans_received >>  emulate_p2b_trans_received
