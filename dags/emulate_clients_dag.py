import random
from datetime import datetime
from dateutil.relativedelta import relativedelta

from faker import Faker

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2
import numpy as np

import logging
logger = logging.getLogger("airflow.task")

hours_probabilities = {
    0: 0.01,
    1: 0.02,
    2: 0.03,
    3: 0.03,
    4: 0.04,
    5: 0.05,
    6: 0.06,
    7: 0.07,
    8: 0.05,
    9: 0.06,
    10: 0.07,
    11: 0.10,
    12: 0.15,
    13: 0.17,
    14: 0.20,
    15: 0.23,
    16: 0.25,
    17: 0.21,
    18: 0.16,
    19: 0.12,
    20: 0.07,
    21: 0.06,
    22: 0.02,
    23: 0.01
}

time_now = datetime.now()
fake = Faker()

def make_noisy_date(date):
    return date + relativedelta(seconds=random.randint(0, 50))


def make_client():
    phone = fake.msisdn()[1:]
    time = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    return phone, time


def take_existing_clients_ids(p=0.01):
    with psycopg2.connect("host=localhost dbname=fintech user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * from clients')
            clients_ids = np.array(cur.fetchall())[:, 0]
            clients_ids = np.random.choice(clients_ids, size=int(len(clients_ids) * p))
    return clients_ids


def make_client_personal_info():
    name, surname = fake.first_name(), fake.last_name()
    date_of_birth = str(fake.date_of_birth())
    place_of_birth = (fake.country() + ', ' + fake.street_address()).replace("'", "")[:39]
    date_added = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    return name, surname, date_of_birth, place_of_birth, date_added


def make_client_identify_document():
    passport_serial = fake.pystr_format().upper()
    date_issued = fake.date()
    place_issued = (fake.street_address()).replace("'", "")[:19]
    country_issued = (fake.country()).replace("'", "")[:19]
    date_added = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    return passport_serial, date_issued, place_issued, country_issued, date_added


def make_client_cards():
    card_holder_name = (fake.name()).upper()
    card_number = fake.credit_card_number(card_type='mastercard')
    card_date = fake.credit_card_expire()
    date_added_client_card = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    return card_holder_name, card_number, card_date, date_added_client_card


def make_loan():
    date_loan_created = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    amount_loan = int(np.random.exponential(scale=1000000))
    duration_days_loan = abs(int(np.random.normal(loc=360, scale=200)))
    year_percentage_loan = random.uniform(0.05, 0.2)
    amount_loan_received = 0
    is_loan_closed = False
    return date_loan_created, amount_loan, duration_days_loan, year_percentage_loan, amount_loan_received, is_loan_closed


def take_existing_loans_ids_for_payment(p=0.01):
    with psycopg2.connect("host=localhost dbname=fintech user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * from loans WHERE is_loan_closed = False')
            loans_ids = np.array(cur.fetchall())
            if len(loans_ids) > 0:
                loans_ids = loans_ids[:, 0]
                loans_ids = np.random.choice(loans_ids, size=int(len(loans_ids) * p))
                return loans_ids
            else:
                return []



def make_loan_payment(loan_id):
    amount_loan_payment = int(np.random.exponential(scale=100000))
    date_loan_replenishment = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    return loan_id, amount_loan_payment, date_loan_replenishment


def make_deposit():
    date_deposit_created = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    amount_deposit = int(np.random.exponential(scale=10000000))
    duration_days_deposit = abs(int(np.random.normal(loc=360, scale=200)))
    year_percentage_deposit = random.uniform(0.05, 0.2)
    return date_deposit_created, amount_deposit, duration_days_deposit, year_percentage_deposit


def take_existing_deposits_ids_for_replenishment(p=0.01):
    with psycopg2.connect("host=localhost dbname=fintech user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * from deposits')
            deposit_ids = np.array(cur.fetchall())
            if len(deposit_ids) > 0:
                deposit_ids = deposit_ids[:, 0]
                deposit_ids = np.random.choice(deposit_ids, size=int(len(deposit_ids) * p))
                return deposit_ids
            else:
                return []


def make_deposit_replenishment(deposit_id):
    amount_deposit_replenishment = int(np.random.exponential(scale=100000))
    date_deposit_replenishment = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    return deposit_id, amount_deposit_replenishment, date_deposit_replenishment


def get_existing_actions(n_actions):
    with psycopg2.connect("host=localhost dbname=fintech user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * from actions')
            actions_ids = np.array(cur.fetchall())
            if len(actions_ids) > 0:
                actions_ids = actions_ids[:, 0]
                actions_ids = random.choices(deposit_ids, k=n_actions)
                return actions_ids
            else:
                return []

def make_client_logs(client_id, action_id):
    action_date = make_noisy_date(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    action_id = action_id
    info = fake.catch_phrase()
    return client_id, action_date, action_id, info



with DAG(
    dag_id="emulate_clients",
    start_date=datetime(2020, 2, 2),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    # make clients
    current_clients = [make_client() for _ in range(1000) if hours_probabilities[time_now.hour] > random.random()]
    current_clients = sorted(current_clients, key=lambda x: x[1])

    clients_sqls = "".join([f"INSERT INTO Clients (phone, date_created) VALUES ({record[0]}, '{record[1]}');" for record in current_clients])

    # make clients personal info
    clients_ids = take_existing_clients_ids()
    clients_personal_info = [[client_id, *make_client_personal_info()] for client_id in clients_ids]
    clients_personal_info = sorted(clients_personal_info, key=lambda x: x[3])
    clients_personal_info_sqls = "".join([
        f"INSERT INTO client_personalinfo (client_id, client_name, client_surname, date_of_birth, place_of_birth, date_added_personal_info)"
        f" VALUES ({record[0]}, '{record[1]}', '{record[2]}', '{record[3]}', '{record[4]}', '{record[5]}');" for record in clients_personal_info])

    # make clients identify documents
    clients_ids = take_existing_clients_ids()
    clients_identify_documents = [[client_id, *make_client_identify_document()] for client_id in clients_ids]
    clients_identify_documents = sorted(clients_identify_documents, key=lambda x: x[5])
    clients_identify_documents_sqls = "".join([
        f"INSERT INTO client_identify_document (client_id, passport_serial, date_issued, place_issued, country_issued, date_added)"
        f" VALUES ({record[0]}, '{record[1]}', '{record[2]}', '{record[3]}', '{record[4]}', '{record[5]}');"
        for record in clients_identify_documents])


    # make clients cards
    clients_ids = take_existing_clients_ids()
    clients_cards = [[client_id, *make_client_cards()] for client_id in clients_ids]
    clients_cards = sorted(clients_cards, key=lambda x: x[4])
    clients_cards_sqls = "".join([
        f"INSERT INTO client_cards (client_id, card_holder_name, card_number, card_date, date_added_client_card)"
        f" VALUES ({record[0]}, '{record[1]}', '{record[2]}', '{record[3]}', '{record[4]}');"
        for record in clients_cards])


    # make client loan
    clients_ids = take_existing_clients_ids(p=0.001)
    clients_loans = [[client_id, *make_loan()] for client_id in clients_ids]
    clients_loans = sorted(clients_loans, key=lambda x: x[1])
    clients_loans_sqls = "".join([
        f"INSERT INTO loans (client_id, date_loan_created, amount_loan, duration_days_loan, year_percentage_loan, amount_loan_received, is_loan_closed)"
        f" VALUES ({record[0]}, '{record[1]}', {record[2]}, {record[3]}, {record[4]}, {record[5]}, {record[6]});"
        for record in clients_loans])


    # make clients Loan_Payment
    loan_ids = take_existing_loans_ids_for_payment()
    if len(loan_ids) > 0:
        clients_loan_payments = [make_loan_payment(loan_id) for loan_id in loan_ids]
        clients_loan_payments = sorted(clients_loan_payments, key=lambda x: x[2])
        clients_loan_payments_sqls = "".join([
            f"INSERT INTO loan_payment (loan_id, amount_loan_payment, date_loan_replenishment)"
            f" VALUES ({record[0]}, {record[1]}, '{record[2]}');"
            for record in clients_loan_payments])
    else:
        clients_loan_payments_sqls = ""


    # make deposit
    clients_ids = take_existing_clients_ids(p=0.001)
    clients_deposits = [[client_id, *make_deposit()] for client_id in clients_ids]
    clients_deposits = sorted(clients_deposits, key=lambda x: x[1])
    clients_deposits_sqls = "".join([
        f"INSERT INTO deposits (client_id, date_deposit_created, amount_deposit, duration_days_deposit, year_percentage_deposit)"
        f" VALUES ({record[0]}, '{record[1]}', {record[2]}, {record[3]}, {record[4]});"
        for record in clients_deposits])


    # make clients deposit_replenishment
    deposit_ids = take_existing_deposits_ids_for_replenishment()
    if len(deposit_ids) > 0:
        clients_deposit_replenishments = [make_deposit_replenishment(deposit_id) for deposit_id in deposit_ids]
        clients_deposit_replenishments = sorted(clients_deposit_replenishments, key=lambda x: x[2])
        clients_deposit_replenishments_sqls = "".join([
            f"INSERT INTO deposit_replenishment (deposit_id, amount_deposit_replenishment, date_deposit_replenishment)"
            f" VALUES ({record[0]}, {record[1]}, '{record[2]}');"
            for record in clients_deposit_replenishments])
    else:
        clients_deposit_replenishments_sqls = ''



    # generate client actions logs
    clients_ids = take_existing_clients_ids(p=0.01)
    client_action_ids = get_existing_actions(len(clients_ids))
    client_action_logs = [make_client_logs(client_id, action_id) for client_id, action_id in zip(clients_ids, client_action_ids)]
    clients_actions_logs_sqls = "".join([
            f"INSERT INTO client_logs (client_id, action_date, action_id, info)"
            f" VALUES ({record[0]}, '{record[1]}', {record[2]}, '{record[3]}');"
            for record in client_action_logs])



    emulate_client = PostgresOperator(
        task_id='emulate_client',
        postgres_conn_id="postgres_connection",
        sql=f"""
            {clients_sqls}
          """,
    )

    emulate_client_personal_info = PostgresOperator(
        task_id='emulate_client_personal_info',
        postgres_conn_id="postgres_connection",
        sql=f"""
            {clients_personal_info_sqls}
              """,
    )

    emulate_client_identify_documents = PostgresOperator(
        task_id='emulate_client_identify_documents',
        postgres_conn_id="postgres_connection",
        sql=f"""
            {clients_identify_documents_sqls}
            """,
    )

    emulate_client_cards = PostgresOperator(
        task_id='emulate_client_cards',
        postgres_conn_id="postgres_connection",
        sql=f"""
                {clients_cards_sqls}
            """,
    )

    emulate_loan = PostgresOperator(
        task_id='emulate_loan',
        postgres_conn_id="postgres_connection",
        sql=f"""
                {clients_loans_sqls}
            """,
    )

    emulate_loan_payment = PostgresOperator(
        task_id='emulate_loan_payment',
        postgres_conn_id="postgres_connection",
        sql=f"""
                {clients_loan_payments_sqls}
            """,
    )

    emulate_deposit = PostgresOperator(
        task_id='emulate_deposit',
        postgres_conn_id="postgres_connection",
        sql=f"""
                {clients_deposits_sqls}
            """,
    )

    emulate_deposit_replenishments = PostgresOperator(
        task_id='emulate_deposit_replenishments',
        postgres_conn_id="postgres_connection",
        sql=f"""
                {clients_deposit_replenishments_sqls}
            """,
    )

    emulate_client_logs = PostgresOperator(
        task_id='emulate_client_logs',
        postgres_conn_id="postgres_connection",
        sql=f"""
                    {clients_actions_logs_sqls}
                """,
    )

    emulate_client >> emulate_client_personal_info
    emulate_client >> emulate_client_identify_documents
    emulate_client >> emulate_client_cards
    emulate_client >> emulate_loan >> emulate_loan_payment
    emulate_client >> emulate_client_logs
    emulate_client >> emulate_deposit >> emulate_deposit_replenishments