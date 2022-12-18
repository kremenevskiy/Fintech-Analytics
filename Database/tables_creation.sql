CREATE TABLE IF NOT EXISTS Loans(
    loan_id BIGSERIAL NOT NULL PRIMARY KEY,
    client_id INT NOT NULL,
    date_loan_created TIMESTAMP NOT NULL,
    amount_loan INT NOT NULL,
    duration_days_loan INT NOT NULL,
    year_percentage_loan REAL NOT NULL,
    amount_loan_received INT NOT NULL,
    is_loan_closed BOOLEAN DEFAULT False NOT NULL
);


CREATE TABLE IF NOT EXISTS Loan_Payment(
    loan_id BIGINT REFERENCES Loans(loan_id),
    amount_loan_payment int NOT NULL,
    date_loan_replenishment TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS Deposits(
    deposit_id BIGSERIAL NOT NULL PRIMARY KEY,
    client_id INT NOT NULL,
    date_deposit_created TIMESTAMP NOT NULL,
    amount_deposit INT NOT NULL,
    duration_days_deposit INT NOT NULL,
    year_percentage_deposit REAL NOT NULL
);


CREATE TABLE IF NOT EXISTS Deposit_Replenishment(
    deposit_id BIGINT REFERENCES Deposits(deposit_id),
    amount_deposit_replenishment int NOT NULL,
    date_deposit_replenishment TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS Clients(
    client_id BIGSERIAL PRIMARY KEY NOT NULL,
--     credential_id INT,
--     personal_info_id INT,
--     client_address_id INT,
    phone VARCHAR(20) NOT NULL,
    date_created TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS Client_Logs(
    client_id INT NOT NULL,
    action_date TIMESTAMP NOT NULL,
    action_id int NOT NULL,
    info VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS ACTIONS(
    action_id BIGSERIAL PRIMARY KEY NOT NULL,
    action_name VARCHAR(50) UNIQUE NOT NULL
);


CREATE TABLE IF NOT EXISTS Client_Address(
    client_id INT NOT NULL,
    country VARCHAR(30),
    city VARCHAR(30),
    street_building VARCHAR(5),
    apartment INT,
    postal_code INT,
    date_added_client_address TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS Client_Identify_Document(
    client_id INT NOT NULL,
    passport_serial VARCHAR(20),
    date_issued TIMESTAMP,
    place_issued VARCHAR(20),
    country_issued VARCHAR(20),
    date_added_client_identify_Document TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS Client_PersonalInfo(
    client_id BIGINT NOT NULL,
    client_name VARCHAR(30) NOT NULL,
    client_surname VARCHAR(30) NOT NULL,
    date_of_birth DATE,
    place_of_birth VARCHAR(40),
    date_added_personal_info TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS Client_Cards(
    client_id INT NOT NULL,
    card_holder_name VARCHAR(30) NOT NULL,
    card_number BIGINT NOT NULL,
    card_date VARCHAR(5) NOT NULL,
    date_added_client_card TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS Transaction_extended(
    transaction_id BIGSERIAL PRIMARY KEY NOT NULL,
    client_id INT NOT NULL,
    trans_type VARCHAR(20),
    payee_phone VARCHAR(20) NOT NULL,
    payee_fullname VARCHAR(40) NOT NULL,
    is_received BOOLEAN DEFAULT False NOT NUll,
    trans_time_created TIMESTAMP NOT NULL,

    trans_amount INT NOT NULL,
    trans_amount_rubles INT NOT NULL,
    commission INT NOT NULL,
    currency_from VARCHAR(5) NOT NULL,
    currency_to VARCHAR(5) NOT NULL,
    exchange_rate REAL NOT NULL,

    country_from VARCHAR(10) NOT NULL,
    country_to VARCHAR(10) NOT NULL
);

drop table transactions;
CREATE TABLE IF NOT EXISTS Transactions(
    transaction_id BIGINT PRIMARY KEY NOT NULL,
    client_id INT NOT NULL,
    trans_type VARCHAR(20),
    payee_phone VARCHAR(20) NOT NULL,
    payee_fullname VARCHAR(40) NOT NULL,
    is_received BOOLEAN DEFAULT False NOT NUll,
    trans_time_created TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS Trans_Amounts(
    transaction_id INT NOT NULL,
    trans_amount INT NOT NULL,
    trans_amount_rubles INT NOT NULL,
    commission INT NOT NULL,
    currency_from VARCHAR(5) NOT NULL,
    currency_to VARCHAR(5) NOT NULL,
    exchange_rate REAL NOT NULL
);


CREATE TABLE IF NOT EXISTS Commission(
    country_from_to VARCHAR(20) UNIQUE NOT NULL,
    commission_amount INT NOT NULL,
    date_created_commission TIMESTAMP NOT NULL
);



CREATE TABLE IF NOT EXISTS Exchange_Rate(
    currency_from_to VARCHAR(10) UNIQUE NOT NULL,
    exchange_rate_amount INT NOT NULL,
    date_created_exchange_rate TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS  Trans_Forwarding(
    transaction_id INT NOT NULL,
    country_from VARCHAR(10) NOT NULL,
    country_to VARCHAR(10) NOT NULL
);


CREATE TABLE IF NOT EXISTS Trans_Type(
    type_id BIGSERIAL PRIMARY KEY NOT NULL,
    type_name VARCHAR(10) UNIQUE NOT NULL,
    started_from VARCHAR(10) NOT NULL
);


CREATE TABLE IF NOT EXISTS  P2P_Trans(
    transaction_id INT NOT NULL,
    payee_card_hash VARCHAR(30) NOT NULL,
    time_received_p2p TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS  P2B_Trans(
    transaction_id INT NOT NULL,
    time_received_p2b TIMESTAMP NOT NULL,
    bank VARCHAR(30) NOT NULL,
    payee_real_fullname VARCHAR(30) NOT NULL,
    payee_real_phone VARCHAR(30) NOT NULL
);

drop table analytic_runs;
CREATE TABLE IF NOT EXISTS Analytic_runs(
    last_unloaded_trans_id INT NOT NULL,
    date_unloading_run TIMESTAMP NOT NULL,
    cnt_transactions_pushed INT NOT NULl
);



