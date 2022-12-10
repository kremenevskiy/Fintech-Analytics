CREATE TABLE IF NOT EXISTS Loans(
    loan_id BIGSERIAL NOT NULL PRIMARY KEY,
    client_id INT NOT NULL,
    date_loan_created TIMESTAMP NOT NULL,
    amount_loan INT NOT NULL,
    duration_days_loan INT NOT NULL,
    year_percentage_loan REAL NOT NULL,
    amount_loan_received INT NOT NULL,
    is_loan_closed BOOLEAN NOT NULL
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





