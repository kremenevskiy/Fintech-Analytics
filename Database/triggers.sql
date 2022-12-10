CREATE OR REPLACE FUNCTION add_loan_payment() RETURNS TRIGGER AS
$BODY$
declare
    total_sum int;
BEGIN
    UPDATE Loans SET amount_loan_received = NEW.amount_loan_payment + Loans.amount_loan_received WHERE (Loans.loan_id = NEW.loan_id);
    UPDATE Loans SET is_loan_closed = True WHERE (Loans.loan_id = NEW.loan_id) AND Loans.amount_loan_received >= loans.amount_loan;
    RETURN new;
END;
$BODY$
language plpgsql;

-- drop trigger if exists trigger_t on Loan_Payment;

CREATE TRIGGER trigger_on_loan_payment
AFTER INSERT
ON Loan_Payment
FOR EACH ROW
EXECUTE PROCEDURE add_loan_payment();


CREATE OR REPLACE FUNCTION add_deposit_replenishment() RETURNS TRIGGER AS
$BODY$
declare
    total_sum int;
BEGIN
    UPDATE Deposits SET amount_deposit = NEW.amount_deposit_replenishment + Deposits.amount_deposit WHERE (Deposits.deposit_id = NEW.deposit_id);
    RETURN new;
END;
$BODY$
language plpgsql;

-- drop trigger if exists trigger_t on Loan_Payment;

CREATE TRIGGER trigger_on_deposit_replenishment
AFTER INSERT
ON Deposit_Replenishment
FOR EACH ROW
EXECUTE PROCEDURE add_deposit_replenishment();


