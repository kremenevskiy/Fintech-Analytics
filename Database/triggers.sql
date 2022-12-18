-- When loan replenished
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

drop trigger if exists trigger_on_loan_payment on Loan_Payment;

CREATE TRIGGER trigger_on_loan_payment
AFTER INSERT
ON Loan_Payment
FOR EACH ROW
EXECUTE PROCEDURE add_loan_payment();


-- When deposit replenished
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

drop trigger if exists trigger_on_deposit_replenishment on Deposit_Replenishment;

CREATE TRIGGER trigger_on_deposit_replenishment
AFTER INSERT
ON Deposit_Replenishment
FOR EACH ROW
EXECUTE PROCEDURE add_deposit_replenishment();


-- When payment received

drop trigger if exists trigger_on_p2p_received on p2p_trans;
drop trigger if exists trigger_on_p2b_received on p2b_trans;

CREATE OR REPLACE FUNCTION on_p2p_trans_received() RETURNS TRIGGER AS
$BODY$
declare
    total_sum int;
BEGIN
    UPDATE transactions SET is_received = True WHERE (transactions.transaction_id = NEW.transaction_id);
    UPDATE transactions SET trans_type = 'CRD2CSH' WHERE (transactions.transaction_id = NEW.transaction_id);
    RETURN new;
END;
$BODY$
language plpgsql;


CREATE OR REPLACE FUNCTION on_p2b_trans_received() RETURNS TRIGGER AS
$BODY$
declare
    total_sum int;
BEGIN
    UPDATE transactions SET is_received = True WHERE (transactions.transaction_id = NEW.transaction_id);
    UPDATE transactions SET trans_type = 'CRD2BANK' WHERE (transactions.transaction_id = NEW.transaction_id);
    RETURN new;
END;
$BODY$
language plpgsql;

CREATE TRIGGER trigger_on_p2p_received
AFTER INSERT
ON p2p_trans
FOR EACH ROW
EXECUTE PROCEDURE on_p2p_trans_received();

CREATE TRIGGER trigger_on_p2b_received
AFTER INSERT
ON p2b_trans
FOR EACH ROW
EXECUTE PROCEDURE on_p2b_trans_received();




-- Triggers for Transactions auto insertion

drop trigger if exists trigger_on_new_transaction_created on transaction_extended;

CREATE OR REPLACE FUNCTION handle_new_transaction() RETURNS TRIGGER AS
$BODY$
declare
    total_sum int;
BEGIN
    INSERT INTO transactions (transaction_id, client_id, payee_phone, payee_fullname, is_received, trans_time_created) VALUES (new.transaction_id, new.client_id, new.payee_phone, new.payee_fullname, new.is_received, new.trans_time_created);
    INSERT INTO trans_amounts (transaction_id, trans_amount, trans_amount_rubles, commission, currency_from, currency_to, exchange_rate) VALUES (new.transaction_id, new.trans_amount, new.trans_amount_rubles, new.commission, new.currency_from, new.currency_to, new.exchange_rate);
    INSERT INTO trans_forwarding (transaction_id, country_from, country_to) VALUES (new.transaction_id, new.country_from, new.country_to);
    DELETE FROM transaction_extended WHERE transaction_id = new.transaction_id;
    RETURN new;
END;
$BODY$
language plpgsql;

CREATE TRIGGER trigger_on_new_transaction_created
AFTER INSERT
ON transaction_extended
FOR EACH ROW
EXECUTE PROCEDURE handle_new_transaction();








