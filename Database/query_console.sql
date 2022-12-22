select * from client_logs;

SELECT *
from clients
inner join
    client_identify_document cid
        on clients.client_id = cid.client_id
where passport_serial is not NULL;


select *
from client_identify_document;


select *
from transaction_extended;


drop table client_logs;
drop table client_address;
drop table client_identify_document;
drop table client_personalinfo;
drop table deposit_replenishment;
drop table loan_payment;
drop table loans;
drop table deposits;
drop table client_cards;
drop table clients;
drop table trans_forwarding;
drop table trans_amounts;
drop table transaction_extended;
drop table p2b_trans;
drop table p2p_trans;
drop table transactions;
drop table analytic_runs;
drop table analytic_table;


select count(*)
from deposits;

select count(*)
from loans;

select count(*)
from client_identify_document;


select *
from p2p_trans;
select *
from p2b_trans;

drop table transactions;
drop table transaction_extended;
drop table trans_amounts;
drop table trans_forwarding;
drop table p2p_trans;
drop table p2b_trans;

select *
from transactions;

select *
    from analytic_runs;



drop table client_personalinfo;
drop table client_identify_document;


select *
from transactions t
inner join trans_type
on t.trans_type LIKE trans_type.type_name;

select * from client_personalinfo;

WITH client_personal_cte AS (SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            client_id
                        ORDER BY
                            date_added_personal_info desc
                        ) row_num_personal_info
             FROM client_personalinfo),
client_personal_unique AS (
    select client_id, client_name, client_surname, date_of_birth, place_of_birth, date_added_personal_info
    from client_personal_cte
    where row_num_personal_info = 1
),
client_identify_document_cte AS (SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            client_id
                        ORDER BY
                            date_added_client_identify_document desc
                        ) row_num_identify_document
             FROM client_identify_document),
client_identify_document_unique AS (
    select client_id, passport_serial, date_issued, place_issued, country_issued, date_added_client_identify_Document
    from client_identify_document_cte
    where row_num_identify_document = 1
),
client_address_cte AS (SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            client_id
                        ORDER BY
                            date_added_client_address desc
                        ) row_num_address
             FROM Client_Address),
client_address_unique AS (
    select client_id, country, city, street_building, apartment, postal_code, date_added_client_address
    from client_address_cte
    where row_num_address = 1
),
client_cards_cte AS (SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            client_id
                        ORDER BY
                            date_added_client_card desc
                        ) row_num_client_card
             FROM Client_Cards),
client_cards_unique AS (
    select client_id, card_holder_name, card_number, card_date, date_added_client_card
    from client_cards_cte
    where row_num_client_card = 1
),
loans_cte AS (SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            client_id
                        ORDER BY
                            date_loan_created desc
                        ) row_num_loans
             FROM Loans),
loans_unique AS (
    select client_id, loan_id, date_loan_created, amount_loan, duration_days_loan, year_percentage_loan, amount_loan_received, is_loan_closed
    from loans_cte
    where row_num_loans = 1
),
deposit_cte AS (SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            client_id
                        ORDER BY
                            date_deposit_created desc
                        ) row_num_deposits
             FROM Deposits),
deposit_unique AS (
    select client_id, deposit_id, date_deposit_created, amount_deposit, duration_days_deposit, year_percentage_deposit
    from deposit_cte
    where row_num_deposits = 1
)
select *
from transactions as t
    left join trans_type
        on t.trans_type LIKE trans_type.type_name
    left join P2P_Trans
        on t.transaction_id = P2P_Trans.transaction_id
    left join P2B_Trans
        on t.transaction_id = P2B_Trans.transaction_id
    left join Trans_Forwarding
        on t.transaction_id = Trans_Forwarding.transaction_id
    left join Trans_Amounts
        on t.transaction_id = Trans_Amounts.transaction_id
    left join clients
        on t.client_id = clients.client_id
    left join client_cards_unique
        on t.client_id = client_cards_unique.client_id
    left join client_personal_unique
        on t.client_id = client_personal_unique.client_id
    left join client_identify_document_unique
        on t.client_id = client_identify_document_unique.client_id
    left join client_address_unique
        on t.client_id = client_address_unique.client_id
    left join deposit_unique
        on t.client_id = deposit_unique.client_id
    left join loans_unique
        on t.client_id = loans_unique.client_id;



select *
from test;

select count(*)
from test;
-- 1089

drop table test;

SELECT * from client_identify_document;
SELECT * from client_identify_document;

select * from analytic_runs;

select * from analytic_table;

SELECT * FROM clients limit 11;

drop table analytic_table;
select * from analytic_table;

select * from analytic_runs;
select count(*)
from analytic_table;


select * from transactions
order by transaction_id desc
limit(5);

select count(*)
from transactions;

select * from analytic_table
order by transaction_id desc
limit(5);


SELECT
  type_name, COUNT(trans_type)
FROM
  analytic_table
GROUP BY
  type_name;


select count(*) from analytic_table;
select count(*)
from analytic_table
where is_received = True;

SELECT coalesce(type_name,'Not received'), count(*)
FROM analytic_table
GROUP BY type_name;

Select date_trunc('hour', date_created), count(*) from analytic_table group by 1;


select count(*) * 0.01
from clients;


select * from analytic_runs;


select * from clients;

select * from transactions;

select count(*)
from clients
where client_id not in (select client_id from transactions);

delete from clients where
    (client_id not in (select client_id from transactions)) and
    (client_id not in (select client_id from deposits)) and
    (client_id not in (select client_id from loans));

with cte as (
select client_id from loans
union all select client_id from deposits
union all select client_id from transactions
union all select client_id from client_identify_document
union all select client_id from client_personalinfo
union all select client_id from client_address
union all select client_id from client_cards
union all select client_id from client_logs)
delete from clients where client_id not in (select client_id from cte limit 3);
select client_id from cte limit 3;
select *
from cte
where client_id = 2178964;

drop table clients;





select *
from deposits
where client_id = 2178964;



select count(*)
from clients;

select count(*)
from transactions;


select * from clients;