def get_query_for_analytic_unloading(last_trans_id):

    transactions_join_sql = f"""
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
),
transacttions_to_unload as (
    select * from transactions
    where transaction_id > {last_trans_id}
)
select *
from transacttions_to_unload as t
    left join trans_type
        on t.trans_type LIKE trans_type.type_name
    left join P2P_Trans
        USING (transaction_id)
    left join P2B_Trans
        USING (transaction_id)
    left join Trans_Forwarding
        USING (transaction_id)
    left join Trans_Amounts
        USING (transaction_id)
    left join clients
        USING (client_id)
    left join client_cards_unique
        USING (client_id)
    left join client_personal_unique
        USING (client_id)
    left join client_identify_document_unique
        USING (client_id)
    left join client_address_unique
        USING (client_id)
    left join deposit_unique
        USING (client_id)
    left join loans_unique
        USING (client_id);
"""
    return transactions_join_sql