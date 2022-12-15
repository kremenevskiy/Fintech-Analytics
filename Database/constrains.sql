ALTER TABLE Loans
ADD CONSTRAINT constraint_fk_client_loan
FOREIGN KEY (client_id)
REFERENCES Clients (client_id);

ALTER TABLE Deposits
ADD CONSTRAINT constraint_fk_client_deposit
FOREIGN KEY (client_id)
REFERENCES Clients (client_id);


ALTER TABLE Client_logs
ADD CONSTRAINT constraint_fk_client_client_logs
FOREIGN KEY (client_id)
REFERENCES Clients (client_id);




ALTER TABLE Client_address
ADD CONSTRAINT constraint_fk_client_client_address
FOREIGN KEY (client_id)
REFERENCES Clients (client_id);


ALTER TABLE client_identify_document
ADD CONSTRAINT constraint_fk_client_client_identify_document
FOREIGN KEY (client_id)
REFERENCES Clients (client_id);


ALTER TABLE client_personalinfo
ADD CONSTRAINT constraint_fk_client_client_personalinfo
FOREIGN KEY (client_id)
REFERENCES Clients (client_id);


ALTER TABLE client_cards
ADD CONSTRAINT constraint_fk_client_cards
FOREIGN KEY (client_id)
REFERENCES Clients (client_id);


ALTER TABLE transactions
ADD CONSTRAINT constraint_fk_client_transaction
FOREIGN KEY (client_id)
REFERENCES Clients (client_id);


ALTER TABLE trans_amounts
ADD CONSTRAINT constraint_fk_transaction_trans_amount
FOREIGN KEY (transaction_id)
REFERENCES Transactions (transaction_id);


ALTER TABLE trans_forwarding
ADD CONSTRAINT constraint_fk_transaction_trans_forwarding
FOREIGN KEY (transaction_id)
REFERENCES Transactions (transaction_id);


ALTER TABLE transactions
ADD CONSTRAINT constraint_fk_trans_type_transaction
FOREIGN KEY (type_id)
REFERENCES trans_type (type_id);


ALTER TABLE p2p_trans
ADD CONSTRAINT constraint_fk_transaction_p2p_trans
FOREIGN KEY (transaction_id)
REFERENCES transactions (transaction_id);


ALTER TABLE p2b_trans
ADD CONSTRAINT constraint_fk_transaction_p2b_trans
FOREIGN KEY (transaction_id)
REFERENCES transactions (transaction_id);


ALTER TABLE client_logs
ADD CONSTRAINT constraint_fk_actions_client_logs
FOREIGN KEY (action_id)
REFERENCES actions (action_id);







