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
