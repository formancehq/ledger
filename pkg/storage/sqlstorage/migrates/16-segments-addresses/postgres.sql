--statement
create table "VAR_LEDGER_NAME".segments (
    segment_parts varchar[],
    posting_index int,
    transaction_id int,
    is_source boolean,
    primary key (segment_parts, posting_index, transaction_id, is_source)
);
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".create_segments() RETURNS void AS $$
DECLARE
    tx record;
    posting jsonb;
    postingIndex int;
BEGIN
    FOR tx IN (
        select id, postings
        from "VAR_LEDGER_NAME".transactions
    ) LOOP
        postingIndex = 0;
        for posting in (select jsonb_array_elements(tx.postings)) loop
            insert into "VAR_LEDGER_NAME".segments (transaction_id, posting_index, segment_parts, is_source) values (
                tx.id, postingIndex, string_to_array(posting->>'source', ':'), true
            );
            insert into "VAR_LEDGER_NAME".segments (transaction_id, posting_index, segment_parts, is_source) values (
                tx.id, postingIndex, string_to_array(posting->>'destination', ':'), false
            );
            postingIndex = postingIndex + 1;
        end loop;
    END LOOP;
END
$$
LANGUAGE plpgsql;
--statement
select "VAR_LEDGER_NAME".create_segments();
