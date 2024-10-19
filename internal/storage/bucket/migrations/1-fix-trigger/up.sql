set search_path = '{{.Schema}}';

create or replace function insert_posting(_transaction_seq bigint, _ledger varchar, _insertion_date timestamp without time zone,
                               _effective_date timestamp without time zone, posting jsonb, _account_metadata jsonb)
    returns void
    language plpgsql
as
$$
declare
    _source_exists      bool;
    _destination_exists bool;
begin

    select true from accounts where ledger = _ledger and address = posting ->> 'source' into _source_exists;
    if posting ->>'source' = posting->>'destination' then
        _destination_exists = true;
    else
        select true from accounts where ledger = _ledger and address = posting ->> 'destination' into _destination_exists;
    end if;

    perform upsert_account(_ledger, posting ->> 'source', _account_metadata -> (posting ->> 'source'), _insertion_date);
    perform upsert_account(_ledger, posting ->> 'destination', _account_metadata -> (posting ->> 'destination'),
                           _insertion_date);

    perform insert_move(_transaction_seq, _ledger, _insertion_date, _effective_date,
                        posting ->> 'source', posting ->> 'asset', (posting ->> 'amount')::numeric, true,
                        _source_exists);
    perform insert_move(_transaction_seq, _ledger, _insertion_date, _effective_date,
                        posting ->> 'destination', posting ->> 'asset', (posting ->> 'amount')::numeric, false,
                        _destination_exists);
end;
$$ set search_path from current;
