do $$
declare
	ledger_id bigint;
	transaction_id bigint;
begin
	set search_path = '{{.Schema}}';
	select id into strict ledger_id from _system.ledgers where name = 'ledger0';

	-- Reproduce the broad trigger installed by AddLedger before this fix.
	execute format('drop trigger if exists %I on transactions', 'update_transaction_metadata_history_' || ledger_id);
	execute format(
		'create trigger %I after update on transactions for each row when (new.ledger = %L) execute procedure update_transaction_metadata_history()',
		'update_transaction_metadata_history_' || ledger_id, 'ledger0'
	);
	execute format('drop trigger if exists %I on transactions', 'insert_transaction_metadata_history_' || ledger_id);
	execute format(
		'create trigger %I after insert on transactions for each row when (new.ledger = %L) execute procedure insert_transaction_metadata_history()',
		'insert_transaction_metadata_history_' || ledger_id, 'ledger0'
	);

	insert into transactions (ledger, id, inserted_at, postings, post_commit_volumes, metadata, reference,
		sources, destinations, sources_arrays, destinations_arrays)
	values ('ledger0', nextval('transaction_id_' || ledger_id), now(),
		'[{"source":"world","destination":"alice","asset":"USD","amount":100}]',
		'{}', '{"order":"123"}', 'metadata-history-migration',
		'["world"]', '["alice"]', '[{"0":"world"}]', '[{"0":"alice"}]')
	returning id into transaction_id;

	update transactions set reverted_at = now(), updated_at = now()
	where ledger = 'ledger0' and id = transaction_id;
	assert (select count(*) from transactions_metadata where ledger = 'ledger0' and transactions_id = transaction_id) = 2,
		'legacy trigger must reproduce the duplicate revision before migration';
end;
$$;
