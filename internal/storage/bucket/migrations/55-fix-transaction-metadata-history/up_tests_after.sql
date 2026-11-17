do $$
declare
	transaction_id bigint;
begin
	set search_path = '{{.Schema}}';
	select id into strict transaction_id from transactions
	where ledger = 'ledger0' and reference = 'metadata-history-migration';

	-- Migration must preserve the existing history, including the old duplicate.
	assert (select count(*) from transactions_metadata where ledger = 'ledger0' and transactions_id = transaction_id) = 2,
		'migration must preserve existing revisions';
	update transactions set reverted_at = null, updated_at = now()
	where ledger = 'ledger0' and id = transaction_id;
	assert (select count(*) from transactions_metadata where ledger = 'ledger0' and transactions_id = transaction_id) = 2,
		'legacy broad trigger must not append history for non-metadata updates after migration';

	update transactions set metadata = '{"order":"456"}', updated_at = now()
	where ledger = 'ledger0' and id = transaction_id;
	assert (select count(*) from transactions_metadata where ledger = 'ledger0' and transactions_id = transaction_id) = 3,
		'real metadata changes must still append a revision';
	assert (select metadata from transactions_metadata where ledger = 'ledger0' and transactions_id = transaction_id order by revision desc limit 1) = '{"order":"456"}'::jsonb,
		'new revision must contain the updated metadata';

	update transactions set metadata = metadata, updated_at = now()
	where ledger = 'ledger0' and id = transaction_id;
	assert (select count(*) from transactions_metadata where ledger = 'ledger0' and transactions_id = transaction_id) = 3,
		'writing identical metadata must not append a revision';

	update transactions set metadata = metadata - 'order', updated_at = now()
	where ledger = 'ledger0' and id = transaction_id;
	assert (select count(*) from transactions_metadata where ledger = 'ledger0' and transactions_id = transaction_id) = 4,
		'deleting metadata must still append a revision';
	assert (select metadata from transactions_metadata where ledger = 'ledger0' and transactions_id = transaction_id order by revision desc limit 1) = '{}'::jsonb,
		'new revision must record the metadata deletion';
end;
$$;
