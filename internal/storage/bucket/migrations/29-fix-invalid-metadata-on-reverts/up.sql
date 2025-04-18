do $$
	declare
		_offset integer := 0;
		_batch_size integer := 1000;
	begin
		set search_path = '{{ .Schema }}';

		drop table if exists txs_view;

		create temp table txs_view as
		with reversed as (
			select
				ledger,
				(convert_from(memento, 'UTF-8')::jsonb -> 'transaction' ->> 'id')::numeric   as reversedTransactionID,
				(convert_from(memento, 'UTF-8')::jsonb ->> 'revertedTransactionID')::numeric as revertedTransactionID,
				date as revertedAt
			from logs
			where type = 'REVERTED_TRANSACTION'
		)
		select reversed.ledger, reversed.reversedTransactionID, reversed.revertedTransactionID, reversed.revertedAt
		from transactions
		join reversed on
			reversed.reversedTransactionID = transactions.id and
			reversed.ledger = transactions.ledger and
			not (transactions.metadata ? 'com.formance.spec/state/reverts')
		;

		create index txs_view_idx on txs_view(reversedTransactionID);

		if (select count(*) from txs_view) = 0 then
			return;
		end if;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || (select count(*) from txs_view));

		loop
			with data as (
				select ledger, reversedTransactionID, revertedTransactionID, revertedAt
				from txs_view
				order by ledger, reversedTransactionID, revertedTransactionID
				offset _offset
				limit _batch_size
			)
			update transactions
			set
				metadata = metadata || ('{"com.formance.spec/state/reverts": "' || data.revertedTransactionID || '"}')::jsonb,
				updated_at = data.revertedAt
			from data
			where transactions.id = data.reversedTransactionID and
			      transactions.ledger = data.ledger;

			exit when not found;

			_offset = _offset + _batch_size;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

			commit;
		end loop;

		drop table if exists txs_view;
	end
$$;

