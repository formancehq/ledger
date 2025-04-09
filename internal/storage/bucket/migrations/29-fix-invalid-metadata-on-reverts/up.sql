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
				(data -> 'transaction' ->> 'id')::numeric   as reversedTransactionID,
				(data ->> 'revertedTransactionID')::numeric as revertedTransactionID
			from logs
			where type = 'REVERTED_TRANSACTION'
		)
		select reversed.ledger, reversed.reversedTransactionID, reversed.revertedTransactionID
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
				select ledger, reversedTransactionID, revertedTransactionID
				from txs_view
				order by ledger, reversedTransactionID, revertedTransactionID
				offset _offset
				limit _batch_size
			)
			update transactions
			set metadata = metadata || ('{"com.formance.spec/state/reverts": "' || data.revertedTransactionID || '"}')::jsonb
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

