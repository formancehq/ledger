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
				id,
				(convert_from(memento, 'UTF-8')::jsonb ->> 'revertedTransactionID')::numeric as revertedTransactionID
			from logs
			where type = 'REVERTED_TRANSACTION' and data->>'revertedTransactionID' is not null
		)
		select reversed.id as log_id, transactions.*
		from transactions
		join reversed on
			reversed.revertedTransactionID = transactions.id and
			reversed.ledger = transactions.ledger;

		create index txs_view_idx on txs_view(log_id, id);

		if (select count(*) from txs_view) = 0 then
			return;
		end if;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || (select count(*) from txs_view));

		loop
			with data as (
				select *
				from txs_view
				order by ledger, log_id, id
				offset _offset
				limit _batch_size
			)
			update logs
			set data = data || jsonb_build_object('revertedTransaction', jsonb_build_object(
			    'id', data.id,
			    'postings', data.postings::jsonb,
                'metadata', data.metadata,
			    'reverted', true,
				'revertedAt', to_json(data.reverted_at)#>>'{}' || 'Z',
				'insertedAt', to_json(data.inserted_at)#>>'{}' || 'Z',
			    'timestamp', to_json(data.timestamp)#>>'{}' || 'Z',
			    'reference', case when data.reference is not null and data.reference <> '' then data.reference end,
			    'postCommitVolumes', data.post_commit_volumes
            ))
			from data
			where logs.id = data.log_id and
			      logs.ledger = data.ledger;

			exit when not found;

			_offset = _offset + _batch_size;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

			commit;
		end loop;

		drop table if exists txs_view;
	end
$$;

