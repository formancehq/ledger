set search_path = '{{.Bucket}}';

do $$
	declare
		_batch_size integer := 30;
		-- select the date where the "11-make-stateless" migration has been applied
		_date timestamp without time zone = (
			select tstamp
			from _system.goose_db_version
			where version_id = 12
		);
		_count integer = (
			select count(*)
			from logs
			where date <= _date
		);
	begin
		for i in 0.._count by _batch_size loop
			update transactions
			set inserted_at = (
				select date
				from logs
				where transactions.id = (data->'transaction'->>'id')::bigint and transactions.ledger = ledger
			)
			where id >= i and id < i + _batch_size;
		end loop;
	end
$$;

alter table moves
alter column transactions_id set not null;