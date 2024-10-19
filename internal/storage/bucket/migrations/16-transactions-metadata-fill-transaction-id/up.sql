
do $$
	declare
		_batch_size integer := 30;
		_count integer;
	begin
		set search_path = '{{.Schema}}';

		select count(seq)
		from transactions_metadata
		where transactions_id is null
		into _count;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || _count);

		loop
			with _outdated_transactions_metadata as (
				select seq
				from transactions_metadata
				where transactions_id is null
				limit _batch_size
			)
			update transactions_metadata
			set transactions_id = (
				select id
				from transactions
				where transactions_metadata.transactions_seq = seq
			)
			from _outdated_transactions_metadata
			where transactions_metadata.seq in (_outdated_transactions_metadata.seq);

			exit when not found;

			commit;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

		end loop;

		alter table transactions_metadata
		alter column transactions_id set not null ;
	end
$$;

