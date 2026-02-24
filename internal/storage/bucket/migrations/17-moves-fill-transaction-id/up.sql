do $$
	declare
		_batch_size integer := 1000;
		_max integer;
	begin
		set search_path = '{{.Schema}}';

		drop table if exists transactions_ids;
		create temporary table transactions_ids as
		select row_number() over (order by transactions.seq) as row_number,
		       moves.seq as moves_seq, transactions.id, transactions.seq as transactions_seq
		from moves
		join transactions on transactions.seq = moves.transactions_seq
		where transactions_id is null;

		create index transactions_ids_rows on transactions_ids(row_number) include (moves_seq, transactions_seq, id);

		analyze transactions_ids;

		select count(*)
		from transactions_ids
		into _max;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || _max);

		for i in 1.._max by _batch_size loop
			with _rows as (
				select *
				from transactions_ids
				where row_number >= i and row_number < i + _batch_size
			)
			update moves
			set transactions_id = _rows.id
			from _rows
			where seq = _rows.moves_seq;

			commit;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);
		end loop;

		drop table transactions_ids;
	end
$$
language plpgsql;