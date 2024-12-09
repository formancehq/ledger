do $$
	declare
		_batch_size integer := 1000;
		_max integer;
	begin
		set search_path = '{{.Schema}}';

		select count(seq)
		from moves
		where transactions_id is null
		into _max;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || _max);
		loop

			with _outdated_moves as (
				select *
				from moves
				where transactions_id is null
				limit _batch_size
			)
			update moves
			set transactions_id = (
				select id
				from transactions
				where seq = moves.transactions_seq
			)
			from _outdated_moves
			where moves.seq in (_outdated_moves.seq);

			exit when not found;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

			commit ;
		end loop;

		alter table moves
		add constraint transactions_id_not_null
		check (transactions_id is not null)
		not valid;
	end
$$
language plpgsql;