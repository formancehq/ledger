set search_path = '{{.Bucket}}';

do $$
	declare
		_batch_size integer := 30;
	begin
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
		end loop;
	end
$$;

alter table moves
alter column transactions_id set not null;