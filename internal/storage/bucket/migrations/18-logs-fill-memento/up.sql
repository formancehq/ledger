set search_path = '{{.Bucket}}';

do $$
	declare
		_batch_size integer := 30;
	begin
		loop
			with _outdated_logs as (
				select seq
				from logs
				where memento is null
				limit _batch_size
			)
			update logs
			set memento = convert_to(data::varchar, 'LATIN1')::bytea
			from _outdated_logs
			where logs.seq in (_outdated_logs.seq);

			exit when not found;
		end loop;
	end
$$;

alter table logs
alter column memento set not null;