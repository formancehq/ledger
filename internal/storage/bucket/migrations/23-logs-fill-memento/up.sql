do $$
	declare
		_batch_size integer := 1000;
		_count integer;
	begin
		set search_path = '{{.Schema}}';

		select count(seq)
		from logs
		where memento is null
		into _count;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || _count);

		loop
			with _outdated_logs as (
				select seq
				from logs
				where memento is null
				limit _batch_size
			)
			update logs
			set memento = convert_to(data::varchar, 'UTF-8')::bytea
			from _outdated_logs
			where logs.seq in (_outdated_logs.seq);

			exit when not found;

			commit;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);
		end loop;

		alter table logs
		add constraint memento_not_null
		check (memento is not null)
		not valid;
	end
$$;

