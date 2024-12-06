
do $$
	declare
		_batch_size integer := 100;
		_count integer;
	begin
		set search_path = '{{.Schema}}';

		select count(seq)
		from accounts_metadata
		where accounts_address is null
		into _count;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || _count);

		loop
			with _outdated_accounts_metadata as (
				select seq
				from accounts_metadata
				where accounts_address is null
				limit _batch_size
			)
			update accounts_metadata
			set accounts_address = (
				select address
				from accounts
				where accounts_metadata.accounts_seq = seq
			)
			from _outdated_accounts_metadata
			where accounts_metadata.seq in (_outdated_accounts_metadata.seq);

			exit when not found;

			commit;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

		end loop;

		alter table accounts_metadata
		alter column accounts_address set not null ;
	end
$$;

