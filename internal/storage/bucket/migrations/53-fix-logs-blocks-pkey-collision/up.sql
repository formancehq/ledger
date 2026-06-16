do $$
	begin
		set search_path = '{{ .Schema }}';

		alter table logs_blocks drop constraint logs_blocks_pkey;
		alter table logs_blocks add primary key (ledger, previous);
	end
$$;
