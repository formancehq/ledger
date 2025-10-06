do $$
	begin
		set search_path = '{{ .Schema }}';

		create table schemas (
			ledger varchar,
			version text not null,
			created_at timestamp without time zone not null default now(),
			primary key (ledger, version)
		);

		alter type log_type add value 'UPDATED_SCHEMA';

		alter table logs
		add column schema_version text;
	end
$$;