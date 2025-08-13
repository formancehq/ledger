do $$
	begin
		set search_path = '{{ .Schema }}';

		-- short-time exclusive lock
		alter table transactions
		add constraint sources_not_null
		check (sources is not null) not valid;

		-- seqscan, but without exclusive lock, concurrent sessions can read/write
		alter table transactions validate constraint sources_not_null;

		-- exclusive lock, fast because the constraints already check the values
		alter table transactions alter column sources set not null;

		-- not needed anymore
		alter table transactions drop constraint sources_not_null;

		-- short-time exclusive lock
		alter table transactions
		add constraint destinations_not_null
		check (destinations is not null) not valid;

		-- seqscan, but without exclusive lock, concurrent sessions can read/write
		alter table transactions validate constraint destinations_not_null;

		-- exclusive lock, fast because the constraints already check the values
		alter table transactions alter column destinations set not null;

		-- not needed anymore
		alter table transactions drop constraint destinations_not_null;

		-- short-time exclusive lock
		alter table transactions
		add constraint sources_arrays_not_null
		check (sources_arrays is not null) not valid;

		-- seqscan, but without exclusive lock, concurrent sessions can read/write
		alter table transactions validate constraint sources_arrays_not_null;

		-- exclusive lock, fast because the constraints already check the values
		alter table transactions alter column sources_arrays set not null;

		-- not needed anymore
		alter table transactions drop constraint sources_arrays_not_null;

		-- short-time exclusive lock
		alter table transactions
		add constraint destinations_arrays_not_null
		check (destinations_arrays is not null) not valid;

		-- seqscan, but without exclusive lock, concurrent sessions can read/write
		alter table transactions validate constraint destinations_arrays_not_null;

		-- exclusive lock, fast because the constraints already check the values
		alter table transactions alter column destinations_arrays set not null;

		-- not needed anymore
		alter table transactions drop constraint destinations_arrays_not_null;
	end
$$;