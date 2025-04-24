do $$
	begin
		set search_path = '{{.Schema}}';
		assert (select count(*) from transactions where inserted_at is null) = 0, 'inserted_at should not be null';
	end;
$$