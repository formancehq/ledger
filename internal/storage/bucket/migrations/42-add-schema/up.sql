do $$
	begin
		set search_path = '{{ .Schema }}';

		create table schemas (
			ledger varchar,
			version text not null,
			created_at timestamp without time zone not null default now(),
			chart jsonb not null,
			primary key (ledger, version)
		);

		alter type log_type add value 'INSERTED_SCHEMA';

		alter table logs
		add column schema_version text;
	end
$$;

create or replace function compute_hash(previous_hash bytea, r logs)
	returns bytea
	language plpgsql
as
$$
declare
	marshalledAsJSON varchar;
begin
	-- select only fields participating in the hash on the backend and format json representation the same way
	select '{' ||
	       '"type":"' || r.type || '",' ||
	       '"data":' || encode(r.memento, 'escape') || ',' ||
	       '"date":"' || (to_json(r.date::timestamp)#>>'{}') || 'Z",' ||
	       '"idempotencyKey":"' || coalesce(r.idempotency_key, '') || '",' ||
	       '"id":0,' ||
	       '"hash":null' into marshalledAsJSON;
	if r.schema_version is not null then
		marshalledAsJSON := marshalledAsJSON || ',"schemaVersion":"' || r.schema_version || '"';
	end if;
	marshalledAsJSON := marshalledAsJSON || '}';

	return (select public.digest(
			case
				when previous_hash is null
					then marshalledAsJSON::bytea
				else '"' || encode(previous_hash::bytea, 'base64')::bytea || E'"\n' || marshalledAsJSON::bytea
				end || E'\n', 'sha256'::text
	               ));
end;
$$ set search_path = '{{ .Schema }}';
