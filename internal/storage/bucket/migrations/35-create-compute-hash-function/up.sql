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
	       '"hash":null' ||
	       '}' into marshalledAsJSON;

	return (select public.digest(
			case
				when previous_hash is null
					then marshalledAsJSON::bytea
				else '"' || encode(previous_hash::bytea, 'base64')::bytea || E'"\n' || marshalledAsJSON::bytea
				end || E'\n', 'sha256'::text
	               ));
end;
$$ set search_path = '{{ .Schema }}';

create or replace function set_log_hash()
	returns trigger
	security definer
	language plpgsql
as
$$
declare
	previousHash bytea;
begin
	select hash into previousHash
	from logs
	where ledger = new.ledger
	order by seq desc
	limit 1;

	new.hash = compute_hash(previousHash, new);

	return new;
end;
$$ set search_path = '{{ .Schema }}';