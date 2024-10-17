create function "{{.Bucket}}".set_log_hash()
	returns trigger
	security definer
	language plpgsql
as
$$
declare
	previousHash bytea;
	marshalledAsJSON varchar;
begin
	select hash into previousHash
	from "{{.Bucket}}".logs
	where ledger = new.ledger
	order by seq desc
	limit 1;

	-- select only fields participating in the hash on the backend and format json representation the same way
	select '{' ||
		'"type":"' || new.type || '",' ||
		'"data":' || encode(new.memento, 'escape') || ',' ||
		'"date":"' || (to_json(new.date::timestamp)#>>'{}') || 'Z",' ||
		'"idempotencyKey":"' || coalesce(new.idempotency_key, '') || '",' ||
		'"id":0,' ||
		'"hash":null' ||
   '}' into marshalledAsJSON;

	new.hash = (
		select public.digest(
			case
			when previousHash is null
			then marshalledAsJSON::bytea
			else '"' || encode(previousHash::bytea, 'base64')::bytea || E'"\n' || convert_to(marshalledAsJSON, 'LATIN1')::bytea
			end || E'\n', 'sha256'::text
		)
	);

	return new;
end;
$$;