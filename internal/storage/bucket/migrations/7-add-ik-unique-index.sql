update "{{.Bucket}}".logs
set idempotency_key = null
where idempotency_key = '';

update "{{.Bucket}}".logs
set idempotency_key = null
where id in (
    select unnest(duplicateLogIds.ids[2:]) as id
    from (
        select array_agg(id order by id) as ids
        from "{{.Bucket}}".logs l
        where idempotency_key is not null
        group by idempotency_key
        having count(*) > 1
    ) duplicateLogIds
);

drop index "{{.Bucket}}".logs_idempotency_key;

create unique index logs_idempotency_key on "{{.Bucket}}".logs (idempotency_key);