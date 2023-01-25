--statement
alter table transactions add column sources text;
--statement
alter table transactions add column destinations text;
--statement
UPDATE transactions
SET sources = (
    select group_concat(json_extract(json_each.value, '$.source'), ';')
    from transactions tx2, json_each(tx2.postings)
    where transactions.id = tx2.id
), destinations = (
    select group_concat(json_extract(json_each.value, '$.destination'), ';')
    from transactions tx2, json_each(tx2.postings)
    where transactions.id = tx2.id
);
