-- Run only after following clickhouse-account-metadata.md.
-- Replace ledger_events and ledger_events_nested with the actual source and
-- freshly created, empty destination identifiers. Stop at the first error.
-- Do not run the copy independently of the preflight statements.

-- BEGIN valid-json
SELECT throwIf(countIf(NOT arrayAll(value -> isValidJSON(value), mapValues(data.accountMetadata))) > 0,
    'Legacy account metadata contains invalid JSON, source remains unchanged')
FROM ledger_events;
-- END valid-json

-- BEGIN objects
SELECT throwIf(countIf(NOT arrayAll(value -> JSONType(value) = 'Object', mapValues(data.accountMetadata))) > 0,
    'Legacy account metadata is not an object, source remains unchanged')
FROM ledger_events;
-- END objects

-- BEGIN strings
SELECT throwIf(countIf(NOT arrayAll(value -> arrayAll(pair -> JSONType(pair.2) = 'String',
    JSONExtractKeysAndValuesRaw(value)), mapValues(data.accountMetadata))) > 0,
    'Legacy account metadata contains non-string values, source remains unchanged')
FROM ledger_events;
-- END strings

-- BEGIN unique-keys
SELECT throwIf(countIf(
    length(mapKeys(data.accountMetadata)) != length(arrayDistinct(mapKeys(data.accountMetadata)))
    OR NOT arrayAll(value -> length(JSONExtractKeysAndValuesRaw(value)) =
        length(arrayDistinct(arrayMap(pair -> pair.1, JSONExtractKeysAndValuesRaw(value)))),
        mapValues(data.accountMetadata))) > 0,
    'Legacy account metadata contains duplicate keys, source remains unchanged')
FROM ledger_events;
-- END unique-keys

-- BEGIN empty-target
SELECT throwIf(count() > 0, 'Migration destination must be empty')
FROM ledger_events_nested;
-- END empty-target

-- BEGIN export
-- Export data as a string so the converter can retain every numeric token.
SELECT log_sequence, type, ledger, date, toJSONString(data) AS data
FROM ledger_events
SETTINGS output_format_json_quote_64bit_integers = 0
FORMAT JSONEachRow;
-- END export
