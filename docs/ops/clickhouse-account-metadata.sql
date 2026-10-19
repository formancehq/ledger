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

-- BEGIN copy
INSERT INTO ledger_events_nested (log_sequence, type, ledger, date, data)
SELECT log_sequence, type, ledger, date,
    concat('{', arrayStringConcat(arrayMap(pair -> concat(toJSONString(pair.1), ':',
        if(pair.1 = 'accountMetadata',
            toJSONString(mapApply((account, metadata) ->
                (account, JSONExtract(metadata, 'Map(String, String)')), data.accountMetadata)),
            pair.2)), JSONExtractKeysAndValuesRaw(toJSONString(data))), ','), '}')
FROM ledger_events
SETTINGS output_format_json_quote_64bit_integers = 0;
-- END copy
