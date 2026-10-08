import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

// Use the generated SDK's operation response decoders, never a substitute schema.
assert.ok(process.argv[2], "usage: node tests/sdk/system-log.mjs <built SDK directory>");
const sdk = pathToFileURL(`${resolve(process.argv[2])}/esm/models/`);
const { GetLogResponse$inboundSchema: getLog } = await import(new URL("operations/get-log.js", sdk));
const { ListLedgerLogsResponse$inboundSchema: listLogs } = await import(new URL("operations/list-ledger-logs.js", sdk));
const { ExecutePreparedQueryResponse$inboundSchema: executeQuery } = await import(new URL("operations/execute-prepared-query.js", sdk));
const fixtures = JSON.parse(await readFile(new URL("system-log-payloads.json", import.meta.url), "utf8"));

let failures = 0;
for (const [name, payload] of Object.entries(fixtures)) {
  const log = { sequence: 42, payload, responseSignature: { hash: "synthetic", nested: { value: "retained" } } };
  const expected = structuredClone(log);
  if (expected.payload.apply?.log?.date) {
    expected.payload.apply.log.date = new Date(expected.payload.apply.log.date);
  }
  const cursor = { pageSize: 1, hasMore: true, next: "opaque-cursor", logData: [log] };
  const operations = {
    getLog: () => getLog.parse({ data: log }).data,
    listLedgerLogs: () => listLogs.parse({ Result: { data: [log] } }).result.data[0],
    executePreparedQuery: () => {
      const result = executeQuery.parse({ Result: { cursor } }).result;
      assert.deepStrictEqual(result.cursor, { ...cursor, logData: [expected] });
      return result.cursor.logData[0];
    },
  };
  for (const [operation, decode] of Object.entries(operations)) {
    try {
      assert.deepStrictEqual(decode(), expected);
    } catch (error) {
      failures++;
      console.error(`FAIL ${operation}/${name}: ${error.message}`);
    }
  }
}

// Additional properties must not replace the existing typed apply branch.
for (const malformed of [
  { apply: { log: { type: "NEW_TRANSACTION", data: "invalid" } } },
  { apply: { log: { type: "NEW_TRANSACTION" } } },
]) {
  assert.throws(() => getLog.parse({ data: { payload: malformed } }));
}
assert.equal(failures, 0, `${failures} generated SDK operation decodes lost SystemLog fields`);
console.log(`PASS: ${Object.keys(fixtures).length} payload fixtures across 3 generated SDK operations; typed apply validation retained`);
