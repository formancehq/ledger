# Offline Pebble v2 to RocksDB migration

This standalone Go module reads one stopped Ledger v3 Pebble database (or an
immutable filesystem snapshot) and creates a new RocksDB database. It does not
change or delete the Pebble source. It is an operator cutover tool, not a server
startup migration. Keep the source until rollback is no longer needed.

## Build and run

Install the RocksDB C library and headers compatible with grocksdb v1.11.1.
On Homebrew macOS, from this directory:

```sh
GOWORK=off CC=/usr/bin/clang CXX=/usr/bin/clang++ CGO_CFLAGS=-I/opt/homebrew/include CGO_LDFLAGS=-L/opt/homebrew/lib go build -o /tmp/pebble-to-rocksdb .
/tmp/pebble-to-rocksdb --stopped --kind main --source /snapshot/main-db --destination /data/main-rocksdb
```

Choose `--kind main`, `read`, or `usage` for each physical database. Point
`--source` at the Pebble database directory itself (for example, `usagedb`),
not its parent. The read and usage kinds open their persisted Pebble comparer
names and write RocksDB with the corresponding bytewise comparator names.
The main kind uses each engine's default bytewise comparator. Do not point
Ledger at the destination until its RocksDB code expects that comparator.

Ledger must be stopped for the entire operation, or the source must be an
immutable, consistent snapshot. `--stopped` is an operator acknowledgment;
the tool cannot prove there are no external writers. The source must be a
Pebble v2 database readable by the pinned module version. Destination parent
must already exist and destination must not exist.

The command writes `<destination>.incomplete`, syncs writes, closes RocksDB,
compares the count and SHA-256 of length-framed ordered key/value pairs, then
rescans the source. It publishes by an exclusive same-directory rename on
macOS or Linux and syncs the
parent directory. A failure leaves `.incomplete` for inspection; the command
will refuse to overwrite it. After checking the failure, move it aside or
remove that **staging directory only**, then rerun from the untouched source.
If parent sync fails after rename, the error names the published destination;
inspect and verify it before use. The command never removes the source or an
existing destination. Run `GOWORK=off CC=/usr/bin/clang CXX=/usr/bin/clang++ CGO_CFLAGS=-I/opt/homebrew/include CGO_LDFLAGS=-L/opt/homebrew/lib go test ./...` here to test all three
database kinds and failure behavior.
