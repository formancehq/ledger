package balancehistory

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination snapshot_reader_generated_test.go -package balancehistory github.com/formancehq/ledger/v3/internal/storage/dal SnapshotReader
