package query_test

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination entity_iterator_generated_test.go -package query_test github.com/formancehq/ledger/v3/internal/storage/readstore EntityIterator
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source ../storage/dal/reader.go -exclude_interfaces PebbleReader -destination pebble_getter_generated_test.go -package query PebbleGetter
