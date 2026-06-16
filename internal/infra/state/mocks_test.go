package state

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination coldstorage_generated_test.go -package state github.com/formancehq/ledger/v3/internal/infra/coldstorage ColdStorage
