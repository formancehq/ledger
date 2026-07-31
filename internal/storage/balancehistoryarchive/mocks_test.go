package balancehistoryarchive

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination coldstorage_generated_test.go -package balancehistoryarchive github.com/formancehq/ledger/v3/internal/infra/coldstorage ColdStorage
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination object_catalog_generated_test.go -package balancehistoryarchive github.com/formancehq/ledger/v3/internal/infra/coldstorage ObjectCatalog
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination identified_storage_generated_test.go -package balancehistoryarchive github.com/formancehq/ledger/v3/internal/infra/coldstorage IdentifiedStorage
