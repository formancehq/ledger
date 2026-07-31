package balancehistorystore

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination coldstorage_generated_test.go -package balancehistorystore github.com/formancehq/ledger/v3/internal/infra/coldstorage ColdStorage
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination identified_storage_generated_test.go -package balancehistorystore github.com/formancehq/ledger/v3/internal/infra/coldstorage IdentifiedStorage
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination context_generated_test.go -package balancehistorystore context Context
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination archive_generated_test.go -package balancehistorystore github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive Archive
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination identified_archive_generated_test.go -package balancehistorystore github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive IdentifiedArchive
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination reclaimer_generated_test.go -package balancehistorystore github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive Reclaimer
