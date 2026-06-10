package grpc

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination mock_backup_storage_test.go -package grpc github.com/formancehq/ledger/v3/internal/infra/backup Storage
