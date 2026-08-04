package ctrl

//go:generate go tool mockgen -typed -write_source_comment=false -write_package_comment=false -destination snapshot_service_client_generated_test.go -package ctrl github.com/formancehq/ledger/v3/internal/proto/snapshotpb SnapshotServiceClient
//go:generate go tool mockgen -typed -write_source_comment=false -write_package_comment=false -destination server_streaming_client_generated_test.go -package ctrl google.golang.org/grpc ServerStreamingClient
