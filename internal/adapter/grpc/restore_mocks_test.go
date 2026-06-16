package grpc

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination storage_generated_test.go -package grpc github.com/formancehq/ledger/v3/internal/infra/backup Storage
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination controller_generated_test.go -package grpc github.com/formancehq/ledger/v3/internal/application/ctrl Controller
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination bucket_service_client_generated_test.go -package grpc github.com/formancehq/ledger/v3/internal/proto/servicepb BucketServiceClient
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination server_streaming_client_generated_test.go -package grpc google.golang.org/grpc ServerStreamingClient
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination server_streaming_server_generated_test.go -package grpc google.golang.org/grpc ServerStreamingServer
