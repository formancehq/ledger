package grpc

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source ../../infra/backup/storage.go -destination storage_generated_test.go -package grpc Storage
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source ../../application/ctrl/controller.go -destination controller_generated_test.go -package grpc Controller
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source ../../proto/servicepb/bucket_grpc.pb.go -exclude_interfaces BucketServiceServer,UnsafeBucketServiceServer -destination bucket_service_client_generated_test.go -package grpc BucketServiceClient
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination server_streaming_generated_test.go -package grpc google.golang.org/grpc ServerStreamingClient,ServerStreamingServer
