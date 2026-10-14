package ctrl

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source ../../proto/snapshotpb/snapshot_grpc.pb.go -exclude_interfaces SnapshotServiceServer,UnsafeSnapshotServiceServer -destination snapshot_service_client_generated_test.go -package ctrl SnapshotServiceClient
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination server_streaming_client_generated_test.go -package ctrl google.golang.org/grpc ServerStreamingClient
