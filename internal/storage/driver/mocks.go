//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../bucket/bucket.go -destination buckets_generated_test.go -package driver . Bucket
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../bucket/bucket.go -destination buckets_generated_test.go -package driver --mock_names Factory=BucketFactory . Factory
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../ledger/factory.go -destination ledger_generated_test.go -package driver --mock_names Factory=LedgerStoreFactory . Factory
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../system/store.go -destination system_generated_test.go -package driver --mock_names Store=SystemStore . Store

package driver
