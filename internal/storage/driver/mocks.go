//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../bucket/bucket.go -destination bucket_generated_test.go -package driver . Bucket
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../bucket/bucket.go -destination bucket_generated_test.go -package driver . Factory

package driver
