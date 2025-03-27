//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../../pagination/resource.go -destination mocks_test.go -package ledger . Resource
package ledger
