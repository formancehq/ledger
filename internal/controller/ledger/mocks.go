//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../../storage/resources/resource.go -destination mocks_test.go -package ledger . Resource
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../../storage/resources/resource.go -destination mocks_test.go -package ledger . PaginatedResource
package ledger
