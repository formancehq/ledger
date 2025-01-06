//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../../controller/system/controller.go -destination mocks_system_controller_test.go -package v1 --mock_names Controller=SystemController . Controller
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../../controller/ledger/controller.go -destination mocks_ledger_controller_test.go -package v1 --mock_names Controller=LedgerController . Controller
package v1
