//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../../controller/ledger/controller.go -destination mocks_ledger_controller_test.go -package bulking --mock_names Controller=LedgerController . Controller
package bulking
