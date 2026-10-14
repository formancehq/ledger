package node

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source ../state/machine.go -destination notifier_generated_test.go -package node Notifier
