package core

type CommitResult struct {
	PreCommitVolumes      AccountsAssetsVolumes
	PostCommitVolumes     AccountsAssetsVolumes
	GeneratedTransactions []Transaction
	GeneratedLogs         []Log
}
