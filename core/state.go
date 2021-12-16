package core

type State struct {
	LastTransaction *Transaction
	LastMetaID      int64
}
