package core

type Script struct {
	Plain    string `json:"plain"`
	AST      AST    `json:"ast"`
	Compiled []byte `json:"bytecode"`
}

type AST struct {
}
