package core

type Contract struct {
	ID      string `json:"id"`
	Expr    Expr   `json:"expr"`
	Account string `json:"account"`
}
