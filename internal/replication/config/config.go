package config

type Validator interface {
	Validate() error
}

type Defaulter interface {
	SetDefaults()
}
