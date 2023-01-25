package configtemplate

type Type string

const (
	TypeString                  Type = "string"
	TypeDurationNs              Type = "duration ns"
	TypeDurationUnsignedInteger Type = "unsigned integer"
)
