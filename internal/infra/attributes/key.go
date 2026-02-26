package attributes

type Key interface {
	comparable
	Bytes() []byte
}

type IDWithTag struct {
	ID  U128
	Tag uint64
}
