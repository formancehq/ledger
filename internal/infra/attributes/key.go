package attributes

type Key interface {
	comparable
	AppendBytes(dst []byte) []byte
}

type IDWithTag struct {
	ID  U128
	Tag uint64
}
