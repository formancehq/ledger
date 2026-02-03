package attributes

type Key interface {
	comparable
	Bytes() []byte
}