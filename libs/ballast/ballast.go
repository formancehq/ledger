package ballast

//lint:ignore U1000 this var is actually used to allocate some memory.
var ballast []byte

func Allocate(sizeInBytes uint) {
	ballast = make([]byte, 0, sizeInBytes)
}

func ReleaseForGC() {
	ballast = nil
}
