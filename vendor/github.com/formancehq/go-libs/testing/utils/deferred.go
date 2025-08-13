package utils

type Deferred[V any] struct {
	value *V
	set   chan struct{}
}

func (d *Deferred[V]) GetValue() V {
	return *d.value
}

func (d *Deferred[V]) LoadAsync(fn func() V) {
	go d.SetValue(fn())
}

func (d *Deferred[V]) SetValue(v V) {
	d.value = &v
	close(d.set)
}

func (d *Deferred[V]) Reset() {
	d.set = make(chan struct{})
	d.value = nil
}

func (d *Deferred[V]) Done() chan struct{} {
	return d.set
}

func NewDeferred[V any]() *Deferred[V] {
	return &Deferred[V]{
		set: make(chan struct{}),
	}
}

func Wait(d ...interface {
	Done() chan struct{}
}) {
	for _, d := range d {
		<-d.Done()
	}
}
