package storage

type Factory interface {
	GetStore(name string) (Store, error)
}
type FactoryFn func(string) (Store, error)

func (f FactoryFn) GetStore(name string) (Store, error) {
	return f(name)
}

var DefaultFactory Factory = FactoryFn(GetStore)

func NewDefaultFactory() (Factory, error) {
	return DefaultFactory, nil
}
