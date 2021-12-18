package storage

import "fmt"

type Factory interface {
	GetStore(name string) (Store, error)
	Close() error
}
type FactoryFn func(string) (Store, error)

func (f FactoryFn) GetStore(name string) (Store, error) {
	return f(name)
}

type closeError struct {
	errs map[string]error
}

func (e *closeError) Error() string {
	buf := ""
	if len(e.errs) == 0 {
		return ""
	}
	for driver, err := range e.errs {
		buf += fmt.Sprintf("%s: %s,", driver, err)
	}
	return buf[:len(buf)-1]
}

type BuiltInFactory struct{}

func (f *BuiltInFactory) GetStore(name string) (Store, error) {
	return GetStore(name)
}

func (f *BuiltInFactory) Close() error {
	closeErr := &closeError{
		errs: map[string]error{},
	}
	for name, driver := range drivers {
		err := driver.Close()
		if err != nil {
			closeErr.errs[name] = err
		}
	}
	if len(closeErr.errs) > 0 {
		return closeErr
	}
	return nil
}

var DefaultFactory Factory = &BuiltInFactory{}

func NewDefaultFactory() (Factory, error) {
	return DefaultFactory, nil
}
