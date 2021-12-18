package storage

type Driver interface {
	Initialize() error
	NewStore(name string) (Store, error)
	Close() error
}

var drivers = make(map[string]Driver)

func RegisterDriver(name string, driver Driver) {
	drivers[name] = driver
}
