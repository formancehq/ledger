package storage

type Driver interface {
	Initialize() error
	NewStore(name string) (Store, error)
}

var builtInDrivers = make(map[string]Driver)

func RegisterDriver(name string, driver Driver) {
	builtInDrivers[name] = driver
}
