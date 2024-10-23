package driver

import "fmt"

type ErrRollbackDetected struct {
	LastRegisterVersion  int
	LastAvailableVersion int
}

func (e ErrRollbackDetected) Error() string {
	return fmt.Sprintf("rollback detected, last register version: %d, last available version: %d", e.LastRegisterVersion, e.LastAvailableVersion)
}

func (e ErrRollbackDetected) Is(err error) bool {
	_, ok := err.(ErrRollbackDetected)
	return ok
}

func newErrRollbackDetected(lastRegisterVersion, lastAvailableVersion int) ErrRollbackDetected {
	return ErrRollbackDetected{
		LastRegisterVersion:  lastRegisterVersion,
		LastAvailableVersion: lastAvailableVersion,
	}
}
