package drivers

import "fmt"

type ErrMalformedConfiguration struct {
	connector string
	err       error
}

func (e *ErrMalformedConfiguration) Error() string {
	return fmt.Sprintf("connector '%s' has malformed configuration: %s", e.connector, e.err)
}

func NewErrMalformedConfiguration(connector string, err error) *ErrMalformedConfiguration {
	return &ErrMalformedConfiguration{
		connector: connector,
		err:       err,
	}
}

type ErrInvalidConfiguration struct {
	connector string
	err       error
}

func (e *ErrInvalidConfiguration) Error() string {
	return fmt.Sprintf("connector '%s' has invalid configuration: %s", e.connector, e.err)
}

func NewErrInvalidConfiguration(connector string, err error) *ErrInvalidConfiguration {
	return &ErrInvalidConfiguration{
		connector: connector,
		err:       err,
	}
}

type ErrDriverNotFound struct {
	driver string
}

func (e *ErrDriverNotFound) Error() string {
	return fmt.Sprintf("driver '%s' not found", e.driver)
}

func NewErrDriverNotFound(connector string) *ErrDriverNotFound {
	return &ErrDriverNotFound{
		driver: connector,
	}
}

type ErrConnectorNotFound string

func (e ErrConnectorNotFound) Error() string {
	return fmt.Sprintf("connector '%s' not found", string(e))
}

func NewErrConnectorNotFound(id string) ErrConnectorNotFound {
	return ErrConnectorNotFound(id)
}
