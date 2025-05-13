package drivers

import "fmt"

type ErrMalformedConfiguration struct {
	exporter string
	err      error
}

func (e *ErrMalformedConfiguration) Error() string {
	return fmt.Sprintf("exporter '%s' has malformed configuration: %s", e.exporter, e.err)
}

func NewErrMalformedConfiguration(exporter string, err error) *ErrMalformedConfiguration {
	return &ErrMalformedConfiguration{
		exporter: exporter,
		err:      err,
	}
}

type ErrInvalidConfiguration struct {
	exporter string
	err      error
}

func (e *ErrInvalidConfiguration) Error() string {
	return fmt.Sprintf("exporter '%s' has invalid configuration: %s", e.exporter, e.err)
}

func NewErrInvalidConfiguration(exporter string, err error) *ErrInvalidConfiguration {
	return &ErrInvalidConfiguration{
		exporter: exporter,
		err:      err,
	}
}

type ErrDriverNotFound struct {
	driver string
}

func (e *ErrDriverNotFound) Error() string {
	return fmt.Sprintf("driver '%s' not found", e.driver)
}

func NewErrDriverNotFound(driver string) *ErrDriverNotFound {
	return &ErrDriverNotFound{
		driver: driver,
	}
}

type ErrExporterNotFound string

func (e ErrExporterNotFound) Error() string {
	return fmt.Sprintf("exporter '%s' not found", string(e))
}

func NewErrExporterNotFound(id string) ErrExporterNotFound {
	return ErrExporterNotFound(id)
}
