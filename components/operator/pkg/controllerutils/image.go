package controllerutils

import (
	"fmt"
)

func GetImage(component, version string) string {
	return fmt.Sprintf("ghcr.io/formancehq/%s:%s", component, version)
}
