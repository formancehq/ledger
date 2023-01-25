//go:generate task generate-client
package main

import "github.com/formancehq/auth/cmd"

func main() {
	cmd.Execute()
}
