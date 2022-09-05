package internal

import (
	"fmt"
	"os"
	"strings"
)

func withPrefix(flag string) string {
	return strings.ToUpper(fmt.Sprintf("%s_%s", envPrefix, EnvVarReplacer.Replace(flag)))
}

func setEnvVar(key, value string) func() {
	prefixedFlag := withPrefix(key)
	oldEnv := os.Getenv(prefixedFlag)
	os.Setenv(prefixedFlag, value)
	return func() {
		os.Setenv(prefixedFlag, oldEnv)
	}
}
