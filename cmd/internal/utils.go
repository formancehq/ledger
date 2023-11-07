package internal

import (
	"os"
	"strings"
)

func setEnvVar(key, value string) func() {
	flag := strings.ToUpper(EnvVarReplacer.Replace(key))
	oldEnv := os.Getenv(flag)
	os.Setenv(flag, value)
	return func() {
		os.Setenv(flag, oldEnv)
	}
}
