package internal

import (
	"net/http"
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

type roundTripperFn func(req *http.Request) (*http.Response, error)

func (fn roundTripperFn) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
