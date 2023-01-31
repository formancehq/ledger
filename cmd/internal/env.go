package internal

import (
	"strings"

	"github.com/spf13/viper"
)

const (
	envPrefix = "numary"
)

var EnvVarReplacer = strings.NewReplacer(".", "_", "-", "_")

func BindEnv(v *viper.Viper) {
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(EnvVarReplacer)
	v.AutomaticEnv()
}
