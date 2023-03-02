package internal

import (
	"strings"

	"github.com/spf13/viper"
)

var EnvVarReplacer = strings.NewReplacer(".", "_", "-", "_")

func BindEnv(v *viper.Viper) {
	v.SetEnvKeyReplacer(EnvVarReplacer)
	v.AutomaticEnv()
}
