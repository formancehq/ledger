package fctl

import (
	"os"
	"strconv"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/spf13/cobra"
)

const (
	MembershipURIFlag = "membership-uri"
	FileFlag          = "config"
	ProfileFlag       = "profile"
	DebugFlag         = "debug"
	InsecureTlsFlag   = "insecure-tls"
)

func GetBool(cmd *cobra.Command, flagName string) bool {
	v, err := cmd.Flags().GetBool(flagName)
	if err != nil {
		fromEnv := strings.ToLower(os.Getenv(strcase.ToScreamingSnake(flagName)))
		return fromEnv == "true" || fromEnv == "1"
	}
	return v
}

func GetString(cmd *cobra.Command, flagName string) string {
	v, err := cmd.Flags().GetString(flagName)
	if err != nil || v == "" {
		return os.Getenv(strcase.ToScreamingSnake(flagName))
	}
	return v
}

func GetStringSlice(cmd *cobra.Command, flagName string) []string {
	v, err := cmd.Flags().GetStringSlice(flagName)
	if err != nil || len(v) == 0 {
		envVar := os.Getenv(strcase.ToScreamingSnake(flagName))
		if envVar == "" {
			return []string{}
		}
		return strings.Split(envVar, " ")
	}
	return v
}

func GetInt(cmd *cobra.Command, flagName string) int {
	v, err := cmd.Flags().GetInt(flagName)
	if err != nil {
		v := os.Getenv(strcase.ToScreamingSnake(flagName))
		if v != "" {
			v, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0
			}
			return int(v)
		}
		return 0
	}
	return v
}
