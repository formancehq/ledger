package v1beta2

import (
	pkgapisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	corev1 "k8s.io/api/core/v1"
)

type PostgresConfigCreateDatabase struct {
	pkgapisv1beta2.PostgresConfigWithDatabase `json:",inline"`
	CreateDatabase                            bool `json:"createDatabase"`
}

func (in *PostgresConfigCreateDatabase) CreateDatabaseInitContainer() corev1.Container {
	return corev1.Container{
		Name:            "init-db",
		Image:           "postgres:13",
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env:             in.Env(""),
		Command: []string{
			"sh",
			"-c",
			`psql -Atx ${POSTGRES_NO_DATABASE_URI}/postgres -c "SELECT 1 FROM pg_database WHERE datname = '${POSTGRES_DATABASE}'" | grep -q 1 && echo "Base already exists" || psql -Atx ${POSTGRES_NO_DATABASE_URI}/postgres -c "CREATE DATABASE \"${POSTGRES_DATABASE}\""`,
		},
	}
}

type CollectorConfig struct {
	pkgapisv1beta2.KafkaConfig `json:",inline"`
	Topic                      string `json:"topic"`
}

func (c CollectorConfig) Env(prefix string) []corev1.EnvVar {
	ret := c.KafkaConfig.Env(prefix)
	return append(ret, pkgapisv1beta2.EnvWithPrefix(prefix, "PUBLISHER_TOPIC_MAPPING", "*:"+c.Topic))
}
