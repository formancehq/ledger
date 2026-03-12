package controller

import (
	"os"
	"strconv"
	"strings"
)

const (
	defaultNodeMetric = "raft.node.lead"
	defaultNodeLabel  = "service.node_id"
	defaultDatasource = "VictoriaMetrics"
)

type Config struct {
	GrafanaURL         string
	GrafanaUser        string
	GrafanaPassword    string
	SnapshotPerNode    bool
	NodeMetric         string
	NodeLabel          string
	DatasourceName     string
	SnapshotNamePrefix string
}

func LoadConfigFromEnv() Config {
	return Config{
		GrafanaURL:         strings.TrimSpace(os.Getenv("GRAFANA_URL")),
		GrafanaUser:        strings.TrimSpace(os.Getenv("GRAFANA_USER")),
		GrafanaPassword:    strings.TrimSpace(os.Getenv("GRAFANA_PASSWORD")),
		SnapshotPerNode:    parseBoolEnv("SNAPSHOT_PER_NODE", true),
		NodeMetric:         envOrDefault("NODE_METRIC", defaultNodeMetric),
		NodeLabel:          envOrDefault("NODE_LABEL", defaultNodeLabel),
		DatasourceName:     envOrDefault("DATASOURCE_NAME", defaultDatasource),
		SnapshotNamePrefix: strings.TrimSpace(os.Getenv("SNAPSHOT_NAME_PREFIX")),
	}
}

func parseBoolEnv(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}
