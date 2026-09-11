package connectionconfig

import (
	"errors"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func init() {
	registerSinkNormalizer(sinkNormalizers, "nats", normalizeNatsSink)
}

func normalizeNatsSink(input *commonpb.NatsSinkConfigInput) (*commonpb.NatsSinkConfig, error) {
	if input == nil {
		return nil, errors.New("NATS configuration is required")
	}
	cfg := &commonpb.NatsSinkConfig{Topic: input.GetTopic()}
	defaultScheme := "nats"
	for raw := range strings.SplitSeq(input.GetUrl(), ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if !strings.Contains(raw, "://") {
			raw = defaultScheme + "://" + raw
		}
		// Preserve the scheme delimiter so an empty host is rejected rather
		// than reinterpreted as a hostname.
		if !strings.HasSuffix(raw, "://") {
			raw = strings.TrimSuffix(raw, "/")
		}
		server, err := parseURL(raw, "nats")
		if err != nil {
			return nil, err
		}
		if len(cfg.GetServers()) == 0 && (server.GetScheme() == "ws" || server.GetScheme() == "wss") {
			defaultScheme = "ws"
		}
		if server.Password == nil {
			server.Token = server.GetUsername()
			server.Username = ""
		}
		cfg.Servers = append(cfg.Servers, server)
	}
	// An empty list retains the NATS driver's existing local-server default.
	return cfg, nil
}
