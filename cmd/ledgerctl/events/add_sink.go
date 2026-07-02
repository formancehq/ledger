package events

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewAddSinkCommand creates the events add-sink command.
func NewAddSinkCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "add-sink",
		Aliases: []string{"add", "upsert"},
		Short:   "Add or update an event sink configuration",
		Long: `Add or update a named event sink configuration.

If a sink with the same name already exists, it is replaced (upsert).
The sink configuration is replicated via Raft consensus.

Currently supported sink types: NATS JetStream, ClickHouse, Kafka, HTTP.

Examples:
  # Add a NATS sink with default settings
  ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events

  # Add a NATS sink with custom batch settings
  ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events \
    --format protobuf --batch-size 128 --batch-delay-ms 50

  # Add a ClickHouse sink
  ledgerctl events add-sink --name analytics --ch-dsn clickhouse://user:pass@localhost:9000/db --ch-table ledger_events

  # Add a Kafka sink
  ledgerctl events add-sink --name streaming --kafka-brokers localhost:9092 --kafka-topic ledger-events

  # Add a Kafka sink with SASL authentication
  ledgerctl events add-sink --name streaming --kafka-brokers broker1:9092,broker2:9092 --kafka-topic ledger-events \
    --kafka-tls --kafka-sasl-mechanism SCRAM-SHA-256 --kafka-sasl-username user --kafka-sasl-password pass

  # Add an HTTP webhook sink
  ledgerctl events add-sink --name webhook --http-endpoint https://example.com/webhooks/ledger

  # Add an HTTP webhook sink with HMAC signature
  ledgerctl events add-sink --name webhook --http-endpoint https://example.com/webhooks/ledger --http-secret my-secret

  # Add a Databricks sink (PAT authentication)
  ledgerctl events add-sink --name analytics --databricks-host adb-123456.azuredatabricks.net \
    --databricks-http-path /sql/1.0/warehouses/abc123 --databricks-token dapi... \
    --databricks-catalog main --databricks-schema default

  # Add a Databricks sink (OAuth M2M / service principal — for workspaces where PATs are disabled)
  ledgerctl events add-sink --name analytics --databricks-host adb-123456.azuredatabricks.net \
    --databricks-http-path /sql/1.0/warehouses/abc123 \
    --databricks-client-id <client-id> --databricks-client-secret <client-secret> \
    --databricks-catalog main --databricks-schema default

  # Add a NATS sink that only receives transaction events
  ledgerctl events add-sink --name txn-only --nats-url nats://localhost:4222 --nats-topic txns \
    --event-types COMMITTED_TRANSACTION,REVERTED_TRANSACTION`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runAddSink,
	}

	cmd.Flags().String("name", "", "Unique name for this sink (required)")
	cmd.Flags().String("nats-url", "", "NATS server URL")
	cmd.Flags().String("nats-topic", "", "NATS topic/subject for events")
	cmd.Flags().String("ch-dsn", "", "ClickHouse DSN (e.g. clickhouse://user:pass@host:9000/db)")
	cmd.Flags().String("ch-table", "ledger_events", "ClickHouse table name")
	cmd.Flags().String("kafka-brokers", "", "Kafka broker addresses (comma-separated, e.g. localhost:9092)")
	cmd.Flags().String("kafka-topic", "", "Kafka topic name")
	cmd.Flags().Bool("kafka-tls", false, "Enable TLS for Kafka connection")
	cmd.Flags().String("kafka-sasl-mechanism", "", "Kafka SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	cmdutil.RegisterEnumCompletion(cmd, "kafka-sasl-mechanism", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
	cmd.Flags().String("kafka-sasl-username", "", "Kafka SASL username")
	cmd.Flags().String("kafka-sasl-password", "", "Kafka SASL password")
	cmd.Flags().String("http-endpoint", "", "HTTP webhook endpoint URL")
	cmd.Flags().String("http-secret", "", "HMAC-SHA256 secret for X-Webhook-Signature header")
	cmd.Flags().String("databricks-host", "", "Databricks server hostname (e.g. adb-123456.azuredatabricks.net)")
	cmd.Flags().String("databricks-http-path", "", "Databricks SQL Warehouse HTTP path")
	cmd.Flags().String("databricks-token", "", "Databricks Personal Access Token (PAT) — mutually exclusive with --databricks-client-id/--databricks-client-secret")
	cmd.Flags().String("databricks-client-id", "", "Databricks OAuth M2M service principal client ID — mutually exclusive with --databricks-token")
	cmd.Flags().String("databricks-client-secret", "", "Databricks OAuth M2M service principal client secret — mutually exclusive with --databricks-token")
	cmd.Flags().String("databricks-catalog", "", "Databricks Unity Catalog name")
	cmd.Flags().String("databricks-schema", "", "Databricks schema name")
	cmd.Flags().String("databricks-table", "ledger_events", "Databricks table name")
	cmd.Flags().Int32("databricks-port", 443, "Databricks port number")
	cmd.Flags().String("format", "json", "Event serialization format (json or protobuf)")
	cmdutil.RegisterEnumCompletion(cmd, "format", "json", "protobuf")
	cmd.Flags().Int32("batch-size", 0, "Max events per batch (default: 64)")
	cmd.Flags().Int64("batch-delay-ms", 0, "Max delay before flush in ms (default: 10)")
	cmd.Flags().String("event-types", "", "Comma-separated event types to filter (e.g. COMMITTED_TRANSACTION,REVERTED_TRANSACTION). Empty = all events")
	_ = cmd.RegisterFlagCompletionFunc("event-types", completeEventTypes)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runAddSink(cmd *cobra.Command, _ []string) error {
	name, _ := cmd.Flags().GetString("name")
	if name == "" {
		return errors.New("--name is required")
	}

	var (
		natsURL, _         = cmd.Flags().GetString("nats-url")
		natsTopic, _       = cmd.Flags().GetString("nats-topic")
		chDSN, _           = cmd.Flags().GetString("ch-dsn")
		chTable, _         = cmd.Flags().GetString("ch-table")
		kafkaBrokersStr, _ = cmd.Flags().GetString("kafka-brokers")
		kafkaTopic, _      = cmd.Flags().GetString("kafka-topic")
		kafkaTLS, _        = cmd.Flags().GetBool("kafka-tls")
		kafkaSASL, _       = cmd.Flags().GetString("kafka-sasl-mechanism")
		kafkaUser, _       = cmd.Flags().GetString("kafka-sasl-username")
		kafkaPass, _       = cmd.Flags().GetString("kafka-sasl-password")
		httpEndpoint, _    = cmd.Flags().GetString("http-endpoint")
		httpSecret, _      = cmd.Flags().GetString("http-secret")
		dbHost, _          = cmd.Flags().GetString("databricks-host")
		dbHTTPPath, _      = cmd.Flags().GetString("databricks-http-path")
		dbToken, _         = cmd.Flags().GetString("databricks-token")
		dbClientID, _      = cmd.Flags().GetString("databricks-client-id")
		dbClientSecret, _  = cmd.Flags().GetString("databricks-client-secret")
		dbCatalog, _       = cmd.Flags().GetString("databricks-catalog")
		dbSchema, _        = cmd.Flags().GetString("databricks-schema")
		dbTable, _         = cmd.Flags().GetString("databricks-table")
		dbPort, _          = cmd.Flags().GetInt32("databricks-port")
	)

	flagChanged := func(name string) bool {
		f := cmd.Flags().Lookup(name)

		return f != nil && f.Changed
	}

	hasNATS := natsURL != "" || natsTopic != ""
	hasCH := chDSN != ""
	hasKafka := kafkaBrokersStr != "" || kafkaTopic != ""
	hasHTTP := httpEndpoint != ""
	// Detect Databricks intent from any --databricks-* flag the user actually
	// set. Keying on dbHost alone silently swallows OAuth-only invocations
	// (--databricks-client-id without --databricks-host) and lets them either
	// fall through to "must specify a sink type" or combine with another sink
	// type because their flags are ignored.
	hasDatabricks := flagChanged("databricks-host") ||
		flagChanged("databricks-http-path") ||
		flagChanged("databricks-token") ||
		flagChanged("databricks-client-id") ||
		flagChanged("databricks-client-secret") ||
		flagChanged("databricks-catalog") ||
		flagChanged("databricks-schema") ||
		flagChanged("databricks-table") ||
		flagChanged("databricks-port")

	sinkCount := 0
	if hasNATS {
		sinkCount++
	}

	if hasCH {
		sinkCount++
	}

	if hasKafka {
		sinkCount++
	}

	if hasHTTP {
		sinkCount++
	}

	if hasDatabricks {
		sinkCount++
	}

	if sinkCount > 1 {
		return errors.New("cannot specify multiple sink types; choose one of: NATS (--nats-url), ClickHouse (--ch-dsn), Kafka (--kafka-brokers), HTTP (--http-endpoint), or Databricks (--databricks-host)")
	}

	if sinkCount == 0 {
		return errors.New("must specify a sink type: NATS (--nats-url and --nats-topic), ClickHouse (--ch-dsn), Kafka (--kafka-brokers and --kafka-topic), HTTP (--http-endpoint), or Databricks (--databricks-host)")
	}

	var (
		format, _       = cmd.Flags().GetString("format")
		batchSize, _    = cmd.Flags().GetInt32("batch-size")
		batchDelayMs, _ = cmd.Flags().GetInt64("batch-delay-ms")
	)

	if batchSize < 0 || batchSize > domain.MaxSinkBatchSize {
		return fmt.Errorf("--batch-size must be in [0, %d] (got %d)", domain.MaxSinkBatchSize, batchSize)
	}

	config := &commonpb.SinkConfig{
		Name:         name,
		Format:       format,
		BatchSize:    batchSize,
		BatchDelayMs: batchDelayMs,
	}

	eventTypesStr, _ := cmd.Flags().GetString("event-types")
	if eventTypesStr != "" {
		eventTypes, err := parseEventTypes(eventTypesStr)
		if err != nil {
			return err
		}

		config.EventTypes = eventTypes
	}

	var sinkType string

	switch {
	case hasNATS:
		if natsURL == "" || natsTopic == "" {
			return errors.New("--nats-url and --nats-topic are both required for NATS sinks")
		}

		config.Type = &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{
				Url:   natsURL,
				Topic: natsTopic,
			},
		}
		sinkType = "NATS"
	case hasCH:
		config.Type = &commonpb.SinkConfig_Clickhouse{
			Clickhouse: &commonpb.ClickHouseSinkConfig{
				Dsn:   chDSN,
				Table: chTable,
			},
		}
		sinkType = "ClickHouse"
	case hasKafka:
		if kafkaBrokersStr == "" || kafkaTopic == "" {
			return errors.New("--kafka-brokers and --kafka-topic are both required for Kafka sinks")
		}

		brokers := strings.Split(kafkaBrokersStr, ",")
		config.Type = &commonpb.SinkConfig_Kafka{
			Kafka: &commonpb.KafkaSinkConfig{
				Brokers:       brokers,
				Topic:         kafkaTopic,
				Tls:           kafkaTLS,
				SaslMechanism: kafkaSASL,
				SaslUsername:  kafkaUser,
				SaslPassword:  kafkaPass,
			},
		}
		sinkType = "Kafka"
	case hasHTTP:
		config.Type = &commonpb.SinkConfig_Http{
			Http: &commonpb.HttpSinkConfig{
				Endpoint: httpEndpoint,
				Secret:   httpSecret,
			},
		}
		sinkType = "HTTP"
	case hasDatabricks:
		hasPAT := dbToken != ""
		hasOAuth := dbClientID != "" || dbClientSecret != ""

		var missing []string
		if dbHost == "" {
			missing = append(missing, "--databricks-host")
		}

		if dbHTTPPath == "" {
			missing = append(missing, "--databricks-http-path")
		}

		if dbCatalog == "" {
			missing = append(missing, "--databricks-catalog")
		}

		if dbSchema == "" {
			missing = append(missing, "--databricks-schema")
		}

		switch {
		case len(missing) > 0:
			return fmt.Errorf("databricks sink is missing required flag(s): %s", strings.Join(missing, ", "))
		case hasPAT && hasOAuth:
			return errors.New("--databricks-token and --databricks-client-id/--databricks-client-secret are mutually exclusive — set exactly one auth method")
		case !hasPAT && !hasOAuth:
			return errors.New("databricks sink requires either --databricks-token (PAT) or both --databricks-client-id and --databricks-client-secret (OAuth M2M)")
		case hasOAuth && (dbClientID == "" || dbClientSecret == ""):
			return errors.New("--databricks-client-id and --databricks-client-secret must both be set for OAuth M2M authentication")
		}

		dbConfig := &commonpb.DatabricksSinkConfig{
			ServerHostname: dbHost,
			HttpPath:       dbHTTPPath,
			Catalog:        dbCatalog,
			Schema:         dbSchema,
			Table:          dbTable,
			Port:           dbPort,
		}
		if hasPAT {
			dbConfig.Auth = &commonpb.DatabricksSinkConfig_Token{Token: dbToken}
		} else {
			dbConfig.Auth = &commonpb.DatabricksSinkConfig_OauthM2M{
				OauthM2M: &commonpb.DatabricksOAuthM2M{
					ClientId:     dbClientID,
					ClientSecret: dbClientSecret,
				},
			}
		}

		config.Type = &commonpb.SinkConfig_Databricks{
			Databricks: dbConfig,
		}
		sinkType = "Databricks"
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Adding event sink %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_AddEventsSink{
				AddEventsSink: &servicepb.AddEventsSinkRequest{
					Config: config,
				},
			},
		},
	}

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, applyReq)
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to add event sink", err)
	}

	spinner.Success("Added")

	if handled, err := cmdutil.EncodeStructured(cmd, redactSinkConfig(config)); handled || err != nil {
		return err
	}

	pterm.Println()
	pterm.Printf("Sink: %s\n", pterm.Cyan(name))
	pterm.Println(pterm.Gray("─────────────────────────────────"))
	pterm.Printf("Type:   %s\n", sinkType)

	switch {
	case hasNATS:
		pterm.Printf("URL:    %s\n", natsURL)
		pterm.Printf("Topic:  %s\n", natsTopic)
	case hasCH:
		pterm.Printf("DSN:    %s\n", cmdutil.ObfuscateDSN(chDSN))
		pterm.Printf("Table:  %s\n", chTable)
	case hasKafka:
		pterm.Printf("Brokers: %s\n", kafkaBrokersStr)
		pterm.Printf("Topic:   %s\n", kafkaTopic)
	case hasHTTP:
		pterm.Printf("Endpoint: %s\n", httpEndpoint)

		if httpSecret != "" {
			pterm.Printf("Secret:   (set)\n")
		}
	case hasDatabricks:
		pterm.Printf("Host:    %s\n", dbHost)
		pterm.Printf("Path:    %s\n", dbHTTPPath)
		pterm.Printf("Catalog: %s\n", dbCatalog)
		pterm.Printf("Schema:  %s\n", dbSchema)
		pterm.Printf("Table:   %s\n", dbTable)
	}

	pterm.Printf("Format: %s\n", format)
	pterm.Printf("Events: %s\n", formatEventTypes(config.GetEventTypes()))

	return nil
}

// validEventTypes maps event type names (excluding UNSPECIFIED) to their proto values.
var validEventTypes = func() map[string]commonpb.EventType {
	m := make(map[string]commonpb.EventType, len(commonpb.EventType_name)-1)
	for v, name := range commonpb.EventType_name {
		if commonpb.EventType(v) == commonpb.EventType_EVENT_TYPE_UNSPECIFIED {
			continue
		}

		m[name] = commonpb.EventType(v)
	}

	return m
}()

// completeEventTypes provides shell completion for the comma-separated
// --event-types flag. It completes the segment after the last comma, keeps the
// already-typed prefix intact, and omits types the user has already selected.
// ShellCompDirectiveNoSpace lets the user keep appending ",<next>" without a
// space breaking the token.
func completeEventTypes(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	prefix, last := "", toComplete
	if idx := strings.LastIndex(toComplete, ","); idx >= 0 {
		prefix, last = toComplete[:idx+1], toComplete[idx+1:]
	}

	selected := make(map[string]struct{})
	for p := range strings.SplitSeq(prefix, ",") {
		if p = strings.ToUpper(strings.TrimSpace(p)); p != "" {
			selected[p] = struct{}{}
		}
	}

	upperLast := strings.ToUpper(strings.TrimSpace(last))

	var comps []string
	for name := range validEventTypes {
		if _, ok := selected[name]; ok {
			continue
		}

		if strings.HasPrefix(name, upperLast) {
			comps = append(comps, prefix+name)
		}
	}
	sort.Strings(comps)

	return comps, cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
}

// parseEventTypes parses a comma-separated list of event type names into proto enum values.
func parseEventTypes(s string) ([]commonpb.EventType, error) {
	parts := strings.Split(s, ",")

	result := make([]commonpb.EventType, 0, len(parts))
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if name == "" {
			continue
		}

		et, ok := validEventTypes[strings.ToUpper(name)]
		if !ok {
			var valid []string
			for n := range validEventTypes {
				valid = append(valid, n)
			}

			return nil, fmt.Errorf("unknown event type %q; valid types: %s", name, strings.Join(valid, ", "))
		}

		result = append(result, et)
	}

	return result, nil
}

// formatEventTypes returns a human-readable string for the event types filter.
func formatEventTypes(types []commonpb.EventType) string {
	if len(types) == 0 {
		return "all"
	}

	names := make([]string, len(types))
	for i, et := range types {
		names[i] = et.String()
	}

	return strings.Join(names, ", ")
}
