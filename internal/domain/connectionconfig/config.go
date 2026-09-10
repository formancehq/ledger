// Package connectionconfig normalizes convenience connection strings into
// independent persisted components without consulting drivers, files or env.
package connectionconfig

import (
	"errors"
	"maps"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Mirror normalizes a source without changing its accepted, signed input.
func Mirror(input *commonpb.MirrorSourceConfigInput) (*commonpb.MirrorSourceConfig, error) {
	if input == nil {
		return nil, nil
	}
	out := &commonpb.MirrorSourceConfig{LedgerName: input.GetLedgerName(), BatchSize: input.GetBatchSize()}
	for _, rule := range input.GetRewriteRules() {
		out.RewriteRules = append(out.RewriteRules, rule.CloneVT())
	}
	switch source := input.GetType().(type) {
	case *commonpb.MirrorSourceConfigInput_Http:
		if source.Http == nil {
			return nil, errors.New("HTTP mirror configuration is required")
		}
		endpoint, err := parseURL(source.Http.GetBaseUrl(), "http")
		if err != nil {
			return nil, err
		}
		cfg := &commonpb.HttpMirrorSourceConfig{BaseUrl: endpoint}
		if auth := source.Http.GetOauth2ClientCredentials(); auth != nil {
			token, err := parseURL(auth.GetTokenEndpoint(), "http")
			if err != nil {
				return nil, err
			}
			cfg.Oauth2ClientCredentials = &commonpb.OAuth2ClientCredentials{ClientId: auth.GetClientId(), ClientSecret: auth.GetClientSecret(), TokenEndpoint: token, Scopes: append([]string(nil), auth.GetScopes()...)}
		}
		out.Type = &commonpb.MirrorSourceConfig_Http{Http: cfg}
	case *commonpb.MirrorSourceConfigInput_Postgres:
		if source.Postgres == nil {
			return nil, errors.New("PostgreSQL mirror configuration is required")
		}
		connection, err := parseDatabase(source.Postgres.GetDsn(), true)
		if err != nil {
			return nil, err
		}
		if source.Postgres.GetAwsIamAuth() != nil && !PostgresEnforcesTLS(connection) {
			return nil, errors.New("AWS IAM requires explicit TLS on every PostgreSQL endpoint")
		}
		out.Type = &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Connection: connection, AwsIamAuth: source.Postgres.GetAwsIamAuth().CloneVT()}}
	default:
		return nil, errors.New("mirror source type is required")
	}

	return out, nil
}

func parseURL(raw, kind string) (*commonpb.ConnectionURL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || u.Host == "" {
		return nil, errors.New("invalid connection URL")
	}
	valid := u.Scheme == "http" || u.Scheme == "https"
	if kind == "nats" {
		// The pinned NATS driver treats schemes other than ws/wss as native.
		valid = u.Scheme != ""
	}
	if kind == "proxy" {
		valid = valid || u.Scheme == "socks5" || u.Scheme == "socks5h"
	}
	if !valid {
		return nil, errors.New("unsupported connection URL scheme")
	}
	address, err := parseAddress(u.Host)
	if err != nil {
		return nil, err
	}
	out := &commonpb.ConnectionURL{Scheme: u.Scheme, Address: address, EscapedPath: u.EscapedPath()}
	if u.User != nil {
		out.Username = u.User.Username()
		if password, present := u.User.Password(); present {
			out.Password = &password
		}
	}
	if u.RawQuery != "" || u.ForceQuery {
		for part := range strings.SplitSeq(u.RawQuery, "&") {
			name, value, present := strings.Cut(part, "=")
			if _, err := url.QueryUnescape(name); err != nil {
				return nil, errors.New("invalid URL query name")
			}
			if _, err := url.QueryUnescape(value); err != nil {
				return nil, errors.New("invalid URL query value")
			}
			parameter := &commonpb.ConnectionQueryParameter{EscapedName: name}
			if present {
				parameter.EscapedValue = &value
			}
			out.Query = append(out.Query, parameter)
		}
	}

	return out, nil
}

func parseAddress(raw string) (*commonpb.ConnectionAddress, error) {
	if raw == "" {
		return &commonpb.ConnectionAddress{}, nil
	}
	// Unix-domain socket directories occur in PostgreSQL host settings.
	if strings.HasPrefix(raw, "/") {
		return &commonpb.ConnectionAddress{Host: raw}, nil
	}
	ipHost := raw
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		ipHost = strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]")
	}
	ip, _, _ := strings.Cut(ipHost, "%")
	if net.ParseIP(ip) != nil {
		return &commonpb.ConnectionAddress{Host: ipHost}, nil
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		if strings.ContainsAny(raw, ":@/?#[]") {
			return nil, errors.New("invalid connection address")
		}
		host = raw
	}
	out := &commonpb.ConnectionAddress{Host: host}
	if port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return nil, errors.New("invalid connection port")
		}
		value := uint32(n)
		out.Port = &value
	}

	return out, nil
}

func parseDatabase(raw string, postgres bool) (*commonpb.DatabaseConnection, error) {
	settings := map[string]string{}
	extras := map[string]string{}
	scheme := "postgres"
	if postgres && !strings.HasPrefix(raw, "postgres://") && !strings.HasPrefix(raw, "postgresql://") {
		var err error
		settings, err = parseKeywords(raw)
		if err != nil {
			return nil, err
		}
	} else {
		u, err := url.Parse(raw)
		if err != nil || u.Opaque != "" {
			return nil, errors.New("invalid database URL")
		}
		scheme = u.Scheme
		if postgres {
			if scheme != "postgres" && scheme != "postgresql" {
				return nil, errors.New("unsupported PostgreSQL URL scheme")
			}
			scheme = "postgres"
		}
		if !postgres && u.Host == "" {
			return nil, errors.New("ClickHouse host is required")
		}
		if u.Host != "" {
			if postgres {
				var hosts, ports []string
				for rawHost := range strings.SplitSeq(u.Host, ",") {
					if rawHost == "" {
						continue
					}
					address, err := parseAddress(rawHost)
					if err != nil {
						return nil, err
					}
					if address.GetHost() != "" {
						hosts = append(hosts, address.GetHost())
					}
					if address.Port != nil {
						ports = append(ports, strconv.FormatUint(uint64(address.GetPort()), 10))
					}
				}
				if len(hosts) > 0 {
					settings["host"] = strings.Join(hosts, ",")
				}
				if len(ports) > 0 {
					settings["port"] = strings.Join(ports, ",")
				}
			} else {
				settings["host"] = u.Host
			}
		}
		if u.User != nil {
			if username := u.User.Username(); username != "" {
				settings["user"] = username
			}
			if password, present := u.User.Password(); present {
				settings["password"] = password
			}
		}
		database := strings.TrimPrefix(u.Path, "/")
		if postgres {
			database = strings.TrimLeft(u.Path, "/")
		}
		if database != "" || (!postgres && u.Path != "") {
			settings["database"] = database
		}
		values, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			return nil, errors.New("invalid database URL query")
		}
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if _, a := values["dbname"]; a && postgres {
			if _, b := values["database"]; b {
				return nil, errors.New("conflicting database aliases")
			}
		}
		for _, key := range keys {
			normalized := key
			if postgres && key == "dbname" {
				normalized = "database"
			}
			if !postgres && key == "username" {
				normalized = "user"
			}
			if !postgres && key != "username" && key != "password" && key != "database" {
				extras[key] = values[key][0]
			} else {
				settings[normalized] = values[key][0]
			}
		}
	}
	out := &commonpb.DatabaseConnection{Scheme: scheme}
	if username, present := settings["user"]; present {
		out.Username = &username
	}
	if database, present := settings["database"]; present {
		out.Database = &database
	}
	if password, present := settings["password"]; present {
		out.Password = &password
	}
	hosts, hasHosts := settings["host"]
	ports, hasPorts := settings["port"]
	if hasPorts {
		for port := range strings.SplitSeq(ports, ",") {
			if _, err := strconv.ParseUint(port, 10, 16); err != nil {
				return nil, errors.New("invalid database port")
			}
		}
	}
	if hasHosts {
		hostList := strings.Split(hosts, ",")
		portList := strings.Split(ports, ",")
		for i, host := range hostList {
			address := &commonpb.ConnectionAddress{Host: host}
			if !postgres {
				var err error
				address, err = parseAddress(host)
				if err != nil {
					return nil, err
				}
			}
			if hasPorts {
				port := portList[0]
				if i < len(portList) {
					port = portList[i]
				}
				if port != "" {
					n, err := strconv.ParseUint(port, 10, 16)
					if err != nil {
						return nil, errors.New("invalid database port")
					}
					value := uint32(n)
					address.Port = &value
				}
			}
			out.Addresses = append(out.Addresses, address)
		}
	}
	if hasHosts {
		delete(settings, "port")
	}
	for _, key := range []string{"host", "user", "password", "database"} {
		delete(settings, key)
	}
	maps.Copy(settings, extras)
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := settings[key]
		option := &commonpb.ConnectionOption{Name: key}
		switch {
		case !postgres && key == "http_proxy":
			proxy, err := parseURL(value, "proxy")
			if err != nil {
				return nil, err
			}
			option.Value = &commonpb.ConnectionOption_Url{Url: proxy}
		case publicOption(key, postgres):
			option.Value = &commonpb.ConnectionOption_PublicText{PublicText: value}
		default:
			option.Value = &commonpb.ConnectionOption_SecretText{SecretText: value}
		}
		out.Options = append(out.Options, option)
	}

	return out, nil
}

func publicOption(name string, postgres bool) bool {
	if postgres {
		switch name {
		case "port", "connect_timeout", "sslmode", "sslkey", "sslcert", "sslrootcert", "sslsni", "sslnegotiation", "krbspn", "krbsrvname", "target_session_attrs", "service", "servicefile", "passfile", "min_protocol_version", "max_protocol_version", "channel_binding", "application_name", "timezone", "statement_cache_capacity", "description_cache_capacity", "default_query_exec_mode", "pool_max_conns", "pool_min_conns", "pool_min_idle_conns", "pool_max_conn_lifetime", "pool_max_conn_idle_time", "pool_health_check_period", "pool_max_conn_lifetime_jitter":
			return true
		}
	} else {
		switch name {
		case "debug", "compress", "compress_level", "max_compression_buffer", "dial_timeout", "block_buffer_size", "read_timeout", "secure", "skip_verify", "connection_open_strategy", "max_open_conns", "max_idle_conns", "conn_max_lifetime", "client_info_product", "http_path":
			return true
		}
	}

	return false
}

func parseKeywords(raw string) (map[string]string, error) {
	out := map[string]string{}
	for position := 0; position < len(raw); {
		for position < len(raw) && space(raw[position]) {
			position++
		}
		if position == len(raw) {
			break
		}
		next := strings.IndexByte(raw[position:], '=')
		if next < 0 {
			return nil, errors.New("invalid PostgreSQL keyword setting")
		}
		equal := position + next
		key := strings.Trim(raw[position:equal], " \t\n\r\v\f")
		if key == "" || strings.ContainsAny(key, " \t\n\r\v\f'\\:/") {
			return nil, errors.New("invalid PostgreSQL setting name")
		}
		position = equal + 1
		for position < len(raw) && space(raw[position]) {
			position++
		}
		quoted := position < len(raw) && raw[position] == '\''
		if quoted {
			position++
		}
		var value strings.Builder
		closed := !quoted
		for position < len(raw) {
			ch := raw[position]
			if quoted && ch == '\'' {
				position++
				closed = true

				break
			}
			if !quoted && space(ch) {
				break
			}
			if ch == '\\' {
				position++
				if position == len(raw) {
					return nil, errors.New("invalid PostgreSQL escape")
				}
				ch = raw[position]
				if ch != '\\' && ch != '\'' {
					value.WriteByte('\\')
				}
			}
			value.WriteByte(ch)
			position++
		}
		if !closed {
			return nil, errors.New("unterminated PostgreSQL quoted setting")
		}
		if key == "dbname" {
			key = "database"
		}
		if key == "user" && value.Len() == 0 {
			continue
		}
		out[key] = value.String()
	}

	return out, nil
}

func space(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\v' || ch == '\f'
}

// RenderURL constructs the driver URL exclusively from normalized components.
func RenderURL(config *commonpb.ConnectionURL) string {
	if config == nil {
		return ""
	}
	u := &url.URL{Scheme: config.GetScheme(), Host: renderAddress(config.GetAddress())}
	path, err := url.PathUnescape(config.GetEscapedPath())
	if err != nil {
		panic("invalid normalized connection path")
	}
	u.Path = path
	u.RawPath = config.GetEscapedPath()
	switch {
	case config.GetToken() != "":
		u.User = url.User(config.GetToken())
	case config.Password != nil:
		u.User = url.UserPassword(config.GetUsername(), config.GetPassword())
	case config.GetUsername() != "":
		u.User = url.User(config.GetUsername())
	}
	parts := make([]string, 0, len(config.GetQuery()))
	for _, query := range config.GetQuery() {
		part := query.GetEscapedName()
		if query.EscapedValue != nil {
			part += "=" + query.GetEscapedValue()
		}
		parts = append(parts, part)
	}
	u.RawQuery = strings.Join(parts, "&")
	u.ForceQuery = len(parts) > 0 && u.RawQuery == ""

	return u.String()
}

func renderAddress(address *commonpb.ConnectionAddress) string {
	if address == nil {
		return ""
	}
	if address.Port != nil {
		return net.JoinHostPort(address.GetHost(), strconv.FormatUint(uint64(address.GetPort()), 10))
	}
	if strings.Contains(address.GetHost(), ":") && !strings.HasPrefix(address.GetHost(), "/") {
		return "[" + address.GetHost() + "]"
	}

	return address.GetHost()
}

// RenderDatabase serializes normalized settings for the existing driver parser;
// it never falls back to an original input string. Driver environment defaults
// remain runtime behavior, outside deterministic order normalization.
func RenderDatabase(config *commonpb.DatabaseConnection) string {
	if config == nil {
		return ""
	}
	if config.GetScheme() == "postgres" {
		values := map[string]string{}
		if len(config.GetAddresses()) > 0 {
			var hosts, ports []string
			anyPort := false
			for _, address := range config.GetAddresses() {
				hosts = append(hosts, address.GetHost())
				if address.Port != nil {
					ports = append(ports, strconv.FormatUint(uint64(address.GetPort()), 10))
					anyPort = true
				} else {
					ports = append(ports, "5432")
				}
			}
			values["host"] = strings.Join(hosts, ",")
			if anyPort {
				values["port"] = strings.Join(ports, ",")
			}
		}
		if config.Username != nil {
			values["user"] = config.GetUsername()
		}
		if config.Password != nil {
			values["password"] = config.GetPassword()
		}
		if config.Database != nil {
			values["dbname"] = config.GetDatabase()
		}
		for _, option := range config.GetOptions() {
			values[option.GetName()] = optionValue(option)
		}
		query := url.Values{}
		for key, value := range values {
			query.Set(key, value)
		}
		if len(query) == 0 {
			return "postgres://"
		}

		return "postgres://?" + query.Encode()
	}
	var addresses []string
	for _, address := range config.GetAddresses() {
		addresses = append(addresses, renderAddress(address))
	}
	u := &url.URL{Scheme: config.GetScheme(), Host: strings.Join(addresses, ","), Path: "/" + config.GetDatabase()}
	if config.Password != nil {
		u.User = url.UserPassword(config.GetUsername(), config.GetPassword())
	} else if config.GetUsername() != "" {
		u.User = url.User(config.GetUsername())
	}
	query := url.Values{}
	for _, option := range config.GetOptions() {
		query.Set(option.GetName(), optionValue(option))
	}
	u.RawQuery = query.Encode()

	return u.String()
}

func optionValue(option *commonpb.ConnectionOption) string {
	switch value := option.GetValue().(type) {
	case *commonpb.ConnectionOption_PublicText:
		return value.PublicText
	case *commonpb.ConnectionOption_SecretText:
		return value.SecretText
	case *commonpb.ConnectionOption_Url:
		return RenderURL(value.Url)
	default:
		panic("normalized connection option has no value")
	}
}

// PostgresEnforcesTLS is an environment-independent admission check for IAM.
func PostgresEnforcesTLS(config *commonpb.DatabaseConnection) bool {
	if config == nil || len(config.GetAddresses()) == 0 {
		return false
	}
	mode := ""
	for _, option := range config.GetOptions() {
		if option.GetName() == "sslmode" {
			mode = optionValue(option)
		}
	}
	if mode != "require" && mode != "verify-ca" && mode != "verify-full" {
		return false
	}
	for _, address := range config.GetAddresses() {
		if address.GetHost() == "" || strings.HasPrefix(address.GetHost(), "/") {
			return false
		}
	}

	return true
}

// RenderNATS builds a server list only from normalized server components.
func RenderNATS(config *commonpb.NatsSinkConfig) string {
	var servers []string
	for _, server := range config.GetServers() {
		servers = append(servers, RenderURL(server))
	}

	return strings.Join(servers, ",")
}
