package ledger

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

type bodyMode uint8

const (
	bodyNone bodyMode = iota
	bodyRequired
	bodyOptional
	bodyDefault
)

type operation struct {
	use, short, method string
	args               int
	global, ledgerArg  bool
	transactionID      bool
	path               func([]string) []string
	body               bodyMode
	validateBody       payloadValidator
	idempotency        bool
	page, reverse      bool
	afterID            bool
	filter, dates      bool
	inspect, bulk      bool
	boolQuery          map[string]string
	stringQuery        map[string]string
}

func (layout) endpoint(op operation) *node {
	if op.method == "" {
		op.method = http.MethodGet
	}
	spec := pluginsdk.CommandSpec{Use: op.use, Short: op.short, Args: pluginsdk.ArgsSpec{Min: op.args, Max: op.args}, Runnable: true, Confirm: op.method == http.MethodDelete}
	if op.ledgerArg {
		spec.Args = pluginsdk.ArgsSpec{Min: 0, Max: 1}
	}
	spec.Flags = append(bodyFlags(op), queryFlags(op)...)
	if !op.global {
		spec.Inputs = []pluginsdk.InputSpec{ledgerInput(op)}
	}

	return &node{spec: spec, op: &op}
}

func bodyFlags(op operation) []pluginsdk.FlagSpec {
	var flags []pluginsdk.FlagSpec
	if op.body != bodyNone {
		value := ""
		if op.body == bodyDefault {
			value = "{}"
		}
		flags = append(flags, pluginsdk.FlagSpec{Name: "data", Type: "string", Default: value, Body: true, Required: op.body == bodyRequired, Usage: "JSON request body: inline JSON, @file or - for stdin"})
	}
	if op.idempotency {
		flags = append(flags, pluginsdk.FlagSpec{Name: "idempotency-key", Type: "string", Usage: "Stable request identity (max 256 bytes); never generated automatically"})
	}
	if op.method == http.MethodDelete {
		flags = append(flags, pluginsdk.FlagSpec{Name: "confirm", Type: "bool", Default: "false", Usage: "Confirm this destructive deletion"})
	}

	return flags
}

func queryFlags(op operation) []pluginsdk.FlagSpec {
	var flags []pluginsdk.FlagSpec
	if op.page || op.inspect {
		usage := "Results per page (0 means 100; server caps at 1000)"
		if op.inspect {
			usage = "Results per index page (1 through 10000)"
		}
		flags = append(flags, pluginsdk.FlagSpec{Name: "page-size", Type: "uint32", Default: "100", Usage: usage})
		flags = append(flags, pluginsdk.FlagSpec{Name: "cursor", Type: "string", Usage: "Opaque next or previous page token returned by the server"})
		if !op.inspect && !op.global {
			afterUsage := "Compatibility alias for --cursor: continue after this account address (exclusive)"
			if op.afterID {
				afterUsage = "Compatibility alias for --cursor: continue after this unsigned transaction or ledger-local log ID (exclusive)"
			}
			flags = append(flags, pluginsdk.FlagSpec{Name: "after", Type: "string", Usage: afterUsage})
		}
	}
	if op.reverse {
		flags = append(flags, pluginsdk.FlagSpec{Name: "reverse", Type: "bool", Default: "false", Usage: "Reverse the endpoint's default ordering"})
	}
	if op.filter {
		flags = append(flags, pluginsdk.FlagSpec{Name: "filter", Type: "string", Usage: "Exact v3 filter expression or structured JSON filter"})
	}
	if op.dates {
		flags = append(flags, pluginsdk.FlagSpec{Name: "start-date", Type: "string", Usage: "Inclusive start date (RFC3339)"}, pluginsdk.FlagSpec{Name: "end-date", Type: "string", Usage: "Exclusive end date (RFC3339)"})
	}
	if op.inspect {
		flags = append(flags, pluginsdk.FlagSpec{Name: "mode", Type: "string", Default: "summary", Usage: "Index inspection: summary, distinctValues or facets"})
	}
	for _, name := range slices.Sorted(maps.Keys(op.boolQuery)) {
		flags = append(flags, pluginsdk.FlagSpec{Name: name, Type: "bool", Default: "false", Usage: "Set the v3 " + op.boolQuery[name] + " query option"})
	}
	for _, name := range slices.Sorted(maps.Keys(op.stringQuery)) {
		flags = append(flags, pluginsdk.FlagSpec{Name: name, Type: "string", Usage: "Set the v3 " + op.stringQuery[name] + " query option"})
	}

	return flags
}

func validateArgs(op operation, args []string) error {
	if slices.Contains(args, "") {
		return errors.New("resource identifiers must not be empty")
	}
	if op.transactionID {
		if _, err := strconv.ParseUint(args[0], 10, 64); err != nil {
			return fmt.Errorf("transaction id must be an unsigned 64-bit integer: %w", err)
		}
	}

	return nil
}

func requestHeaders(op operation, flags map[string]string) (http.Header, error) {
	if op.method == http.MethodDelete && flags["confirm"] != "true" {
		return nil, errors.New("deletion requires --confirm")
	}
	if len(flags["idempotency-key"]) > 256 || strings.ContainsAny(flags["idempotency-key"], "\r\n") {
		return nil, errors.New("idempotency-key must be at most 256 bytes without newlines")
	}
	headers := make(http.Header)
	if flags["idempotency-key"] != "" {
		headers.Set("Idempotency-Key", flags["idempotency-key"])
	}
	if flags["consistency"] != "" {
		consistency := strings.ToLower(strings.TrimSpace(flags["consistency"]))
		if consistency != "linearizable" && consistency != "stale" {
			return nil, errors.New("consistency must be linearizable or stale")
		}
		if !op.global || op.page {
			headers.Set("X-Consistency", consistency)
		}
	}

	return headers, nil
}

func requestQuery(op operation, req pluginsdk.ExecuteRequest) (url.Values, error) {
	query := make(url.Values)
	if err := paginationQuery(query, op, req.Flags); err != nil {
		return nil, err
	}
	if op.reverse && req.ChangedFlags["reverse"] {
		query.Set("reverse", req.Flags["reverse"])
	}
	if req.Flags["filter"] != "" {
		query.Set("filter", req.Flags["filter"])
	}
	if err := dateQuery(query, req.Flags); err != nil {
		return nil, err
	}
	if op.inspect {
		if err := inspectQuery(query, req.Flags); err != nil {
			return nil, err
		}
	}
	copyOptions(req, query, op.boolQuery)
	copyOptions(req, query, op.stringQuery)

	return query, nil
}

func paginationQuery(query url.Values, op operation, flags map[string]string) error {
	if op.page || op.inspect {
		query.Set("pageSize", flags["page-size"])
		if flags["cursor"] != "" {
			query.Set("cursor", flags["cursor"])
		}
	}

	return nil
}

// normalizePagination prepares the same forward token for HTTP and alternate
// executors. --after is consumed before dispatch; cursor is the canonical input.
// Index inspection's value tokens remain opaque here.
func normalizePagination(op operation, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteRequest, error) {
	if !op.page {
		return req, nil
	}
	if after := req.Flags["after"]; after != "" {
		if req.Flags["cursor"] != "" {
			return req, errors.New("--after and --cursor cannot be used together")
		}
		if op.afterID {
			id, err := strconv.ParseUint(after, 10, 64)
			if err != nil {
				return req, fmt.Errorf("after must be an unsigned 64-bit transaction or log ID: %w", err)
			}
			after = strconv.FormatUint(id, 10)
		}
		token, err := json.Marshal(struct {
			Key string `json:"key"`
		}{Key: after})
		if err != nil {
			return req, fmt.Errorf("encode after cursor: %w", err)
		}
		req.Flags["cursor"] = base64.RawURLEncoding.EncodeToString(token)
		req.ChangedFlags = maps.Clone(req.ChangedFlags)
		if req.ChangedFlags == nil {
			req.ChangedFlags = make(map[string]bool)
		}
		req.ChangedFlags["cursor"] = true
		delete(req.Flags, "after")
		delete(req.ChangedFlags, "after")
	}
	if token := req.Flags["cursor"]; token != "" {
		if err := validatePageCursor(token, op.afterID); err != nil {
			return req, fmt.Errorf("invalid --cursor: %w", err)
		}
	}

	return req, nil
}

func validatePageCursor(token string, numeric bool) error {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return fmt.Errorf("expected a base64url page token: %w", err)
	}
	fields, err := payloadObject(raw, "cursor")
	if err != nil {
		return err
	}
	var key string
	for name, value := range fields {
		switch name {
		case "key":
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return errors.New("cursor.key must be a string")
			}
			if err := json.Unmarshal(value, &key); err != nil {
				return fmt.Errorf("cursor.key must be a string: %w", err)
			}
		case "back":
			if err := validateJSONBool(value, "cursor.back"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown cursor field %q", name)
		}
	}
	if numeric && key != "" {
		if _, err := strconv.ParseUint(key, 10, 64); err != nil {
			return fmt.Errorf("cursor.key must be an unsigned 64-bit transaction or log ID: %w", err)
		}
	}

	return nil
}

func dateQuery(query url.Values, flags map[string]string) error {
	for name, value := range map[string]string{"startDate": flags["start-date"], "endDate": flags["end-date"]} {
		if value == "" {
			continue
		}
		date, err := time.Parse(time.RFC3339Nano, value)
		if err != nil || date.Before(time.Unix(0, 0)) {
			return fmt.Errorf("%s must be an RFC3339 date on or after the Unix epoch", name)
		}
		query.Set(name, value)
	}

	return nil
}

func inspectQuery(query url.Values, flags map[string]string) error {
	if flags["mode"] != "summary" && flags["mode"] != "distinctValues" && flags["mode"] != "facets" {
		return errors.New("mode must be summary, distinctValues or facets")
	}
	size, err := strconv.ParseUint(flags["page-size"], 10, 32)
	if err != nil || size < 1 || size > 10000 {
		return errors.New("index page-size must be between 1 and 10000")
	}
	query.Set("mode", flags["mode"])

	return nil
}

func copyOptions(req pluginsdk.ExecuteRequest, query url.Values, options map[string]string) {
	for flag, param := range options {
		if req.ChangedFlags[flag] {
			query.Set(param, req.Flags[flag])
		}
	}
}
