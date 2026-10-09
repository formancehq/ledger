package ledger

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/formancehq/fctl/pkg/pluginsdk"
	httpclient "github.com/formancehq/fctl/pkg/pluginsdk/httpclient"
)

const defaultServiceVersion = "3.0.0"

type plugin struct {
	http           *http.Client
	serviceVersion string
}

// New creates a Ledger plugin with the caller's HTTP transport. Authentication,
// endpoint selection and file/stdin handling belong to the host.
func New(httpClient *http.Client) pluginsdk.Plugin {
	return NewVersion(httpClient, defaultServiceVersion)
}

// NewVersion creates a plugin for the co-released Ledger service version.
// The version belongs to this instance; callers cannot mutate other instances.
func NewVersion(httpClient *http.Client, serviceVersion string) pluginsdk.Plugin {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &plugin{http: httpClient, serviceVersion: cmp.Or(serviceVersion, defaultServiceVersion)}
}

func (p *plugin) GetManifest(ctx context.Context) (pluginsdk.Manifest, error) {
	if err := ctx.Err(); err != nil {
		return pluginsdk.Manifest{}, err
	}
	manifest, _ := buildLayout()
	manifest.Version = p.serviceVersion

	return manifest, nil
}

func (p *plugin) Execute(ctx context.Context, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
	manifest, operations := buildLayout()
	manifest.Version = p.serviceVersion
	req, err := pluginsdk.NormalizeRequest(manifest, req)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	op := operations[strings.Join(req.CommandPath, "/")]

	return p.executeOperation(ctx, op, req)
}

func (p *plugin) executeOperation(ctx context.Context, op operation, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
	if err := validateArgs(op, req.Args); err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	path, err := requestPath(op, req.Args, req.Flags["ledger"])
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	headers, err := requestHeaders(op, req.Flags)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	query, err := requestQuery(op, req)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	client, err := httpclient.New(req.Endpoint, p.http)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	body, err := requestBody(op, req)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	result, err := client.Do(ctx, op.method, path, query, body, headers)
	response := pluginsdk.ExecuteResponse{Data: result}
	if err != nil {
		if !op.bulk {
			return pluginsdk.ExecuteResponse{}, err
		}
		if failure, ok := errors.AsType[*httpclient.Error](err); op.bulk && ok {
			response.Data = failure.Body
		}

		return response, err
	}
	if op.bulk {
		return response, bulkError(result)
	}

	return response, nil
}

func requestBody(op operation, req pluginsdk.ExecuteRequest) (json.RawMessage, error) {
	if op.body == bodyNone {
		return nil, nil
	}
	if len(req.Body) > 0 {
		return req.Body, nil
	}
	if op.body == bodyDefault && req.Flags["data"] == "{}" {
		return json.RawMessage("{}"), nil
	}
	if op.body == bodyRequired || req.ChangedFlags["data"] {
		return nil, errors.New("host must supply the already-read JSON request body")
	}

	return nil, nil
}

// bulkError inspects only status fields; payload numbers stay raw JSON.
func bulkError(result json.RawMessage) error {
	var response struct {
		ErrorCode string `json:"errorCode"`
		Data      []struct {
			ErrorCode    string `json:"errorCode"`
			ResponseType string `json:"responseType"`
		} `json:"data"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return fmt.Errorf("invalid bulk response: %w", err)
	}
	if response.ErrorCode != "" {
		return fmt.Errorf("bulk failed (%s); inspect the JSON response", response.ErrorCode)
	}
	for i, item := range response.Data {
		if item.ErrorCode != "" || item.ResponseType == "ERROR" {
			return fmt.Errorf("bulk element %d failed (%s); inspect the JSON response", i, item.ErrorCode)
		}
	}

	return nil
}
