package ledger

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/formancehq/fctl/pkg/pluginsdk"
	httpclient "github.com/formancehq/fctl/pkg/pluginsdk/httpclient"
)

const defaultServiceVersion = "3.0.0"

type plugin struct {
	execute         func(context.Context, preparedRequest) (pluginsdk.ExecuteResponse, error)
	prepareEndpoint func(string) (*httpclient.Client, error)
	serviceVersion  string
}

type preparedRequest struct {
	request pluginsdk.ExecuteRequest
	op      operation
	path    string
	headers http.Header
	query   url.Values
	client  *httpclient.Client
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

	return &plugin{
		execute: executeHTTP,
		prepareEndpoint: func(endpoint string) (*httpclient.Client, error) {
			return httpclient.New(endpoint, httpClient)
		},
		serviceVersion: cmp.Or(serviceVersion, defaultServiceVersion),
	}
}

// NewWithExecutor reuses the Ledger manifest and command validation with an
// alternate transport. The executor receives normalized flags and an already
// prepared JSON Body; it owns transport-specific endpoint and payload decoding.
// The host retains authentication, file/stdin handling, prompts and rendering.
// The executor must honor ctx and return partial data alongside errors when
// applicable. It is called once per execution, without automatic retries.
// A nil executor permits manifest inspection but cannot execute commands.
func NewWithExecutor(serviceVersion string, executor func(context.Context, pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error)) pluginsdk.Plugin {
	p := &plugin{serviceVersion: cmp.Or(serviceVersion, defaultServiceVersion)}
	if executor != nil {
		p.execute = func(ctx context.Context, req preparedRequest) (pluginsdk.ExecuteResponse, error) {
			return executor(ctx, req.request)
		}
	}

	return p
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
	if p.prepareEndpoint == nil {
		if err := ctx.Err(); err != nil {
			return pluginsdk.ExecuteResponse{}, err
		}
	}
	manifest, operations := buildLayout()
	manifest.Version = p.serviceVersion
	req, err := pluginsdk.NormalizeRequest(manifest, req)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	op := operations[strings.Join(req.CommandPath, "/")]

	prepared, err := p.prepareRequest(op, req)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	if p.execute == nil {
		return pluginsdk.ExecuteResponse{}, errors.New("ledger command execution requires an executor")
	}

	return p.execute(ctx, prepared)
}

func (p *plugin) prepareRequest(op operation, req pluginsdk.ExecuteRequest) (preparedRequest, error) {
	if err := validateArgs(op, req.Args); err != nil {
		return preparedRequest{}, err
	}
	path, err := requestPath(op, req.Args, req.Flags["ledger"])
	if err != nil {
		return preparedRequest{}, err
	}
	headers, err := requestHeaders(op, req.Flags)
	if err != nil {
		return preparedRequest{}, err
	}
	query, err := requestQuery(op, req)
	if err != nil {
		return preparedRequest{}, err
	}
	// Preserve the HTTP adapter's endpoint validation order without requiring
	// an alternate executor to use an HTTP URL.
	var client *httpclient.Client
	if p.prepareEndpoint != nil {
		client, err = p.prepareEndpoint(req.Endpoint)
		if err != nil {
			return preparedRequest{}, err
		}
	}
	body, err := requestBody(op, req)
	if err != nil {
		return preparedRequest{}, err
	}
	req.Body = body

	return preparedRequest{request: req, op: op, path: path, headers: headers, query: query, client: client}, nil
}

func executeHTTP(ctx context.Context, req preparedRequest) (pluginsdk.ExecuteResponse, error) {
	result, err := req.client.Do(ctx, req.op.method, req.path, req.query, req.request.Body, req.headers)
	response := pluginsdk.ExecuteResponse{Data: result}
	if err != nil {
		if !req.op.bulk {
			return pluginsdk.ExecuteResponse{}, err
		}
		if failure, ok := errors.AsType[*httpclient.Error](err); ok {
			response.Data = failure.Body
		}

		return response, err
	}
	if req.op.bulk {
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
