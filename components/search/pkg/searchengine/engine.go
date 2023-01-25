package searchengine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/formancehq/search/pkg/es"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type Engine interface {
	doRequest(ctx context.Context, m map[string]interface{}) (*es.Response, error)
}

type EngineFn func(ctx context.Context, m map[string]interface{}) (*es.Response, error)

func (fn EngineFn) doRequest(ctx context.Context, m map[string]interface{}) (*es.Response, error) {
	return fn(ctx, m)
}

var NotImplementedEngine = EngineFn(func(ctx context.Context, m map[string]interface{}) (*es.Response, error) {
	return nil, errors.New("not implemented")
})

type DefaultEngineOption interface {
	apply(*DefaultEngine)
}
type DefaultEngineOptionFn func(engine *DefaultEngine)

func (fn DefaultEngineOptionFn) apply(engine *DefaultEngine) {
	fn(engine)
}

func WithESIndices(esIndices ...string) DefaultEngineOptionFn {
	return func(engine *DefaultEngine) {
		engine.indices = esIndices
	}
}

func WithRequestOption(opt func(req *opensearchapi.SearchRequest)) DefaultEngineOptionFn {
	return func(engine *DefaultEngine) {
		engine.requestOptions = append(engine.requestOptions, opt)
	}
}

var DefaultEsIndices = []string{"ledger"}

var DefaultEngineOptions = []DefaultEngineOption{
	WithESIndices(DefaultEsIndices...),
}

type Response map[string][]interface{}

type DefaultEngine struct {
	openSearchClient *opensearch.Client
	indices          []string
	requestOptions   []func(req *opensearchapi.SearchRequest)
}

func (e *DefaultEngine) doRequest(ctx context.Context, m map[string]interface{}) (*es.Response, error) {

	ctx, span := otel.Tracer("com.formance.search").Start(ctx, "Search")
	defer span.End()

	recordFailingSpan := func(err error) error {
		if err == nil {
			return nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	data, err := json.Marshal(m)
	if err != nil {
		return nil, recordFailingSpan(errors.Wrap(err, "marshalling query"))
	}

	span.SetAttributes(
		attribute.String("query", string(data)),
		attribute.StringSlice("indices", e.indices),
	)

	httpResponse, err := e.openSearchClient.Search(
		e.openSearchClient.Search.WithBody(bytes.NewReader(data)),
		e.openSearchClient.Search.WithStoredFields("_all"),
		e.openSearchClient.Search.WithSource("*"),
		e.openSearchClient.Search.WithIndex(e.indices...),
		e.openSearchClient.Search.WithContext(ctx),
	)
	if err != nil {
		return nil, recordFailingSpan(errors.Wrap(err, "doing request"))
	}
	defer httpResponse.Body.Close()

	if httpResponse.IsError() {
		switch httpResponse.StatusCode {
		case 404:
			return &es.Response{}, nil
		default:
			data, err := io.ReadAll(httpResponse.Body)
			if err != nil || len(data) == 0 {
				return nil, recordFailingSpan(errors.New(httpResponse.Status()))
			}
			return nil, recordFailingSpan(errors.New(string(data)))
		}
	}

	res := &es.Response{}
	err = json.NewDecoder(httpResponse.Body).Decode(res)
	if err != nil {
		return nil, recordFailingSpan(errors.Wrap(err, "decoding result"))
	}

	span.SetAttributes(attribute.Int("hits.total.value", res.Hits.Total.Value))
	span.SetAttributes(attribute.String("hits.total.relation", res.Hits.Total.Relation))
	span.SetAttributes(attribute.Int("took", res.Took))

	return res, nil
}

func NewDefaultEngine(openSearchClient *opensearch.Client, opts ...DefaultEngineOption) *DefaultEngine {

	engine := &DefaultEngine{
		openSearchClient: openSearchClient,
	}
	opts = append(DefaultEngineOptions, opts...)
	for _, opt := range opts {
		opt.apply(engine)
	}
	return engine
}
