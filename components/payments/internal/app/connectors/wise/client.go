package wise

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const apiEndpoint = "https://api.wise.com"

type apiTransport struct {
	APIKey     string
	underlying http.RoundTripper
}

func (t *apiTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", t.APIKey))

	return t.underlying.RoundTrip(req)
}

type client struct {
	httpClient *http.Client
}

type profile struct {
	ID   uint64 `json:"id"`
	Type string `json:"type"`
}

type transfer struct {
	ID             uint64  `json:"id"`
	Reference      string  `json:"reference"`
	Status         string  `json:"status"`
	SourceAccount  uint64  `json:"sourceAccount"`
	SourceCurrency string  `json:"sourceCurrency"`
	SourceValue    float64 `json:"sourceValue"`
	TargetAccount  uint64  `json:"targetAccount"`
	TargetCurrency string  `json:"targetCurrency"`
	TargetValue    float64 `json:"targetValue"`
	Business       uint64  `json:"business"`
	Created        string  `json:"created"`
	//nolint:tagliatelle // allow for clients
	CustomerTransactionID string `json:"customerTransactionId"`
	Details               struct {
		Reference string `json:"reference"`
	} `json:"details"`
	Rate float64 `json:"rate"`
	User uint64  `json:"user"`

	createdAt time.Time
}

func (t *transfer) UnmarshalJSON(data []byte) error {
	type Alias transfer

	aux := &struct {
		Created string `json:"created"`
		*Alias
	}{
		Alias: (*Alias)(t),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error

	t.createdAt, err = time.Parse("2006-01-02 15:04:05", aux.Created)
	if err != nil {
		return fmt.Errorf("failed to parse created time: %w", err)
	}

	return nil
}

func (w *client) endpoint(path string) string {
	return fmt.Sprintf("%s/%s", apiEndpoint, path)
}

func (w *client) getProfiles() ([]profile, error) {
	var profiles []profile

	res, err := w.httpClient.Get(w.endpoint("v1/profiles"))
	if err != nil {
		return profiles, err
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	err = json.Unmarshal(body, &profiles)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal profiles: %w", err)
	}

	return profiles, nil
}

func (w *client) getTransfers(ctx context.Context, profile *profile) ([]transfer, error) {
	var transfers []transfer

	limit := 10
	offset := 0

	for {
		req, err := http.NewRequestWithContext(ctx,
			http.MethodGet, w.endpoint("v1/transfers"), http.NoBody)
		if err != nil {
			return transfers, err
		}

		q := req.URL.Query()
		q.Add("limit", fmt.Sprintf("%d", limit))
		q.Add("profile", fmt.Sprintf("%d", profile.ID))
		q.Add("offset", fmt.Sprintf("%d", offset))
		req.URL.RawQuery = q.Encode()

		res, err := w.httpClient.Do(req)
		if err != nil {
			return transfers, err
		}

		body, err := io.ReadAll(res.Body)
		if err != nil {
			res.Body.Close()

			return nil, fmt.Errorf("failed to read response body: %w", err)
		}

		if err = res.Body.Close(); err != nil {
			return nil, fmt.Errorf("failed to close response body: %w", err)
		}

		var transferList []transfer

		err = json.Unmarshal(body, &transferList)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal transfers: %w", err)
		}

		transfers = append(transfers, transferList...)

		if len(transferList) < limit {
			break
		}

		offset += limit
	}

	return transfers, nil
}

func newClient(apiKey string) *client {
	httpClient := &http.Client{
		Transport: &apiTransport{
			APIKey:     apiKey,
			underlying: otelhttp.NewTransport(http.DefaultTransport),
		},
	}

	return &client{
		httpClient: httpClient,
	}
}
