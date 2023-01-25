package client

import (
	"encoding/json"
	"fmt"
	"net/http"
)

//nolint:tagliatelle // allow for clients
type Account struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Balance     string `json:"balance"`
	Currency    string `json:"currency"`
	CustomerID  string `json:"customerId"`
	Identifiers []struct {
		AccountNumber string `json:"accountNumber"`
		SortCode      string `json:"sortCode"`
		Type          string `json:"type"`
	} `json:"identifiers"`
	DirectDebit bool   `json:"directDebit"`
	CreatedDate string `json:"createdDate"`
}

func (m *Client) GetAccounts() ([]Account, error) {
	resp, err := m.httpClient.Get(m.buildEndpoint("accounts"))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var res responseWrapper[[]Account]
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return res.Content, nil
}
