package client

import (
	"encoding/json"
	"fmt"
	"net/http"
)

//nolint:tagliatelle // allow different styled tags in client
type Transaction struct {
	ID              string      `json:"id"`
	Type            string      `json:"type"`
	Amount          float64     `json:"amount"`
	Credit          bool        `json:"credit"`
	SourceID        string      `json:"sourceId"`
	Description     string      `json:"description"`
	PostedDate      string      `json:"postedDate"`
	TransactionDate string      `json:"transactionDate"`
	Account         Account     `json:"account"`
	AdditionalInfo  interface{} `json:"additionalInfo"`
}

func (m *Client) GetTransactions(accountID string) ([]Transaction, error) {
	resp, err := m.httpClient.Get(m.buildEndpoint("accounts/%s/transactions", accountID))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var res responseWrapper[[]Transaction]
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return res.Content, nil
}
