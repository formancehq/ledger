package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

//nolint:tagliatelle // allow different styled tags in client
type Transaction struct {
	ID        string `json:"id"`
	Currency  string `json:"currency"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	Action    string `json:"action"`

	Amount string `json:"amount"`
}

func (c *Client) GetTransactions(ctx context.Context, page int) ([]Transaction, int, error) {
	if page < 1 {
		return nil, 0, fmt.Errorf("page must be greater than 0")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.buildEndpoint("v2/transactions/find?page=%d", page), http.NoBody)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Add("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	//nolint:tagliatelle // allow for client code
	type response struct {
		Transactions []Transaction `json:"transactions"`
		Pagination   struct {
			NextPage int `json:"next_page"`
		}
	}

	var res response
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, 0, err
	}

	return res.Transactions, res.Pagination.NextPage, nil
}
