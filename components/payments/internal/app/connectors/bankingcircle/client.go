package bankingcircle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/logging"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type client struct {
	httpClient *http.Client

	username string
	password string

	endpoint              string
	authorizationEndpoint string

	logger logging.Logger

	accessToken          string
	accessTokenExpiresAt time.Time
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
}

func newClient(username, password, endpoint, authorizationEndpoint string, logger logging.Logger) (*client, error) {
	c := &client{
		httpClient: newHTTPClient(),

		username:              username,
		password:              password,
		endpoint:              endpoint,
		authorizationEndpoint: authorizationEndpoint,

		logger: logger,
	}

	if err := c.login(context.TODO()); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *client) login(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.authorizationEndpoint+"/api/v1/authorizations/authorize", http.NoBody)
	if err != nil {
		return fmt.Errorf("failed to create login request: %w", err)
	}

	req.SetBasicAuth(c.username, c.password)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to login: %w", err)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			c.logger.Error(err)
		}
	}()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read login response body: %w", err)
	}

	//nolint:tagliatelle // allow for client-side structures
	type response struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}

	var res response

	if err = json.Unmarshal(responseBody, &res); err != nil {
		return fmt.Errorf("failed to unmarshal login response: %w", err)
	}

	c.accessToken = res.AccessToken
	c.accessTokenExpiresAt = time.Now().Add(time.Duration(res.ExpiresIn) * time.Second)

	return nil
}

func (c *client) ensureAccessTokenIsValid(ctx context.Context) error {
	if c.accessTokenExpiresAt.After(time.Now()) {
		return nil
	}

	return c.login(ctx)
}

//nolint:tagliatelle // allow for client-side structures
type payment struct {
	PaymentID            string      `json:"paymentId"`
	TransactionReference string      `json:"transactionReference"`
	ConcurrencyToken     string      `json:"concurrencyToken"`
	Classification       string      `json:"classification"`
	Status               string      `json:"status"`
	Errors               interface{} `json:"errors"`
	LastChangedTimestamp time.Time   `json:"lastChangedTimestamp"`
	DebtorInformation    struct {
		PaymentBulkID interface{} `json:"paymentBulkId"`
		AccountID     string      `json:"accountId"`
		Account       struct {
			Account              string `json:"account"`
			FinancialInstitution string `json:"financialInstitution"`
			Country              string `json:"country"`
		} `json:"account"`
		VibanID interface{} `json:"vibanId"`
		Viban   struct {
			Account              string `json:"account"`
			FinancialInstitution string `json:"financialInstitution"`
			Country              string `json:"country"`
		} `json:"viban"`
		InstructedDate interface{} `json:"instructedDate"`
		DebitAmount    struct {
			Currency string  `json:"currency"`
			Amount   float64 `json:"amount"`
		} `json:"debitAmount"`
		DebitValueDate time.Time   `json:"debitValueDate"`
		FxRate         interface{} `json:"fxRate"`
		Instruction    interface{} `json:"instruction"`
	} `json:"debtorInformation"`
	Transfer struct {
		DebtorAccount interface{} `json:"debtorAccount"`
		DebtorName    interface{} `json:"debtorName"`
		DebtorAddress interface{} `json:"debtorAddress"`
		Amount        struct {
			Currency string  `json:"currency"`
			Amount   float64 `json:"amount"`
		} `json:"amount"`
		ValueDate             interface{} `json:"valueDate"`
		ChargeBearer          interface{} `json:"chargeBearer"`
		RemittanceInformation interface{} `json:"remittanceInformation"`
		CreditorAccount       interface{} `json:"creditorAccount"`
		CreditorName          interface{} `json:"creditorName"`
		CreditorAddress       interface{} `json:"creditorAddress"`
	} `json:"transfer"`
	CreditorInformation struct {
		AccountID string `json:"accountId"`
		Account   struct {
			Account              string `json:"account"`
			FinancialInstitution string `json:"financialInstitution"`
			Country              string `json:"country"`
		} `json:"account"`
		VibanID interface{} `json:"vibanId"`
		Viban   struct {
			Account              string `json:"account"`
			FinancialInstitution string `json:"financialInstitution"`
			Country              string `json:"country"`
		} `json:"viban"`
		CreditAmount struct {
			Currency string  `json:"currency"`
			Amount   float64 `json:"amount"`
		} `json:"creditAmount"`
		CreditValueDate time.Time   `json:"creditValueDate"`
		FxRate          interface{} `json:"fxRate"`
	} `json:"creditorInformation"`
}

func (c *client) getAllPayments(ctx context.Context) ([]*payment, error) {
	var payments []*payment

	for page := 0; ; page++ {
		pagedPayments, err := c.getPayments(ctx, page)
		if err != nil {
			return nil, err
		}

		if len(pagedPayments) == 0 {
			break
		}

		payments = append(payments, pagedPayments...)
	}

	return payments, nil
}

func (c *client) getPayments(ctx context.Context, page int) ([]*payment, error) {
	if err := c.ensureAccessTokenIsValid(ctx); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/api/v1/payments/singles", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create login request: %w", err)
	}

	q := req.URL.Query()
	q.Add("PageSize", "5000")
	q.Add("PageNumber", fmt.Sprint(page))

	req.URL.RawQuery = q.Encode()

	req.Header.Set("Authorization", "Bearer "+c.accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to login: %w", err)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			c.logger.Error(err)
		}
	}()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read login response body: %w", err)
	}

	type response struct {
		Result   []*payment `json:"result"`
		PageInfo struct {
			CurrentPage int `json:"currentPage"`
			PageSize    int `json:"pageSize"`
		} `json:"pageInfo"`
	}

	var res response

	if err = json.Unmarshal(responseBody, &res); err != nil {
		return nil, fmt.Errorf("failed to unmarshal login response: %w", err)
	}

	return res.Result, nil
}
