// Package main implements a PayTheFly crypto payment connector for Formance Ledger.
//
// This connector receives PayTheFly webhook notifications and creates
// corresponding double-entry transactions in the Formance Ledger.
//
// PayTheFly supports BSC (chainId=56, 18 decimals) and TRON (chainId=728126428, 6 decimals).
//
// Environment variables:
//   - PAYTHEFLY_PROJECT_KEY: HMAC-SHA256 key for webhook signature verification
//   - LEDGER_URL: Formance Ledger API endpoint (e.g., http://localhost:3068)
//   - LEDGER_NAME: Target ledger name (default: "paythefly")
//   - LISTEN_ADDR: Webhook listener address (default: ":8081")
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"time"
)

// PayTheFly webhook envelope
type WebhookBody struct {
	Data      string `json:"data"`
	Sign      string `json:"sign"`
	Timestamp int64  `json:"timestamp"`
}

// PayTheFly webhook inner payload
// Fields: value (not amount), confirmed (not status), serial_no, tx_hash, wallet, tx_type
type WebhookPayload struct {
	Value     string `json:"value"`
	Confirmed bool   `json:"confirmed"`
	SerialNo  string `json:"serial_no"`
	TxHash    string `json:"tx_hash"`
	Wallet    string `json:"wallet"`
	TxType    int    `json:"tx_type"` // 1=payment, 2=withdrawal
}

// Formance Ledger transaction request
type LedgerTransaction struct {
	Postings  []Posting         `json:"postings"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Reference string            `json:"reference,omitempty"`
}

type Posting struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Amount      *big.Int `json:"amount"`
	Asset       string   `json:"asset"`
}

func main() {
	projectKey := os.Getenv("PAYTHEFLY_PROJECT_KEY")
	if projectKey == "" {
		log.Fatal("PAYTHEFLY_PROJECT_KEY environment variable is required")
	}

	ledgerURL := os.Getenv("LEDGER_URL")
	if ledgerURL == "" {
		ledgerURL = "http://localhost:3068"
	}

	ledgerName := os.Getenv("LEDGER_NAME")
	if ledgerName == "" {
		ledgerName = "paythefly"
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8081"
	}

	connector := &PayTheFlyConnector{
		projectKey: projectKey,
		ledgerURL:  ledgerURL,
		ledgerName: ledgerName,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}

	http.HandleFunc("/webhook/paythefly", connector.HandleWebhook)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	log.Printf("PayTheFly connector listening on %s", listenAddr)
	log.Printf("Ledger endpoint: %s (ledger: %s)", ledgerURL, ledgerName)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

type PayTheFlyConnector struct {
	projectKey string
	ledgerURL  string
	ledgerName string
	httpClient *http.Client
}

// HandleWebhook processes incoming PayTheFly webhook notifications.
// Webhook body: { "data": "<json string>", "sign": "<hmac hex>", "timestamp": <unix> }
// Signature: HMAC-SHA256(data + "." + timestamp, projectKey)
// Response must contain "success" string.
func (c *PayTheFlyConnector) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		log.Printf("[PayTheFly] failed to read body: %v", err)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var webhook WebhookBody
	if err := json.Unmarshal(body, &webhook); err != nil {
		log.Printf("[PayTheFly] failed to parse webhook body: %v", err)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// Verify HMAC-SHA256 signature (timing-safe comparison)
	if !c.verifySignature(webhook.Data, webhook.Timestamp, webhook.Sign) {
		log.Printf("[PayTheFly] signature verification failed")
		http.Error(w, "invalid signature", http.StatusForbidden)
		return
	}

	// Reject stale webhooks (> 5 minutes)
	if time.Now().Unix()-webhook.Timestamp > 300 {
		log.Printf("[PayTheFly] timestamp too old: %d", webhook.Timestamp)
		http.Error(w, "timestamp expired", http.StatusBadRequest)
		return
	}

	var payload WebhookPayload
	if err := json.Unmarshal([]byte(webhook.Data), &payload); err != nil {
		log.Printf("[PayTheFly] failed to parse payload: %v", err)
		http.Error(w, "invalid data", http.StatusBadRequest)
		return
	}

	// Only process confirmed payment transactions (tx_type=1)
	if payload.TxType == 1 && payload.Confirmed {
		if err := c.createLedgerTransaction(&payload); err != nil {
			log.Printf("[PayTheFly] failed to create ledger transaction: %v", err)
			// Still return success to avoid webhook retries
		} else {
			log.Printf("[PayTheFly] transaction recorded: serial_no=%s tx_hash=%s value=%s",
				payload.SerialNo, payload.TxHash, payload.Value)
		}
	} else if payload.TxType == 2 && payload.Confirmed {
		if err := c.createWithdrawalTransaction(&payload); err != nil {
			log.Printf("[PayTheFly] failed to create withdrawal transaction: %v", err)
		} else {
			log.Printf("[PayTheFly] withdrawal recorded: serial_no=%s tx_hash=%s value=%s",
				payload.SerialNo, payload.TxHash, payload.Value)
		}
	} else {
		log.Printf("[PayTheFly] skipping: tx_type=%d confirmed=%v serial_no=%s",
			payload.TxType, payload.Confirmed, payload.SerialNo)
	}

	// Response MUST contain "success" string per PayTheFly API spec
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "success")
}

// verifySignature verifies the HMAC-SHA256 signature using timing-safe comparison.
// Signature = HMAC-SHA256(data + "." + timestamp, projectKey)
func (c *PayTheFlyConnector) verifySignature(data string, timestamp int64, signature string) bool {
	message := fmt.Sprintf("%s.%d", data, timestamp)
	mac := hmac.New(sha256.New, []byte(c.projectKey))
	mac.Write([]byte(message))
	expectedMAC := hex.EncodeToString(mac.Sum(nil))

	return subtle.ConstantTimeCompare([]byte(expectedMAC), []byte(signature)) == 1
}

// createLedgerTransaction creates a double-entry payment transaction in Formance Ledger.
// Payment flow: world -> paythefly:payments:{wallet} (records incoming crypto payment)
func (c *PayTheFlyConnector) createLedgerTransaction(payload *WebhookPayload) error {
	amount := parseAmount(payload.Value)
	if amount.Cmp(big.NewInt(0)) <= 0 {
		return fmt.Errorf("invalid payment amount: %s", payload.Value)
	}

	tx := LedgerTransaction{
		Postings: []Posting{
			{
				Source:      "world",
				Destination: fmt.Sprintf("paythefly:payments:%s", payload.Wallet),
				Amount:      amount,
				Asset:       "CRYPTO/USDT",
			},
		},
		Metadata: map[string]string{
			"paythefly_serial_no": payload.SerialNo,
			"paythefly_tx_hash":   payload.TxHash,
			"paythefly_wallet":    payload.Wallet,
			"paythefly_value":     payload.Value,
			"paythefly_tx_type":   "payment",
		},
		Reference: fmt.Sprintf("ptf_%s", payload.SerialNo),
	}

	return c.postTransaction(tx)
}

// createWithdrawalTransaction creates a double-entry withdrawal transaction in Formance Ledger.
// Withdrawal flow: paythefly:treasury -> paythefly:withdrawals:{wallet}
func (c *PayTheFlyConnector) createWithdrawalTransaction(payload *WebhookPayload) error {
	amount := parseAmount(payload.Value)
	if amount.Cmp(big.NewInt(0)) <= 0 {
		return fmt.Errorf("invalid withdrawal amount: %s", payload.Value)
	}

	tx := LedgerTransaction{
		Postings: []Posting{
			{
				Source:      "paythefly:treasury",
				Destination: fmt.Sprintf("paythefly:withdrawals:%s", payload.Wallet),
				Amount:      amount,
				Asset:       "CRYPTO/USDT",
			},
		},
		Metadata: map[string]string{
			"paythefly_serial_no": payload.SerialNo,
			"paythefly_tx_hash":   payload.TxHash,
			"paythefly_wallet":    payload.Wallet,
			"paythefly_value":     payload.Value,
			"paythefly_tx_type":   "withdrawal",
		},
		Reference: fmt.Sprintf("ptf_w_%s", payload.SerialNo),
	}

	return c.postTransaction(tx)
}

// postTransaction sends a transaction to the Formance Ledger v2 API
func (c *PayTheFlyConnector) postTransaction(tx LedgerTransaction) error {
	body, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}

	url := fmt.Sprintf("%s/v2/%s/transactions", c.ledgerURL, c.ledgerName)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", tx.Reference)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ledger API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ledger API returned %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// parseAmount converts a human-readable decimal amount string to the smallest unit (big.Int).
// For BSC (18 decimals): "0.01" → 10000000000000000
// Uses 18 decimals by default (BSC). For TRON (6 decimals), override with PAYTHEFLY_DECIMALS env.
func parseAmount(value string) *big.Int {
	decimalsStr := os.Getenv("PAYTHEFLY_DECIMALS")
	decimals := 18
	if d, err := strconv.Atoi(decimalsStr); err == nil && d > 0 {
		decimals = d
	}

	// Parse the decimal string
	rat := new(big.Rat)
	if _, ok := rat.SetString(value); !ok {
		return big.NewInt(0)
	}

	// Multiply by 10^decimals
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	rat.Mul(rat, new(big.Rat).SetInt(multiplier))

	// Convert to integer (truncate any remaining fractional part)
	result := new(big.Int)
	result.Div(rat.Num(), rat.Denom())

	return result
}
