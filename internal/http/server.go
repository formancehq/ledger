package http

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

type Server struct {
	server     *http.Server
	logger     *zap.Logger
	ledgerService service.Ledger
	port       int
}

func NewServer(port int, logger *zap.Logger, ledgerService service.Ledger) *Server {
	return &Server{
		logger:        logger,
		ledgerService: ledgerService,
		port:          port,
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/transactions", s.handleCreateTransaction)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	s.logger.Info("Starting HTTP server", zap.Int("port", s.port))

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server failed", zap.Error(err))
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	return s.Stop()
}

func (s *Server) Stop() error {
	if s.server != nil {
		s.logger.Info("Stopping HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*stdtime.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}
	return nil
}

// Request/Response structures for JSON

type CreateTransactionRequest struct {
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata,omitempty"`
	Timestamp      string                          `json:"timestamp,omitempty"` // ISO 8601 format
	Metadata       metadata.Metadata               `json:"metadata,omitempty"`
	Reference      string                          `json:"reference,omitempty"`
	Postings       []PostingRequest                `json:"postings"`
	DryRun         bool                            `json:"dryRun,omitempty"`
	IdempotencyKey string                          `json:"idempotencyKey,omitempty"`
}

type PostingRequest struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      string `json:"amount"` // big.Int as string
	Asset       string `json:"asset"`
}

type CreateTransactionResponse struct {
	Transaction     TransactionResponse `json:"transaction"`
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata,omitempty"`
}

type TransactionResponse struct {
	Postings  []PostingResponse    `json:"postings"`
	Metadata  metadata.Metadata    `json:"metadata,omitempty"`
	Timestamp string                `json:"timestamp"` // ISO 8601 format
	Reference string                `json:"reference,omitempty"`
	ID        *uint64               `json:"id,omitempty"`
}

type PostingResponse struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      string `json:"amount"` // big.Int as string
	Asset       string `json:"asset"`
}

func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	// Convert request to service parameters
	postings := make(ledger.Postings, 0, len(req.Postings))
	for _, p := range req.Postings {
		amount, ok := new(big.Int).SetString(p.Amount, 10)
		if !ok {
			http.Error(w, fmt.Sprintf("Invalid amount: %s", p.Amount), http.StatusBadRequest)
			return
		}
		postings = append(postings, ledger.NewPosting(p.Source, p.Destination, p.Asset, amount))
	}

	// Parse timestamp
	var timestamp time.Time
	if req.Timestamp != "" {
		parsed, err := stdtime.Parse(stdtime.RFC3339, req.Timestamp)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid timestamp format: %v", err), http.StatusBadRequest)
			return
		}
		timestamp = time.New(parsed)
	} else {
		timestamp = time.Now()
	}

	params := service.Parameters[service.CreateTransaction]{
		DryRun:         req.DryRun,
		IdempotencyKey: req.IdempotencyKey,
		Input: service.CreateTransaction{
			AccountMetadata: req.AccountMetadata,
			Timestamp:       timestamp,
			Metadata:        req.Metadata,
			Reference:       req.Reference,
			Postings:        postings,
		},
	}

	// Call ledger service
	_, createdTx, err := s.ledgerService.CreateTransaction(r.Context(), params)
	if err != nil {
		s.logger.Error("Failed to create transaction", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to create transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert response to JSON
	response := CreateTransactionResponse{
		Transaction:     transactionToResponse(createdTx.Transaction),
		AccountMetadata: createdTx.AccountMetadata,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Error("Failed to encode response", zap.Error(err))
	}
}

func transactionToResponse(tx ledger.Transaction) TransactionResponse {
	postings := make([]PostingResponse, 0, len(tx.Postings))
	for _, p := range tx.Postings {
		postings = append(postings, PostingResponse{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      p.Amount.String(),
			Asset:       p.Asset,
		})
	}

	var id *uint64
	if tx.ID != nil {
		id = tx.ID
	}

	timestamp := ""
	if !tx.Timestamp.IsZero() {
		timestamp = tx.Timestamp.Time.Format(stdtime.RFC3339)
	}

	return TransactionResponse{
		Postings:  postings,
		Metadata:  tx.Metadata,
		Timestamp: timestamp,
		Reference: tx.Reference,
		ID:        id,
	}
}

