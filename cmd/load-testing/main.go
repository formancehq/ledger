package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "load-testing",
	Short: "Load testing tool for Ledger v3 POC",
	Long:  "A load testing tool for testing the Ledger v3 POC system with different transaction types",
}

var (
	serverURL       string
	concurrency     int
	duration        time.Duration
	rate            int // transactions per second
	transactionType string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:9000", "Server URL")
	rootCmd.PersistentFlags().IntVar(&concurrency, "concurrency", 10, "Number of concurrent workers")
	rootCmd.PersistentFlags().DurationVar(&duration, "duration", 30*time.Second, "Test duration")
	rootCmd.PersistentFlags().IntVar(&rate, "rate", 100, "Target transactions per second")
	rootCmd.PersistentFlags().StringVar(&transactionType, "type", "simple", "Transaction type: simple, multi-asset, multi-account, or mixed")

	rootCmd.RunE = runLoadTest
}

func runLoadTest(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	logger.Info("Starting load test",
		zap.String("server", serverURL),
		zap.Int("concurrency", concurrency),
		zap.Duration("duration", duration),
		zap.Int("rate", rate),
		zap.String("type", transactionType),
	)

	// Create SDK instance
	sdk := client.New(
		client.WithServerURL(serverURL),
	)

	// Statistics
	var (
		successCount int64
		errorCount   int64
		totalLatency int64 // in nanoseconds
	)

	// Create context with timeout
	testCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	// Calculate interval between requests to achieve target rate
	interval := time.Second / time.Duration(rate/concurrency)

	// Start workers
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-testCtx.Done():
					return
				case <-ticker.C:
					// Generate transaction based on type
					req := generateTransaction(transactionType, workerID)

					// Execute transaction
					reqStart := time.Now()
					_, err := sdk.Transactions.CreateTransaction(testCtx, req)
					latency := time.Since(reqStart)

					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						logger.Debug("Transaction failed",
							zap.Int("worker", workerID),
							zap.Error(err),
						)
					} else {
						atomic.AddInt64(&successCount, 1)
						atomic.AddInt64(&totalLatency, latency.Nanoseconds())
					}
				}
			}
		}(i)
	}

	// Wait for all workers to finish
	wg.Wait()

	elapsed := time.Since(startTime)

	// Print statistics
	avgLatency := time.Duration(0)
	if successCount > 0 {
		avgLatency = time.Duration(totalLatency / successCount)
	}

	actualRate := float64(successCount) / elapsed.Seconds()

	fmt.Println("\n=== Load Test Results ===")
	fmt.Printf("Duration: %v\n", elapsed)
	fmt.Printf("Successful transactions: %d\n", successCount)
	fmt.Printf("Failed transactions: %d\n", errorCount)
	fmt.Printf("Total transactions: %d\n", successCount+errorCount)
	fmt.Printf("Success rate: %.2f%%\n", float64(successCount)/float64(successCount+errorCount)*100)
	fmt.Printf("Average latency: %v\n", avgLatency)
	fmt.Printf("Actual rate: %.2f tx/s\n", actualRate)
	fmt.Printf("Target rate: %d tx/s\n", rate)

	return nil
}

func generateTransaction(txType string, workerID int) components.CreateTransactionRequest {
	baseAccount := fmt.Sprintf("account-%d", workerID%10) // 10 different accounts
	timestamp := time.Now()

	switch txType {
	case "simple":
		return generateSimpleTransaction(baseAccount, timestamp)
	case "multi-asset":
		return generateMultiAssetTransaction(baseAccount, timestamp)
	case "multi-account":
		return generateMultiAccountTransaction(timestamp)
	case "mixed":
		// Randomly choose between different types
		switch (workerID + int(time.Now().Unix())) % 3 {
		case 0:
			return generateSimpleTransaction(baseAccount, timestamp)
		case 1:
			return generateMultiAssetTransaction(baseAccount, timestamp)
		case 2:
			return generateMultiAccountTransaction(timestamp)
		}
	}

	// Default to simple
	return generateSimpleTransaction(baseAccount, timestamp)
}

func generateSimpleTransaction(account string, timestamp time.Time) components.CreateTransactionRequest {
	amount := big.NewInt(int64(100 + (time.Now().UnixNano() % 1000))) // Random amount between 100 and 1100
	ref := uuid.New().String()

	return components.CreateTransactionRequest{
		Postings: []components.PostingRequest{
			{
				Source:      "world",
				Destination: account,
				Asset:       "USD",
				Amount:      amount,
			},
		},
		Timestamp: &timestamp,
		Reference: &ref,
	}
}

func generateMultiAssetTransaction(account string, timestamp time.Time) components.CreateTransactionRequest {
	usdAmount := big.NewInt(int64(100 + (time.Now().UnixNano() % 1000)))
	eurAmount := big.NewInt(int64(50 + (time.Now().UnixNano() % 500)))
	ref := uuid.New().String()

	return components.CreateTransactionRequest{
		Postings: []components.PostingRequest{
			{
				Source:      "world",
				Destination: account,
				Asset:       "USD",
				Amount:      usdAmount,
			},
			{
				Source:      "world",
				Destination: account,
				Asset:       "EUR",
				Amount:      eurAmount,
			},
		},
		Timestamp: &timestamp,
		Reference: &ref,
	}
}

func generateMultiAccountTransaction(timestamp time.Time) components.CreateTransactionRequest {
	fromAccount := fmt.Sprintf("account-%d", time.Now().UnixNano()%10)
	toAccount := fmt.Sprintf("account-%d", (time.Now().UnixNano()+1)%10)
	amount := big.NewInt(int64(10 + (time.Now().UnixNano() % 100)))
	ref := uuid.New().String()

	return components.CreateTransactionRequest{
		Postings: []components.PostingRequest{
			{
				Source:      fromAccount,
				Destination: toAccount,
				Asset:       "USD",
				Amount:      amount,
			},
		},
		Timestamp: &timestamp,
		Reference: &ref,
	}
}
