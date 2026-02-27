//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	v2 "github.com/formancehq/ledger-v3-poc/internal/adapter/v2"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockV2Server simulates a v2 ledger API for mirror integration tests.
type mockV2Server struct {
	mu   sync.Mutex
	logs []v2.V2Log
	srv  *httptest.Server
}

func newMockV2Server() *mockV2Server {
	m := &mockV2Server{}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *mockV2Server) URL() string {
	return m.srv.URL
}

func (m *mockV2Server) Close() {
	m.srv.Close()
}

// addLog appends a v2 log to the mock server's data.
func (m *mockV2Server) addLog(log v2.V2Log) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, log)
}

func (m *mockV2Server) handler(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Parse "after" query param
	afterStr := r.URL.Query().Get("after")
	var afterID uint64
	if afterStr != "" {
		_, _ = fmt.Sscanf(afterStr, "%d", &afterID)
	}

	// Filter logs after the given ID (ascending order)
	var result []v2.V2Log
	for _, log := range m.logs {
		if log.ID > afterID {
			result = append(result, log)
		}
	}

	// Respect pageSize
	pageSizeStr := r.URL.Query().Get("pageSize")
	pageSize := 100
	if pageSizeStr != "" {
		_, _ = fmt.Sscanf(pageSizeStr, "%d", &pageSize)
	}

	hasMore := false
	if len(result) > pageSize {
		result = result[:pageSize]
		hasMore = true
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v2.V2LogPage{
		Cursor: v2.V2LogCursor{
			PageSize: pageSize,
			HasMore:  hasMore,
			Data:     result,
		},
	})
}

func newV2TransactionLog(id uint64, txID uint64, source, destination, amount, asset string) v2.V2Log {
	data, _ := json.Marshal(v2.V2NewTransactionData{
		Transaction: v2.V2Transaction{
			ID: txID,
			Postings: []v2.V2Posting{{
				Source:      source,
				Destination: destination,
				Amount:      amount,
				Asset:       asset,
			}},
			Timestamp: time.Now().Format(time.RFC3339Nano),
		},
	})
	return v2.V2Log{
		ID:   id,
		Type: "NEW_TRANSACTION",
		Date: time.Now().Format(time.RFC3339Nano),
		Data: data,
	}
}

func newV2SetMetadataLog(id uint64, targetType, targetID string, metadata map[string]any) v2.V2Log {
	rawTargetID, _ := json.Marshal(targetID)
	data, _ := json.Marshal(v2.V2SetMetadataData{
		TargetType: targetType,
		TargetID:   rawTargetID,
		Metadata:   metadata,
	})
	return v2.V2Log{
		ID:   id,
		Type: "SET_METADATA",
		Date: time.Now().Format(time.RFC3339Nano),
		Data: data,
	}
}

var _ = Describe("Mirror", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = testSingleHTTPPort
		grpcPort = testSingleGRPCPort
	)

	BeforeAll(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)
	})

	Context("When creating a mirror ledger", func() {
		It("Should create a mirror ledger with HTTP source", func() {
			mockV2 := newMockV2Server()
			defer mockV2.Close()

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_CreateLedger{
						CreateLedger: &servicepb.CreateLedgerRequest{
							Name: "mirror-http",
							Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
							MirrorSource: &commonpb.MirrorSourceConfig{
								LedgerName: "default",
								Type: &commonpb.MirrorSourceConfig_Http{
									Http: &commonpb.HttpMirrorSourceConfig{
										BaseUrl: mockV2.URL(),
									},
								},
							},
						},
					},
				}},
			})
			Expect(err).To(Succeed())

			// Verify the ledger exists and is in mirror mode
			ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: "mirror-http",
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal("mirror-http"))
			Expect(ledger.Mode).To(Equal(commonpb.LedgerMode_LEDGER_MODE_MIRROR))
		})

		It("Should create a mirror ledger with Postgres source config", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_CreateLedger{
						CreateLedger: &servicepb.CreateLedgerRequest{
							Name: "mirror-pg",
							Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
							MirrorSource: &commonpb.MirrorSourceConfig{
								LedgerName: "default",
								Type: &commonpb.MirrorSourceConfig_Postgres{
									Postgres: &commonpb.PostgresMirrorSourceConfig{
										Dsn: "postgres://user:pass@host:5432/ledger",
									},
								},
							},
						},
					},
				}},
			})
			Expect(err).To(Succeed())

			ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: "mirror-pg",
			})
			Expect(err).To(Succeed())
			Expect(ledger.Mode).To(Equal(commonpb.LedgerMode_LEDGER_MODE_MIRROR))
		})
	})

	Context("When writing to a mirror ledger (write guard)", Ordered, func() {
		BeforeAll(func() {
			mockV2 := newMockV2Server()
			DeferCleanup(mockV2.Close)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_CreateLedger{
						CreateLedger: &servicepb.CreateLedgerRequest{
							Name: "mirror-guard",
							Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
							MirrorSource: &commonpb.MirrorSourceConfig{
								LedgerName: "default",
								Type: &commonpb.MirrorSourceConfig_Http{
									Http: &commonpb.HttpMirrorSourceConfig{
										BaseUrl: mockV2.URL(),
									},
								},
							},
						},
					},
				}},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject creating transactions on mirror ledger", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction("mirror-guard", []*commonpb.Posting{
						newPosting("world", "users:001", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonLedgerInMirrorMode))
		})

		It("Should reject saving metadata on mirror ledger", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction("mirror-guard", "users:001", map[string]string{"key": "val"}),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
		})
	})

	Context("When syncing from a v2 source", Ordered, func() {
		var mockV2 *mockV2Server

		BeforeAll(func() {
			mockV2 = newMockV2Server()
			DeferCleanup(mockV2.Close)

			// Seed v2 logs before creating the mirror
			mockV2.addLog(newV2TransactionLog(1, 0, "world", "users:001", "100", "USD/2"))
			mockV2.addLog(newV2TransactionLog(2, 1, "world", "users:002", "200", "EUR/2"))
			mockV2.addLog(newV2SetMetadataLog(3, "ACCOUNT", "users:001", map[string]any{"role": "admin"}))

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_CreateLedger{
						CreateLedger: &servicepb.CreateLedgerRequest{
							Name: "mirror-sync",
							Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
							MirrorSource: &commonpb.MirrorSourceConfig{
								LedgerName: "default",
								Type: &commonpb.MirrorSourceConfig_Http{
									Http: &commonpb.HttpMirrorSourceConfig{
										BaseUrl: mockV2.URL(),
									},
								},
							},
						},
					},
				}},
			})
			Expect(err).To(Succeed())
		})

		It("Should sync transactions from v2", func() {
			// Wait for mirror worker to sync the v2 logs
			Eventually(func(g Gomega) {
				stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
					Ledger:   "mirror-sync",
					PageSize: 10,
				})
				g.Expect(err).To(Succeed())

				var txs []*commonpb.Transaction
				for {
					tx, err := stream.Recv()
					if err == io.EOF {
						break
					}
					g.Expect(err).To(Succeed())
					txs = append(txs, tx)
				}

				// We expect at least 2 transactions from the v2 logs
				g.Expect(len(txs)).To(BeNumerically(">=", 2))
			}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("Should sync account metadata from v2", func() {
			Eventually(func(g Gomega) {
				account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:  "mirror-sync",
					Address: "users:001",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account).NotTo(BeNil())

				roleVal := findMetadataValue(account.Metadata, "role")
				g.Expect(roleVal).NotTo(BeNil())
				g.Expect(roleVal.GetStringValue()).To(Equal("admin"))
			}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When promoting a mirror ledger", Ordered, func() {
		var mockV2 *mockV2Server

		BeforeAll(func() {
			mockV2 = newMockV2Server()
			DeferCleanup(mockV2.Close)

			mockV2.addLog(newV2TransactionLog(1, 0, "world", "users:001", "50", "USD/2"))

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_CreateLedger{
						CreateLedger: &servicepb.CreateLedgerRequest{
							Name: "mirror-promote",
							Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
							MirrorSource: &commonpb.MirrorSourceConfig{
								LedgerName: "default",
								Type: &commonpb.MirrorSourceConfig_Http{
									Http: &commonpb.HttpMirrorSourceConfig{
										BaseUrl: mockV2.URL(),
									},
								},
							},
						},
					},
				}},
			})
			Expect(err).To(Succeed())

			// Wait for sync to complete
			Eventually(func(g Gomega) {
				stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
					Ledger:   "mirror-promote",
					PageSize: 10,
				})
				g.Expect(err).To(Succeed())

				var txs []*commonpb.Transaction
				for {
					tx, err := stream.Recv()
					if err == io.EOF {
						break
					}
					g.Expect(err).To(Succeed())
					txs = append(txs, tx)
				}
				g.Expect(len(txs)).To(BeNumerically(">=", 1))
			}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("Should promote ledger from mirror to normal mode", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_PromoteLedger{
						PromoteLedger: &servicepb.PromoteLedgerRequest{
							Ledger: "mirror-promote",
						},
					},
				}},
			})
			Expect(err).To(Succeed())

			// Verify mode changed to NORMAL
			ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: "mirror-promote",
			})
			Expect(err).To(Succeed())
			Expect(ledger.Mode).To(Equal(commonpb.LedgerMode_LEDGER_MODE_NORMAL))
			Expect(ledger.MirrorSource).To(BeNil())
		})

		It("Should allow writing to the promoted ledger", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction("mirror-promote", []*commonpb.Posting{
						newPosting("world", "users:002", big.NewInt(200), "USD/2"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})
	})

	Context("When promoting a non-mirror ledger", func() {
		It("Should fail with LEDGER_NOT_IN_MIRROR_MODE", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction("normal-ledger-promote", nil)},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_PromoteLedger{
						PromoteLedger: &servicepb.PromoteLedgerRequest{
							Ledger: "normal-ledger-promote",
						},
					},
				}},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonLedgerNotInMirrorMode))
		})
	})
})
