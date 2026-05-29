//go:build e2e

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
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
				Amount:      json.Number(amount),
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
		httpPort = testutil.TestSingleHTTPPort
		grpcPort = testutil.TestSingleGRPCPort
	)

	BeforeAll(func() {
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)
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
					actions.CreateTransactionAction("mirror-guard", []*commonpb.Posting{
						actions.NewPosting("world", "users:001", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonLedgerInMirrorMode))
		})

		It("Should reject saving metadata on mirror ledger", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction("mirror-guard", "users:001", map[string]string{"key": "val"}),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
		})

		It("Should reject deleting metadata on mirror ledger", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.DeleteAccountMetadataAction("mirror-guard", "users:001", "key"),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
		})

		It("Should allow setting metadata field type on mirror ledger", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SetMetadataFieldTypeAction("mirror-guard", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "category", commonpb.MetadataType_METADATA_TYPE_STRING),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow removing metadata field type on mirror ledger", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RemoveMetadataFieldTypeAction("mirror-guard", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "category"),
				},
			})
			Expect(err).To(Succeed())
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
				txs, err := listAllTransactions(ctx, client, "mirror-sync", 10, 0)
				g.Expect(err).To(Succeed())

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

				roleVal := actions.FindMetadataValue(account.Metadata, "role")
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
				txs, err := listAllTransactions(ctx, client, "mirror-promote", 10, 0)
				g.Expect(err).To(Succeed())
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
					actions.CreateTransactionAction("mirror-promote", []*commonpb.Posting{
						actions.NewPosting("world", "users:002", big.NewInt(200), "USD/2"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})
	})

	Context("When promoting a non-mirror ledger", func() {
		It("Should fail with LEDGER_NOT_IN_MIRROR_MODE", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction("normal-ledger-promote", nil)},
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

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonLedgerNotInMirrorMode))
		})
	})

	Context("When syncing from a v2 source with OAuth2 client credentials", Ordered, func() {
		var (
			mockV2    *authMockV2Server
			mockOAuth *mockOAuth2TokenServer
		)

		BeforeAll(func() {
			mockOAuth = newMockOAuth2TokenServer("test-client-id", "test-client-secret")
			DeferCleanup(mockOAuth.Close)

			mockV2 = newAuthMockV2Server(mockOAuth.accessToken)
			DeferCleanup(mockV2.Close)

			// Seed v2 logs before creating the mirror
			mockV2.addLog(newV2TransactionLog(1, 0, "world", "oauth2:user1", "500", "USD/2"))
			mockV2.addLog(newV2SetMetadataLog(2, "ACCOUNT", "oauth2:user1", map[string]any{"provider": "oauth2"}))

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_CreateLedger{
						CreateLedger: &servicepb.CreateLedgerRequest{
							Name: "mirror-oauth2",
							Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
							MirrorSource: &commonpb.MirrorSourceConfig{
								LedgerName: "default",
								Type: &commonpb.MirrorSourceConfig_Http{
									Http: &commonpb.HttpMirrorSourceConfig{
										BaseUrl: mockV2.URL(),
										Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{
											ClientId:      "test-client-id",
											ClientSecret:  "test-client-secret",
											TokenEndpoint: mockOAuth.TokenEndpoint(),
										},
									},
								},
							},
						},
					},
				}},
			})
			Expect(err).To(Succeed())
		})

		It("Should obtain an OAuth2 token and sync transactions", func() {
			Eventually(func(g Gomega) {
				txs, err := listAllTransactions(ctx, client, "mirror-oauth2", 10, 0)
				g.Expect(err).To(Succeed())

				g.Expect(len(txs)).To(BeNumerically(">=", 1))
			}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("Should have used the OAuth2 token for v2 API calls", func() {
			Expect(mockV2.authenticatedRequests()).To(BeNumerically(">", 0))
			Expect(mockV2.unauthenticatedRequests()).To(Equal(int64(0)))
		})

		It("Should have requested a token from the OAuth2 server", func() {
			Expect(mockOAuth.tokenRequestCount()).To(BeNumerically(">", 0))
		})

		It("Should sync account metadata via OAuth2-authenticated requests", func() {
			Eventually(func(g Gomega) {
				account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:  "mirror-oauth2",
					Address: "oauth2:user1",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account).NotTo(BeNil())

				providerVal := actions.FindMetadataValue(account.Metadata, "provider")
				g.Expect(providerVal).NotTo(BeNil())
				g.Expect(providerVal.GetStringValue()).To(Equal("oauth2"))
			}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})
	})
})

// mockOAuth2TokenServer is a minimal OAuth2 token endpoint that supports
// the client_credentials grant type for testing.
type mockOAuth2TokenServer struct {
	clientID     string
	clientSecret string
	accessToken  string
	tokenReqs    atomic.Int64
	srv          *httptest.Server
}

func newMockOAuth2TokenServer(clientID, clientSecret string) *mockOAuth2TokenServer {
	m := &mockOAuth2TokenServer{
		clientID:     clientID,
		clientSecret: clientSecret,
		accessToken:  "mock-access-token-e2e",
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/token", m.handleToken)
	m.srv = httptest.NewServer(mux)
	return m
}

func (m *mockOAuth2TokenServer) TokenEndpoint() string {
	return m.srv.URL + "/token"
}

func (m *mockOAuth2TokenServer) Close() {
	m.srv.Close()
}

func (m *mockOAuth2TokenServer) tokenRequestCount() int64 {
	return m.tokenReqs.Load()
}

func (m *mockOAuth2TokenServer) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	_ = r.ParseForm()
	if r.FormValue("grant_type") != "client_credentials" {
		http.Error(w, `{"error":"unsupported_grant_type"}`, http.StatusBadRequest)
		return
	}

	// golang.org/x/oauth2/clientcredentials sends credentials via Basic auth
	clientID, clientSecret, ok := r.BasicAuth()
	if !ok {
		clientID = r.FormValue("client_id")
		clientSecret = r.FormValue("client_secret")
	}

	if clientID != m.clientID || clientSecret != m.clientSecret {
		http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
		return
	}

	m.tokenReqs.Add(1)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": m.accessToken,
		"token_type":   "Bearer",
		"expires_in":   3600,
	})
}

// authMockV2Server is a mock v2 server that requires Bearer token authentication.
type authMockV2Server struct {
	mu            sync.Mutex
	logs          []v2.V2Log
	expectedToken string
	authReqs      atomic.Int64
	unauthReqs    atomic.Int64
	srv           *httptest.Server
}

func newAuthMockV2Server(expectedToken string) *authMockV2Server {
	m := &authMockV2Server{
		expectedToken: expectedToken,
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *authMockV2Server) URL() string {
	return m.srv.URL
}

func (m *authMockV2Server) Close() {
	m.srv.Close()
}

func (m *authMockV2Server) addLog(log v2.V2Log) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, log)
}

func (m *authMockV2Server) authenticatedRequests() int64 {
	return m.authReqs.Load()
}

func (m *authMockV2Server) unauthenticatedRequests() int64 {
	return m.unauthReqs.Load()
}

func (m *authMockV2Server) handler(w http.ResponseWriter, r *http.Request) {
	// Check Bearer token
	auth := r.Header.Get("Authorization")
	if auth != "Bearer "+m.expectedToken {
		m.unauthReqs.Add(1)
		http.Error(w, `{"error": "unauthorized"}`, http.StatusUnauthorized)
		return
	}
	m.authReqs.Add(1)

	m.mu.Lock()
	defer m.mu.Unlock()

	afterStr := r.URL.Query().Get("after")
	var afterID uint64
	if afterStr != "" {
		_, _ = fmt.Sscanf(afterStr, "%d", &afterID)
	}

	var result []v2.V2Log
	for _, log := range m.logs {
		if log.ID > afterID {
			result = append(result, log)
		}
	}

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
