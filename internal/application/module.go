package application

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"google.golang.org/grpc/credentials"
	httpcompat "github.com/formancehq/ledger-v3-poc/internal/compat/http"
	"github.com/formancehq/ledger-v3-poc/internal/crypto/keystore"
	clusterhealth "github.com/formancehq/ledger-v3-poc/internal/health"
	"github.com/formancehq/ledger-v3-poc/internal/monitoring/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/admission"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/service/coldstorage"
	"github.com/formancehq/ledger-v3-poc/internal/service/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/service/events"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/service/receipt"
	"github.com/formancehq/ledger-v3-poc/internal/service/state"
	"github.com/formancehq/ledger-v3-poc/internal/service/transport"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/formancehq/ledger-v3-poc/internal/storage/diskusage"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// walFreshStart indicates whether the WAL was empty before the node was created.
// It is used to decide whether a joining node needs to register as a learner.
type walFreshStart bool

// nodeProvideResult groups the outputs of the Node provider so we can also
// expose the WAL-fresh-start indicator through the fx dependency graph.
type nodeProvideResult struct {
	fx.Out
	Node       *node.Node
	FreshStart walFreshStart
}

func Module() fx.Option {
	return fx.Options(
		transport.Module(),
		attributes.Module(),
		fx.Provide(
			func(
				cfg Config,
				logger logging.Logger,
				connectionPool *transport.ConnectionPool,
				meterProvider metric.MeterProvider,
			) *node.DefaultTransport {
				return node.NewTransport(
					logger,
					connectionPool,
					meterProvider,
					cfg.RaftConfig.NodeID,
					cfg.TransportConfig,
					cfg.ClusterID,
				)
			},
			func(cfg Config, meterProvider metric.MeterProvider, logger logging.Logger) (*data.Store, error) {
				return data.NewStore(
					cfg.DataDir,
					logger,
					meterProvider.Meter("pebble.runtime_store"),
					cfg.PebbleConfig,
				)
			},
			func(cfg Config, logger logging.Logger, meterProvider metric.MeterProvider) (*wal.DefaultWAL, error) {
				return wal.New(cfg.RaftConfig.WalDir, logger.WithFields(map[string]any{
					"cmp": "wal",
				}), meterProvider.Meter("wal"))
			},
			func(cfg Config) (*spool.Default, error) {
				return spool.NewDefault(spool.DefaultSpoolConfig{
					Dir: filepath.Join(cfg.RaftConfig.WalDir, "spool"),
				})
			},
			func(transport *node.DefaultTransport) state.SnapshotFetcherProvider {
				return ctrl.GRPCSnapshotFetcherProvider(transport)
			},
			// Provide events.Proposer from the Raft node (used by event emitter to replicate cursor)
			func(n *node.Node) events.Proposer {
				return n
			},
			func(cfg node.NodeConfig, meterProvider metric.MeterProvider) (*cache.Cache, error) {
				return cache.New(cfg.RotationThreshold, meterProvider.Meter("cache"))
			},
			func(
				cfg Config,
				logger logging.Logger,
				store *data.Store,
				meterProvider metric.MeterProvider,
				c *cache.Cache,
				attrs *attributes.Attributes,
				ks *keystore.KeyStore,
				notifications *events.Notifications,
			) (*state.Machine, error) {
				return state.NewMachine(
					logger,
					store,
					meterProvider.Meter("raft.node"),
					c,
					attrs,
					cfg.RaftConfig.RotationThreshold,
					ks,
					cfg.AuditEnabled,
					notifications,
				)
			},
			func(
				params struct {
					fx.In
					NodeConfig              node.NodeConfig
					Logger                  logging.Logger
					Transport               *node.DefaultTransport
					MeterProvider           metric.MeterProvider
					Store                   *data.Store
					WAL                     *wal.DefaultWAL
					Spool                   *spool.Default
					SnapshotFetcherProvider state.SnapshotFetcherProvider
					Machine                 *state.Machine
				},
			) (nodeProvideResult, error) {
				// Check WAL emptiness before NewNode writes the initial snapshot.
				snapshot, err := params.WAL.Snapshot()
				if err != nil {
					return nodeProvideResult{}, fmt.Errorf("reading WAL snapshot: %w", err)
				}
				freshStart := walFreshStart(len(snapshot.Metadata.ConfState.Voters) == 0)

				n, err := node.NewNode(
					params.NodeConfig,
					params.Transport,
					params.Store,
					params.Logger,
					params.MeterProvider.Meter("raft.node"),
					params.Spool,
					params.WAL,
					params.SnapshotFetcherProvider,
					params.Machine,
				)
				if err != nil {
					return nodeProvideResult{}, err
				}
				return nodeProvideResult{Node: n, FreshStart: freshStart}, nil
			},
			func(cfg Config) *receipt.Signer {
				if cfg.ReceiptSigningKey == "" {
					return nil
				}
				return receipt.NewSigner([]byte(cfg.ReceiptSigningKey))
			},
			func(cfg Config) node.NodeConfig {
				cfg.RaftConfig.SetDefaults()
				return cfg.RaftConfig
			},
			func(cfg Config) node.TransportConfig {
				return cfg.TransportConfig
			},
			func(cfg Config) (credentials.TransportCredentials, error) {
				return ClientTransportCredentials(cfg.TLSConfig)
			},
			// RaftServer for internal inter-node communication (Raft transport + Snapshot)
			func(cfg Config, logger logging.Logger) (*RaftServer, error) {
				_, raftPort, err := net.SplitHostPort(cfg.RaftConfig.BindAddr)
				if err != nil {
					return nil, fmt.Errorf("invalid bind address format: %w", err)
				}
				port, err := strconv.Atoi(raftPort)
				if err != nil {
					return nil, fmt.Errorf("invalid port in bind address: %w", err)
				}

				tlsOpt, err := ServerCredentials(cfg.TLSConfig)
				if err != nil {
					return nil, fmt.Errorf("loading TLS credentials for raft server: %w", err)
				}

				return NewRaftServer(port, logger, tlsOpt), nil
			},
			// ServiceServer for external client-facing API
			func(cfg Config, logger logging.Logger) (*ServiceServer, error) {
				tlsOpt, err := ServerCredentials(cfg.TLSConfig)
				if err != nil {
					return nil, fmt.Errorf("loading TLS credentials for service server: %w", err)
				}

				return NewServiceServer(cfg.GRPCPort, logger, cfg.Debug, tlsOpt), nil
			},
			func(cfg Config, logger logging.Logger, ctrl ctrl.Controller, s *data.Store, attrs *attributes.Attributes, signer *receipt.Signer) servicepb.BucketServiceServer {
				return NewBucketServiceServer(logger, ctrl, s, attrs, cfg.AuditEnabled, signer)
			},
			func(logger logging.Logger, s *data.Store) snapshotpb.SnapshotServiceServer {
				return NewSnapshotServiceServer(logger, s)
			},
			func(cfg Config) *diskusage.Collector {
				return diskusage.NewCollector(
					cfg.RaftConfig.WalDir,
					cfg.DataDir,
					10*time.Second,
				)
			},
			func(n *node.Node, raftTransport *node.DefaultTransport, servicePool *transport.ServiceConnectionPool, collector *diskusage.Collector, store *data.Store, logger logging.Logger, cfg Config) clusterpb.ClusterServiceServer {
				return NewClusterServiceServer(n, raftTransport, servicePool, collector, store, logger,
					cfg.RaftConfig.AdvertiseAddr,
					cfg.ServiceAdvertiseAddr(),
				)
			},
			func(n *node.Node, collector *diskusage.Collector, servicePool *transport.ServiceConnectionPool, cfg Config, logger logging.Logger) *clusterhealth.HealthChecker {
				return clusterhealth.NewHealthChecker(
					n, collector, servicePool,
					logger,
					cfg.HealthConfig.Interval,
					cfg.HealthConfig.WALThreshold,
					cfg.HealthConfig.DataThreshold,
					cfg.HealthConfig.ClockSkewThreshold,
				)
			},
			func() *keystore.KeyStore {
				return keystore.NewKeyStore()
			},
			events.NewNotifications,
			events.NewManager,
			httpcompat.NewServer,
			httpcompat.NewHandler,
			func(node *node.Node, ctrl ctrl.Controller) httpcompat.Backend {
				return httpcompat.NewDefaultBackend(node, ctrl)
			},
			func(
				cfg Config,
				node *node.Node,
				cache *cache.Cache,
				store *data.Store,
				logger logging.Logger,
				attrs *attributes.Attributes,
				meterProvider metric.MeterProvider,
				hc *clusterhealth.HealthChecker,
				ks *keystore.KeyStore,
				receiptSigner *receipt.Signer,
			) ctrl.Admission {
				var opts []func(*admission.Admission)
				if cfg.AdmissionMetrics {
					opts = append(opts, admission.WithMetrics())
				}
				if receiptSigner != nil {
					opts = append(opts, admission.WithReceiptSigner(receiptSigner))
				}
				return admission.NewAdmission(
					cache,
					store,
					logger,
					node,
					attrs,
					meterProvider,
					hc,
					ks,
					opts...,
				)
			},
			func(
				logger logging.Logger,
				store *data.Store,
				machine *state.Machine,
				admissionHandler ctrl.Admission,
				raftNode *node.Node,
			) *state.Sealer {
				return state.NewSealer(logger, store, machine.SealRequestCh(), func(periodID uint64, sealingHash []byte) {
					_, _ = admissionHandler.Admit(context.Background(), &servicepb.Request{
						Type: &servicepb.Request_SealPeriod{
							SealPeriod: &servicepb.SealPeriodRequest{
								PeriodId:    periodID,
								SealingHash: sealingHash,
							},
						},
					})
				}, raftNode.IsLeader, func(periodID uint64) bool {
					cp := machine.ClosingPeriod()
					return cp != nil && cp.Id == periodID
				})
			},
			func(cfg Config, logger logging.Logger) (coldstorage.ColdStorage, error) {
				switch cfg.ColdStorageConfig.Driver {
				case "s3":
					if cfg.ColdStorageConfig.S3Bucket == "" {
						return nil, fmt.Errorf("--cold-storage-s3-bucket is required when driver=s3")
					}
					s3Client, err := coldstorage.NewS3Client(
						cfg.ColdStorageConfig.S3Region,
						cfg.ColdStorageConfig.S3Endpoint,
					)
					if err != nil {
						return nil, fmt.Errorf("creating S3 client: %w", err)
					}
					logger.WithFields(map[string]any{
						"bucket":   cfg.ColdStorageConfig.S3Bucket,
						"region":   cfg.ColdStorageConfig.S3Region,
						"endpoint": cfg.ColdStorageConfig.S3Endpoint,
					}).Infof("Using S3 cold storage")
					return coldstorage.NewS3Storage(s3Client, cfg.ColdStorageConfig.S3Bucket), nil
				case "filesystem", "":
					basePath := cfg.ColdStorageConfig.BasePath
					if basePath == "" {
						basePath = filepath.Join(cfg.DataDir, "cold-storage")
					}
					return coldstorage.NewFilesystemStorage(basePath), nil
				default:
					return nil, fmt.Errorf("unknown cold storage driver: %s", cfg.ColdStorageConfig.Driver)
				}
			},
			func(
				cfg Config,
				logger logging.Logger,
				store *data.Store,
				cold coldstorage.ColdStorage,
				machine *state.Machine,
				admissionHandler ctrl.Admission,
				raftNode *node.Node,
			) *state.Archiver {
				bucketID := cfg.ColdStorageConfig.BucketID
				if bucketID == "" {
					bucketID = cfg.ClusterID
				}
				if bucketID == "" {
					bucketID = store.DataDir()
				}
				return state.NewArchiver(
					logger,
					store,
					cold,
					machine.ArchiveRequestCh(),
					func(periodID uint64) {
						_, _ = admissionHandler.Admit(context.Background(), &servicepb.Request{
							Type: &servicepb.Request_ConfirmArchivePeriod{
								ConfirmArchivePeriod: &servicepb.ConfirmArchivePeriodRequest{
									PeriodId: periodID,
								},
							},
						})
					},
					raftNode.IsLeader,
					bucketID,
				)
			},
			func(
				raftNode *node.Node,
				servicePool *transport.ServiceConnectionPool,
				admission ctrl.Admission,
				store *data.Store,
				logger logging.Logger,
				attrs *attributes.Attributes,
				) ctrl.Controller {
				return NewRoutedController(
					ctrl.NewDefaultController(admission, store, logger, attrs),
					raftNode,
					servicePool,
				)
			},
		),
		fx.Decorate(func(
			params struct {
			fx.In
			Handler       http.Handler
			MeterProvider *sdkmetric.MeterProvider      `optional:"true"`
			Exporter      *otlpmetrics.InMemoryExporter `optional:"true"`
		},
		) http.Handler {
			// If InMemoryExporter is available, wrap handler to add metrics endpoint
			if params.Exporter != nil && params.MeterProvider != nil {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/metrics" {
						otlpmetrics.NewInMemoryExporterHandler(params.MeterProvider, params.Exporter)(w, r)
						return
					}
					params.Handler.ServeHTTP(w, r)
				})
			}
			return params.Handler
		}),
		fx.Invoke(
			func(
				lc fx.Lifecycle,
				runtime *data.Store,
				wal *wal.DefaultWAL,
				logger logging.Logger,
			) {
				lc.Append(fx.Hook{
					OnStop: func(ctx context.Context) error {
						return wal.Close()
					},
				})
				lc.Append(fx.Hook{
					OnStop: func(_ context.Context) error {
						return runtime.Close()
					},
				})
			},
			func(
				lc fx.Lifecycle,
				t *node.DefaultTransport,
				logger logging.Logger,
			) {
				lc.Append(fx.Hook{
					OnStop: func(ctx context.Context) error {
						logger.Infof("Stopping raft transport")
						return t.Stop(ctx)
					},
					OnStart: func(ctx context.Context) error {
						otlplogs.Go(func() {
							t.Start(context.WithoutCancel(ctx))
						}, logger)
						return nil
					},
				})
			},
			// Register Raft transport and Snapshot services on RaftServer (internal)
			func(raftServer *RaftServer, transport *node.DefaultTransport) error {
				node.RegisterRaftTransportService(raftServer.GetServer(), transport)
				return nil
			},
			func(raftServer *RaftServer, snapshotServiceServer snapshotpb.SnapshotServiceServer) error {
				RegisterSnapshotService(raftServer.GetServer(), snapshotServiceServer)
				return nil
			},
			// Register business services on ServiceServer (external)
			func(raftServer *RaftServer) error {
				hs := health.NewServer()
				healthpb.RegisterHealthServer(raftServer.GetServer(), hs)
				hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
				return nil
			},
			func(serviceServer *ServiceServer) error {
				hs := health.NewServer()
				healthpb.RegisterHealthServer(serviceServer.GetServer(), hs)
				hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
				return nil
			},
			func(serviceServer *ServiceServer, bucketServiceServer servicepb.BucketServiceServer) error {
				RegisterBucketService(serviceServer.GetServer(), bucketServiceServer)
				return nil
			},
			func(serviceServer *ServiceServer, clusterServiceServer clusterpb.ClusterServiceServer) error {
				RegisterClusterService(serviceServer.GetServer(), clusterServiceServer)
				return nil
			},
			// Start Raft server (internal) - must start before adding peers
			func(
				lc fx.Lifecycle,
				raftServer *RaftServer,
				logger logging.Logger,
				defaultTransport *node.DefaultTransport,
				servicePool *transport.ServiceConnectionPool,
				cfg node.NodeConfig,
			) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						logger.Infof("Starting Raft gRPC server")
						listening := make(chan struct{})
						reflection.Register(raftServer.GetServer())
						otlplogs.Go(func() {
							if err := raftServer.Start(listening); err != nil {
								panic(err)
							}
						}, logger)

						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-listening:
						}

						logger.Infof("Raft gRPC server started successfully")
						for _, peerEntry := range cfg.Peers {
							logger := logger.WithFields(map[string]any{"peer": peerEntry})
							logger.Infof("Adding peer to transport and service pool")
							defaultTransport.AddPeer(peerEntry.ID, peerEntry.Address)
							if err := servicePool.AddPeer(peerEntry.ID, peerEntry.ServiceAddress); err != nil {
								logger.WithFields(map[string]any{"error": err}).Errorf("Failed to add peer to service pool")
							}
						}

						return nil
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Stopping Raft gRPC server")
						return raftServer.Stop()
					},
				})
			},
			// Start Service server (external)
			func(
				lc fx.Lifecycle,
				serviceServer *ServiceServer,
				logger logging.Logger,
			) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						logger.Infof("Starting Service gRPC server")
						listening := make(chan struct{})
						otlplogs.Go(func() {
							if err := serviceServer.Start(listening); err != nil {
								panic(err)
							}
						}, logger)

						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-listening:
						}

						logger.Infof("Service gRPC server started successfully")
						return nil
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Stopping Service gRPC server")
						return serviceServer.Stop()
					},
				})
			},
			// Wire Observer: handle ConfChange and LeadershipChange events
			func(
				n *node.Node,
				defaultTransport *node.DefaultTransport,
				servicePool *transport.ServiceConnectionPool,
				logger logging.Logger,
				manager *events.Manager,
			) {
				n.SetObserver(node.NewObserver(func(event any) {
					switch e := event.(type) {
					case node.ConfChangeEvent:
						handleConfChangeEvent(e, defaultTransport, servicePool, logger)
					case node.LeadershipChangeEvent:
						handleLeadershipChangeEvent(e, manager, logger)
					default:
						logger.Errorf("Unknown observer event type: %T", event)
					}
				}))
			},
			func(lc fx.Lifecycle, node *node.Node, logger logging.Logger) (*node.Node, error) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						ready := make(chan struct{})
						otlplogs.Go(func() {
							if err := node.Run(context.WithoutCancel(ctx), ready); err != nil {
								panic(err)
							}
						}, logger)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-ready:
							logger.Infof("Raft cluster started successfully")
							return nil
						}
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Shutting down raft cluster")
						if err := node.Stop(ctx); err != nil {
							return fmt.Errorf("shutting down raft cluster: %w", err)
						}
						logger.Infof("Raft cluster stopped successfully")
						return nil
					},
				})

				return node, nil
			},
			// Join mode: auto-register as learner on the leader after raft starts.
			// Any peer will forward the request to the current leader automatically.
			func(
				lc fx.Lifecycle,
				cfg Config,
				freshStart walFreshStart,
				servicePool *transport.ServiceConnectionPool,
				logger logging.Logger,
			) {
				if cfg.RaftConfig.Bootstrap || len(cfg.RaftConfig.Peers) == 0 {
					return
				}
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						// Only register as learner on the very first start;
						// on restart the node is already a cluster member.
						if !freshStart {
							logger.Infof("Restart detected, skipping learner registration")
							return nil
						}

						peer := cfg.RaftConfig.Peers[0]
						conn := servicePool.GetConnection(peer.ID)
						if conn == nil {
							return fmt.Errorf("failed to register as learner: peer %d is not reachable", peer.ID)
						}

						logger.Infof("Join mode: requesting a peer to add this node as learner")
						client := clusterpb.NewClusterServiceClient(conn)
						_, err := client.AddLearner(ctx, &clusterpb.AddLearnerRequest{
							NodeId:         cfg.RaftConfig.NodeID,
							RaftAddress:    cfg.RaftConfig.AdvertiseAddr,
							ServiceAddress: cfg.ServiceAdvertiseAddr(),
						})
						if err != nil {
							return fmt.Errorf("failed to register as learner via peer %d: %w", peer.ID, err)
						}

						logger.WithFields(map[string]any{
							"peer": peer.ID,
						}).Infof("Successfully registered as learner")
						return nil
					},
				})
			},
			func(lc fx.Lifecycle, cfg Config, handler http.Handler) {
				lc.Append(httpserver.NewHook(handler,
					httpserver.WithAddress(fmt.Sprintf(":%d", cfg.HTTPPort)),
				))
			},
			func(lc fx.Lifecycle, collector *diskusage.Collector, meterProvider metric.MeterProvider) error {
				registration, err := collector.RegisterMetrics(meterProvider.Meter("storage"))
				if err != nil {
					return fmt.Errorf("registering disk usage metrics: %w", err)
				}
				lc.Append(fx.Hook{
					OnStart: func(_ context.Context) error {
						collector.Start()
						return nil
					},
					OnStop: func(_ context.Context) error {
						collector.Stop()
						return registration.Unregister()
					},
				})
				return nil
			},
			func(lc fx.Lifecycle, hc *clusterhealth.HealthChecker) {
				lc.Append(fx.Hook{
					OnStart: func(_ context.Context) error {
						hc.Start()
						return nil
					},
					OnStop: func(_ context.Context) error {
						hc.Stop()
						return nil
					},
				})
			},
			// Start and stop the event Manager
			func(lc fx.Lifecycle, manager *events.Manager) {
				lc.Append(fx.Hook{
					OnStart: func(_ context.Context) error {
						manager.Start()
						return nil
					},
					OnStop: func(_ context.Context) error {
						manager.Stop()
						return nil
					},
				})
			},
			func(lc fx.Lifecycle, sealer *state.Sealer, machine *state.Machine) {
				lc.Append(fx.Hook{
					OnStart: func(_ context.Context) error {
						sealer.Start(machine.ClosingPeriod())
						return nil
					},
					OnStop: func(_ context.Context) error {
						sealer.Stop()
						return nil
					},
				})
			},
			func(lc fx.Lifecycle, archiver *state.Archiver) {
				lc.Append(fx.Hook{
					OnStart: func(_ context.Context) error {
						archiver.Start()
						return nil
					},
					OnStop: func(_ context.Context) error {
						archiver.Stop()
						return nil
					},
				})
			},
		),
	)
}

// handleConfChangeEvent processes a single ConfChangeEvent by updating the
// transport and service pool when a node joins the cluster.
func handleConfChangeEvent(
	e node.ConfChangeEvent,
	defaultTransport *node.DefaultTransport,
	servicePool *transport.ServiceConnectionPool,
	logger logging.Logger,
) {
	switch e.ChangeType {
	case raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeAddNode:
		if len(e.Context) == 0 {
			return
		}
		ccCtx, err := node.UnmarshalConfChangeContext(e.Context)
		if err != nil {
			logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal ConfChange context")
			return
		}
		logger.WithFields(map[string]any{
			"node_id":         e.NodeID,
			"raft_address":    ccCtx.RaftAddress,
			"service_address": ccCtx.ServiceAddress,
		}).Infof("Adding peer from ConfChange")
		defaultTransport.AddPeer(e.NodeID, ccCtx.RaftAddress)
		if err := servicePool.AddPeer(e.NodeID, ccCtx.ServiceAddress); err != nil {
			logger.WithFields(map[string]any{"error": err}).Errorf("Failed to add peer to service pool from ConfChange")
		}
	}
}

// handleLeadershipChangeEvent notifies the event Manager of leadership changes.
func handleLeadershipChangeEvent(
	e node.LeadershipChangeEvent,
	manager *events.Manager,
	logger logging.Logger,
) {
	if e.IsLeader {
		logger.Infof("Became leader — reconciling event emitter")
	} else {
		logger.Infof("Lost leadership — tearing down event emitter")
	}
	manager.OnLeadershipChange(e.IsLeader)
}
