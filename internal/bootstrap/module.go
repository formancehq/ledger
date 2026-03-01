package bootstrap

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
	"github.com/formancehq/go-libs/v3/oidc"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	httpcompat "github.com/formancehq/ledger-v3-poc/internal/adapter/http"
	grpcadp "github.com/formancehq/ledger-v3-poc/internal/adapter/grpc"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/keystore"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	clusterhealth "github.com/formancehq/ledger-v3-poc/internal/infra/health"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/application/admission"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/coldstorage"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/application/events"
	"github.com/formancehq/ledger-v3-poc/internal/application/mirror"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/receipt"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/diskusage"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
	"google.golang.org/grpc/credentials"
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
			fx.Annotate(func(
				cfg Config,
				logger logging.Logger,
				raftPool *transport.ConnectionPool,
				meterProvider metric.MeterProvider,
			) *node.DefaultTransport {
				return node.NewTransport(
					logger,
					raftPool,
					meterProvider,
					cfg.RaftConfig.NodeID,
					cfg.TransportConfig,
					cfg.ClusterID,
					cfg.RaftConfig.TransportBufferSize,
					cfg.RaftConfig.AdvertiseAddr,
					cfg.ServiceAdvertiseAddr(),
				)
			}, fx.ParamTags(``, ``, `name:"raft"`, ``)),
			func(cfg Config, meterProvider metric.MeterProvider, logger logging.Logger) (*dal.Store, error) {
				store, err := dal.NewStore(
					cfg.DataDir,
					logger,
					meterProvider.Meter("pebble.runtime_store"),
					cfg.PebbleConfig,
				)
				if err != nil {
					return nil, err
				}

				if !cfg.Restore {
					if err := ValidateOrPersistConfig(store, cfg, logger, cfg.UnsafeSkipConfigValidation); err != nil {
						_ = store.Close()
						return nil, fmt.Errorf("configuration safety check failed: %w", err)
					}
				}

				return store, nil
			},
			func(store *dal.Store, logger logging.Logger, machine *state.Machine) *dal.SmartCompactor {
				return dal.NewSmartCompactor(store, logger, machine.ColdCompactionCh())
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
				store *dal.Store,
				meterProvider metric.MeterProvider,
				c *cache.Cache,
				attrs *attributes.Attributes,
				ks *keystore.KeyStore,
				ss *state.SharedState,
				eventNotifications *events.Notifications,
				mirrorNotifications *mirror.Notifications,
			) (*state.Machine, error) {
				machineStart := time.Now()
				m, err := state.NewMachine(
					logger,
					store,
					meterProvider.Meter("raft.node"),
					c,
					attrs,
					cfg.RaftConfig.RotationThreshold,
					ks,
					ss,
					eventNotifications,
					mirrorNotifications,
					cfg.NumscriptCacheSize,
				)
				if err != nil {
					return nil, err
				}
				logger.WithFields(map[string]any{
					"duration": time.Since(machineStart).String(),
				}).Infof("FSM Machine created")
				return m, nil
			},
			func(
				params struct {
					fx.In
					NodeConfig              node.NodeConfig
					Logger                  logging.Logger
					Transport               *node.DefaultTransport
					MeterProvider           metric.MeterProvider
					Store                   *dal.Store
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
				params.Logger.WithFields(map[string]any{
					"freshStart":     freshStart,
					"walVoters":      snapshot.Metadata.ConfState.Voters,
					"walLearners":    snapshot.Metadata.ConfState.Learners,
					"snapshotIndex":  snapshot.Metadata.Index,
					"snapshotTerm":   snapshot.Metadata.Term,
				}).Infof("WAL fresh start detection")

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
			func(cfg Config, logger logging.Logger) *signing.ResponseSigner {
				if cfg.ResponseSigningKeyFile == "" {
					return nil
				}
				seed, err := signing.LoadSeedFromFile(cfg.ResponseSigningKeyFile)
				if err != nil {
					logger.Errorf("Failed to load response signing key: %v", err)
					return nil
				}
				signer := signing.NewResponseSigner(seed)
				logger.WithFields(map[string]any{
					"key_id": signer.KeyID(),
				}).Infof("Response signing enabled")
				return signer
			},
			func(cfg Config) node.NodeConfig {
				cfg.RaftConfig.DataDir = cfg.DataDir
				cfg.RaftConfig.SetDefaults()
				return cfg.RaftConfig
			},
			func(cfg Config) node.TransportConfig {
				return cfg.TransportConfig
			},
			func(cfg Config) transport.PoolConfig {
				return cfg.PoolConfig
			},
			func(cfg Config) (credentials.TransportCredentials, error) {
				return ClientTransportCredentials(cfg.TLSConfig)
			},
			// RaftServer for internal inter-node communication (Raft transport + Snapshot)
			func(cfg Config, logger logging.Logger) (*grpcadp.RaftServer, error) {
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

				return grpcadp.NewRaftServer(port, logger, tlsOpt), nil
			},
			// ServiceServer for external client-facing API
			func(cfg Config, logger logging.Logger) (*grpcadp.ServiceServer, error) {
				tlsOpt, err := ServerCredentials(cfg.TLSConfig)
				if err != nil {
					return nil, fmt.Errorf("loading TLS credentials for service server: %w", err)
				}

				return grpcadp.NewServiceServer(cfg.GRPCPort, logger, cfg.Debug, tlsOpt), nil
			},
			fx.Annotate(func(cfg Config, logger logging.Logger, ctrl ctrl.Controller, s *dal.Store, attrs *attributes.Attributes, ss *state.SharedState, signer *receipt.Signer, respSigner *signing.ResponseSigner, keySet oidc.KeySet) servicepb.BucketServiceServer {
				authCfg := internalauth.AuthConfig{
					Enabled:     cfg.AuthConfig.Enabled,
					KeySet:      keySet,
					Issuer:      cfg.AuthConfig.Issuer,
					Service:     cfg.AuthConfig.Service,
					CheckScopes: cfg.AuthConfig.CheckScopes,
				}
				return grpcadp.NewBucketServiceServer(logger, ctrl, s, attrs, ss, signer, respSigner, authCfg)
			}, fx.ParamTags(``, ``, ``, ``, ``, ``, ``, ``, `optional:"true"`)),
			func(logger logging.Logger, s *dal.Store) snapshotpb.SnapshotServiceServer {
				return grpcadp.NewSnapshotServiceServer(logger, s)
			},
			func(cfg Config, meterProvider metric.MeterProvider) *diskusage.Collector {
				return diskusage.NewCollector(
					cfg.RaftConfig.WalDir,
					cfg.DataDir,
					10*time.Second,
					meterProvider.Meter("storage"),
				)
			},
			fx.Annotate(func(n *node.Node, raftTransport *node.DefaultTransport, servicePool *transport.ConnectionPool, collector *diskusage.Collector, store *dal.Store, ss *state.SharedState, logger logging.Logger, cfg Config, keySet oidc.KeySet) clusterpb.ClusterServiceServer {
				authCfg := internalauth.AuthConfig{
					Enabled:     cfg.AuthConfig.Enabled,
					KeySet:      keySet,
					Issuer:      cfg.AuthConfig.Issuer,
					Service:     cfg.AuthConfig.Service,
					CheckScopes: cfg.AuthConfig.CheckScopes,
				}
				return grpcadp.NewClusterServiceServer(n, raftTransport, servicePool, collector, store, ss, logger,
					cfg.RaftConfig.AdvertiseAddr,
					cfg.ServiceAdvertiseAddr(),
					authCfg,
				)
			}, fx.ParamTags(``, ``, `name:"service"`, ``, ``, ``, ``, ``, `optional:"true"`)),
			fx.Annotate(func(n *node.Node, collector *diskusage.Collector, servicePool *transport.ConnectionPool, cfg Config, logger logging.Logger) *clusterhealth.HealthChecker {
				return clusterhealth.NewHealthChecker(
					n, collector, servicePool,
					logger,
					cfg.HealthConfig.Interval,
					cfg.HealthConfig.WALThreshold,
					cfg.HealthConfig.DataThreshold,
					cfg.HealthConfig.ClockSkewThreshold,
				)
			}, fx.ParamTags(``, ``, `name:"service"`, ``, ``)),
			func() *keystore.KeyStore {
				return keystore.NewKeyStore()
			},
			state.NewSharedState,
			events.NewNotifications,
			events.NewManager,
			mirror.NewNotifications,
			func(store *dal.Store, proposer mirror.Proposer, c *cache.Cache, attrs *attributes.Attributes, logger logging.Logger, notifications *mirror.Notifications, meterProvider metric.MeterProvider, cfg Config) *mirror.Manager {
				return mirror.NewManager(store, proposer, c, attrs, logger, notifications, meterProvider, cfg.MirrorMaxBatchSize)
			},
			// Provide mirror.Proposer from the Raft node
			func(n *node.Node) mirror.Proposer {
				return n
			},
			httpcompat.NewServer,
			fx.Annotate(func(cfg Config, logger logging.Logger, backend httpcompat.Backend, keySet oidc.KeySet) http.Handler {
				authCfg := internalauth.AuthConfig{
					Enabled:     cfg.AuthConfig.Enabled,
					KeySet:      keySet,
					Issuer:      cfg.AuthConfig.Issuer,
					Service:     cfg.AuthConfig.Service,
					CheckScopes: cfg.AuthConfig.CheckScopes,
				}
				return httpcompat.NewHandler(logger, backend, authCfg)
			}, fx.ParamTags(``, ``, ``, `optional:"true"`)),
			func(node *node.Node, ctrl ctrl.Controller) httpcompat.Backend {
				return httpcompat.NewDefaultBackend(node, ctrl)
			},
			func(
				cfg Config,
				node *node.Node,
				cache *cache.Cache,
				store *dal.Store,
				logger logging.Logger,
				attrs *attributes.Attributes,
				meterProvider metric.MeterProvider,
				hc *clusterhealth.HealthChecker,
				ks *keystore.KeyStore,
				ss *state.SharedState,
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
					ss,
					opts...,
				)
			},
			func(
				logger logging.Logger,
				store *dal.Store,
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
				}, raftNode.IsLeader, machine)
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
				store *dal.Store,
				cold coldstorage.ColdStorage,
				machine *state.Machine,
				admissionHandler ctrl.Admission,
				raftNode *node.Node,
			) *state.Archiver {
				bucketID := cfg.ColdStorageConfig.BucketID
				if bucketID == "" {
					bucketID = cfg.ClusterID
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
				logger logging.Logger,
				machine *state.Machine,
				admissionHandler ctrl.Admission,
				raftNode *node.Node,
			) *state.PeriodScheduler {
				return state.NewPeriodScheduler(
					logger,
					raftNode.IsLeader,
					machine.PeriodSchedule,
					func() {
						_, _ = admissionHandler.Admit(context.Background(), &servicepb.Request{
							Type: &servicepb.Request_ClosePeriod{
								ClosePeriod: &servicepb.ClosePeriodRequest{},
							},
						})
					},
					machine.ScheduleChanged(),
				)
			},
			func(
				logger logging.Logger,
				store *dal.Store,
				attrs *attributes.Attributes,
				machine *state.Machine,
				raftNode *node.Node,
			) *state.MetadataConverter {
				return state.NewMetadataConverter(
					logger,
					store,
					attrs,
					machine.MetadataConvertRequestCh(),
					NewNodeProposer(raftNode),
					raftNode.IsLeader,
					100, // batchSize
					4,   // poolSize — max concurrent field conversions
				)
			},
			fx.Annotate(func(
				raftNode *node.Node,
				servicePool *transport.ConnectionPool,
				admission ctrl.Admission,
				store *dal.Store,
				logger logging.Logger,
				attrs *attributes.Attributes,
			) ctrl.Controller {
				return NewRoutedController(
					ctrl.NewDefaultController(admission, store, logger, attrs),
					raftNode,
					servicePool,
				)
			}, fx.ParamTags(``, `name:"service"`, ``, ``, ``, ``)),
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
				runtime *dal.Store,
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
			func(raftServer *grpcadp.RaftServer, transport *node.DefaultTransport) error {
				node.RegisterRaftTransportService(raftServer.GetServer(), transport)
				return nil
			},
			func(raftServer *grpcadp.RaftServer, snapshotServiceServer snapshotpb.SnapshotServiceServer) error {
				grpcadp.RegisterSnapshotService(raftServer.GetServer(), snapshotServiceServer)
				return nil
			},
			// Register business services on ServiceServer (external)
			func(raftServer *grpcadp.RaftServer) error {
				hs := health.NewServer()
				healthpb.RegisterHealthServer(raftServer.GetServer(), hs)
				hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
				return nil
			},
			func(serviceServer *grpcadp.ServiceServer) error {
				hs := health.NewServer()
				healthpb.RegisterHealthServer(serviceServer.GetServer(), hs)
				hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
				return nil
			},
			func(serviceServer *grpcadp.ServiceServer, bucketServiceServer servicepb.BucketServiceServer) error {
				grpcadp.RegisterBucketService(serviceServer.GetServer(), bucketServiceServer)
				return nil
			},
			func(serviceServer *grpcadp.ServiceServer, clusterServiceServer clusterpb.ClusterServiceServer) error {
				grpcadp.RegisterClusterService(serviceServer.GetServer(), clusterServiceServer)
				return nil
			},
			// Start Raft server (internal) - must start before adding peers
			fx.Annotate(func(
				lc fx.Lifecycle,
				raftServer *grpcadp.RaftServer,
				logger logging.Logger,
				defaultTransport *node.DefaultTransport,
				servicePool *transport.ConnectionPool,
				cfg node.NodeConfig,
				n *node.Node,
				fullCfg Config,
			) {
				// Store own address in Node so it gets included in the next snapshot.
				// This ensures that after a snapshot cycle, all nodes know this node's address.
				n.SetPeerAddress(cfg.NodeID, cfg.AdvertiseAddr, fullCfg.ServiceAdvertiseAddr())

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

						// Load peers from config (set during --join or --peers)
						for _, peerEntry := range cfg.Peers {
							logger := logger.WithFields(map[string]any{"peer": peerEntry})
							logger.Infof("Adding peer to transport and service pool")
							defaultTransport.AddPeer(peerEntry.ID, peerEntry.Address)
							if err := servicePool.AddPeer(peerEntry.ID, peerEntry.ServiceAddress); err != nil {
								logger.WithFields(map[string]any{"error": err}).Errorf("Failed to add peer to service pool")
							}
						}

						// Recover peers from snapshot + WAL (populated by NewNode during recovery).
						for nodeID, addr := range n.RecoveredPeers() {
							if nodeID == cfg.NodeID {
								continue // skip self
							}
							logger := logger.WithFields(map[string]any{
								"peer_id":      nodeID,
								"raft_addr":    addr.RaftAddress,
								"service_addr": addr.ServiceAddress,
							})
							logger.Infof("Restoring recovered peer")
							defaultTransport.AddPeer(nodeID, addr.RaftAddress)
							if err := servicePool.AddPeer(nodeID, addr.ServiceAddress); err != nil {
								logger.WithFields(map[string]any{"error": err}).Errorf("Failed to add recovered peer to service pool")
							}
						}

						return nil
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Stopping Raft gRPC server")
						return raftServer.Stop()
					},
				})
			}, fx.ParamTags(``, ``, ``, ``, `name:"service"`, ``, ``, ``)),
			// Start Service server (external)
			func(
				lc fx.Lifecycle,
				serviceServer *grpcadp.ServiceServer,
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
			fx.Annotate(func(
				n *node.Node,
				defaultTransport *node.DefaultTransport,
				servicePool *transport.ConnectionPool,
				logger logging.Logger,
				eventsManager *events.Manager,
				mirrorManager *mirror.Manager,
			) {
				n.SetObserver(node.NewObserver(func(event any) {
					switch e := event.(type) {
					case node.ConfChangeEvent:
						handleConfChangeEvent(e, defaultTransport, servicePool, logger)
					case node.LeadershipChangeEvent:
						handleLeadershipChangeEvent(e, eventsManager, mirrorManager, logger)
					default:
						logger.Errorf("Unknown observer event type: %T", event)
					}
				}))
			}, fx.ParamTags(``, ``, `name:"service"`, ``, ``, ``)),
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
			fx.Annotate(func(
				lc fx.Lifecycle,
				cfg Config,
				freshStart walFreshStart,
				servicePool *transport.ConnectionPool,
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
							logger.WithFields(map[string]any{
								"nodeID": cfg.RaftConfig.NodeID,
							}).Infof("WARNING: Learner registration SKIPPED — WAL already contains ConfState voters (not a fresh start). " +
								"If this node was never successfully added to the cluster, delete its WAL directory and retry")
							return nil
						}

						peer := cfg.RaftConfig.Peers[0]
						logger.WithFields(map[string]any{
							"nodeID":         cfg.RaftConfig.NodeID,
							"targetPeerID":   peer.ID,
							"targetPeerAddr": peer.ServiceAddress,
							"raftAddress":    cfg.RaftConfig.AdvertiseAddr,
							"serviceAddress": cfg.ServiceAdvertiseAddr(),
						}).Infof("Join mode: requesting peer to add this node as learner")

						conn := servicePool.GetConnection(peer.ID)
						if conn == nil {
							return fmt.Errorf("failed to register as learner: no gRPC connection to peer %d (address: %s)", peer.ID, peer.ServiceAddress)
						}

						client := clusterpb.NewClusterServiceClient(conn)
						_, err := client.AddLearner(ctx, &clusterpb.AddLearnerRequest{
							NodeId:         cfg.RaftConfig.NodeID,
							RaftAddress:    cfg.RaftConfig.AdvertiseAddr,
							ServiceAddress: cfg.ServiceAdvertiseAddr(),
						})
						if err != nil {
							return fmt.Errorf("failed to register as learner via peer %d (%s): %w", peer.ID, peer.ServiceAddress, err)
						}

						logger.WithFields(map[string]any{
							"peer": peer.ID,
						}).Infof("Successfully registered as learner on the cluster")
						return nil
					},
				})
			}, fx.ParamTags(``, ``, ``, `name:"service"`, ``)),
			func(lc fx.Lifecycle, cfg Config, handler http.Handler) {
				lc.Append(httpserver.NewHook(handler,
					httpserver.WithAddress(fmt.Sprintf(":%d", cfg.HTTPPort)),
				))
			},
			func(lc fx.Lifecycle, collector *diskusage.Collector) {
				lc.Append(worker.FxHook(collector))
			},
			func(lc fx.Lifecycle, compactor *dal.SmartCompactor) {
				lc.Append(worker.FxHook(compactor))
			},
			func(lc fx.Lifecycle, hc *clusterhealth.HealthChecker) {
				lc.Append(worker.FxHook(hc))
			},
			func(lc fx.Lifecycle, manager *events.Manager) {
				lc.Append(worker.FxHook(manager))
			},
			func(lc fx.Lifecycle, manager *mirror.Manager) {
				lc.Append(worker.FxHook(manager))
			},
			func(lc fx.Lifecycle, sealer *state.Sealer) {
				lc.Append(worker.FxHook(sealer))
			},
			func(lc fx.Lifecycle, archiver *state.Archiver) {
				lc.Append(worker.FxHook(archiver))
			},
			func(lc fx.Lifecycle, scheduler *state.PeriodScheduler) {
				lc.Append(worker.FxHook(scheduler))
			},
			func(lc fx.Lifecycle, converter *state.MetadataConverter) {
				lc.Append(worker.FxHook(converter))
			},
		),
	)
}

// handleConfChangeEvent processes a single ConfChangeEvent by updating the
// transport and service pool when a node joins or leaves the cluster.
// Peer addresses are persisted in the Node (updated by processReady) and
// included in Raft snapshots, so no separate PeerStore is needed.
func handleConfChangeEvent(
	e node.ConfChangeEvent,
	defaultTransport *node.DefaultTransport,
	servicePool *transport.ConnectionPool,
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
	case raftpb.ConfChangeRemoveNode:
		logger.WithFields(map[string]any{
			"node_id": e.NodeID,
		}).Infof("Removing peer from ConfChange")
		defaultTransport.RemovePeer(context.Background(), e.NodeID)
		if err := servicePool.RemovePeer(e.NodeID); err != nil {
			logger.WithFields(map[string]any{"error": err}).Errorf("Failed to remove peer from service pool")
		}
	}
}

// handleLeadershipChangeEvent notifies the event and mirror Managers of leadership changes.
func handleLeadershipChangeEvent(
	e node.LeadershipChangeEvent,
	eventsManager *events.Manager,
	mirrorManager *mirror.Manager,
	logger logging.Logger,
) {
	if e.IsLeader {
		logger.Infof("Became leader — reconciling event emitter and mirror workers")
	} else {
		logger.Infof("Lost leadership — tearing down event emitter and mirror workers")
	}
	eventsManager.OnLeadershipChange(e.IsLeader)
	mirrorManager.OnLeadershipChange(e.IsLeader)
}
