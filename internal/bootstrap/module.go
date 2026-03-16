package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/oidc"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"

	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	grpcadp "github.com/formancehq/ledger-v3-poc/internal/adapter/grpc"
	httpcompat "github.com/formancehq/ledger-v3-poc/internal/adapter/http"
	"github.com/formancehq/ledger-v3-poc/internal/application/admission"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/application/events"
	"github.com/formancehq/ledger-v3-poc/internal/application/indexbuilder"
	"github.com/formancehq/ledger-v3-poc/internal/application/mirror"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/coldstorage"
	clusterhealth "github.com/formancehq/ledger-v3-poc/internal/infra/health"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/diskusage"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/preload"
	"github.com/formancehq/ledger-v3-poc/internal/infra/receipt"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/keystore"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
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
					logger.Infof("Validating persisted configuration...")

					configStart := time.Now()

					err := ValidateOrPersistConfig(store, cfg, logger, cfg.UnsafeSkipConfigValidation)
					if err != nil {
						_ = store.Close()

						return nil, fmt.Errorf("configuration safety check failed: %w", err)
					}

					logger.WithFields(map[string]any{
						"duration": time.Since(configStart).String(),
					}).Infof("Configuration validation done")
				}

				return store, nil
			},
			fx.Annotate(func(cfg Config, store *dal.Store, logger logging.Logger, notifications *signal.Notifications, machine *state.Machine) *dal.SmartCompactor {
				return dal.NewSmartCompactor(store, logger, notifications, machine.ColdCompactionCh(), cfg.PebbleConfig.IncrementalCompactThreshold)
			}, fx.ParamTags(``, ``, ``, `name:"events"`, ``)),
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
			ctrl.GRPCSnapshotFetcherProvider,
			func(
				cfg node.NodeConfig,
				logger logging.Logger,
				store *dal.Store,
				meterProvider metric.MeterProvider,
				sp *spool.Default,
				w *wal.DefaultWAL,
				snapshotFetcherProvider state.SnapshotFetcherProvider,
				machine *state.Machine,
			) (*node.Applier, error) {
				return node.NewApplier(
					machine,
					sp,
					store,
					w,
					logger,
					meterProvider.Meter("raft.node"),
					cfg.SnapshotThreshold,
					cfg.CompactionMargin,
					cfg.ReplayBatchSize,
					snapshotFetcherProvider,
				)
			},
			// Provide events.Proposer from the Raft node (used by event emitter to replicate cursor)
			func(n *node.Node) events.Proposer {
				return n
			},
			func(cfg node.NodeConfig, meterProvider metric.MeterProvider) (*cache.Cache, error) {
				return cache.New(cfg.RotationThreshold, meterProvider.Meter("cache"))
			},
			func(n *node.Node, c *cache.Cache, attrs *attributes.Attributes, store *dal.Store, logger logging.Logger) *preload.Preloader {
				return preload.New(n.IndexTracker(), c, attrs, store, logger)
			},
			fx.Annotate(func(
				cfg Config,
				logger logging.Logger,
				store *dal.Store,
				meterProvider metric.MeterProvider,
				c *cache.Cache,
				attrs *attributes.Attributes,
				ks *keystore.KeyStore,
				ss *state.SharedState,
				eventNotifications *signal.Notifications,
				mirrorNotifications *signal.Notifications,
				indexNotifications *signal.Notifications,
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
					indexNotifications,
					cfg.NumscriptCacheSize,
				)
				if err != nil {
					return nil, err
				}

				logger.WithFields(map[string]any{
					"duration": time.Since(machineStart).String(),
				}).Infof("FSM Machine created")

				return m, nil
			}, fx.ParamTags(``, ``, ``, ``, ``, ``, ``, ``, `name:"events"`, `name:"mirror"`, `name:"index"`)),
			func(
				params struct {
					fx.In

					NodeConfig    node.NodeConfig
					Logger        logging.Logger
					Transport     *node.DefaultTransport
					MeterProvider metric.MeterProvider
					WAL           *wal.DefaultWAL
					Applier       *node.Applier
					Machine       *state.Machine
				},
			) (nodeProvideResult, error) {
				// Check WAL emptiness before NewNode writes the initial snapshot.
				snapshot, err := params.WAL.Snapshot()
				if err != nil {
					return nodeProvideResult{}, fmt.Errorf("reading WAL snapshot: %w", err)
				}

				freshStart := walFreshStart(len(snapshot.Metadata.ConfState.Voters) == 0)
				params.Logger.WithFields(map[string]any{
					"freshStart":    freshStart,
					"walVoters":     snapshot.Metadata.ConfState.Voters,
					"walLearners":   snapshot.Metadata.ConfState.Learners,
					"snapshotIndex": snapshot.Metadata.Index,
					"snapshotTerm":  snapshot.Metadata.Term,
				}).Infof("WAL fresh start detection")

				n, err := node.NewNode(
					params.NodeConfig,
					params.Transport,
					params.Applier,
					params.Logger,
					params.MeterProvider.Meter("raft.node"),
					params.WAL,
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
				poolCfg := cfg.PoolConfig
				if cfg.ClusterSecret != "" {
					poolCfg.AuthToken = cfg.ClusterSecret
				}

				return poolCfg
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

				return grpcadp.NewServiceServer(cfg.GRPCPort, logger, cfg.Debug, cfg.GRPCSlowThreshold, tlsOpt), nil
			},
			// Provide a single AuthConfig used by gRPC and HTTP handlers.
			fx.Annotate(buildAuthConfig, fx.ParamTags(``, ``, `optional:"true"`)),
			func(cfg Config, logger logging.Logger, ctrl ctrl.Controller, s *dal.Store, rs *readstore.Store, attrs *attributes.Attributes, ss *state.SharedState, signer *receipt.Signer, respSigner *signing.ResponseSigner, authCfg internalauth.AuthConfig) servicepb.BucketServiceServer {
				return grpcadp.NewBucketServiceServer(logger, ctrl, s, rs, attrs, ss, signer, respSigner, authCfg, cfg.QueryProfileThreshold)
			},
			grpcadp.NewSnapshotServiceServer,
			func(cfg Config, meterProvider metric.MeterProvider) *diskusage.Collector {
				readIndexDir := cfg.ReadIndexConfig.Dir
				if readIndexDir == "" {
					readIndexDir = filepath.Join(cfg.DataDir, "read-indexes")
				}

				return diskusage.NewCollector(
					cfg.RaftConfig.WalDir,
					cfg.DataDir,
					readIndexDir,
					10*time.Second,
					meterProvider.Meter("storage"),
				)
			},
			fx.Annotate(func(n *node.Node, raftTransport *node.DefaultTransport, servicePool *transport.ConnectionPool, collector *diskusage.Collector, store *dal.Store, ss *state.SharedState, ib *indexbuilder.Builder, rs *readstore.Store, logger logging.Logger, cfg Config, authCfg internalauth.AuthConfig) clusterpb.ClusterServiceServer {
				return grpcadp.NewClusterServiceServer(n, raftTransport, servicePool, collector, store, ss, ib, rs, logger,
					cfg.RaftConfig.AdvertiseAddr,
					cfg.ServiceAdvertiseAddr(),
					authCfg,
				)
			}, fx.ParamTags(``, ``, `name:"service"`, ``, ``, ``, ``, ``, ``, ``, ``)),
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
			keystore.NewKeyStore,
			state.NewSharedState,
			fx.Annotate(signal.NewNotifications, fx.ResultTags(`name:"events"`)),
			fx.Annotate(events.NewManager, fx.ParamTags(``, ``, ``, `name:"events"`)),
			fx.Annotate(signal.NewNotifications, fx.ResultTags(`name:"mirror"`)),
			fx.Annotate(signal.NewNotifications, fx.ResultTags(`name:"index"`)),
			fx.Annotate(func(store *dal.Store, proposer mirror.Proposer, preloader *preload.Preloader, logger logging.Logger, notifications *signal.Notifications, meterProvider metric.MeterProvider, cfg Config) *mirror.Manager {
				return mirror.NewManager(store, proposer, preloader, logger, notifications, meterProvider, cfg.MirrorMaxBatchSize)
			}, fx.ParamTags(``, ``, ``, ``, `name:"mirror"`, ``, ``)),
			// Provide mirror.Proposer from the Raft node
			func(n *node.Node) mirror.Proposer {
				return n
			},
			// Read index store (Pebble) — always enabled
			func(cfg Config, logger logging.Logger) (*readstore.Store, error) {
				dir := cfg.ReadIndexConfig.Dir
				if dir == "" {
					dir = filepath.Join(cfg.DataDir, "read-indexes")
				}

				return readstore.New(dir, logger, cfg.ReadIndexConfig.PebbleConfig)
			},
			// Index builder — tails the Raft log to populate the read index
			func(store *dal.Store, rs *readstore.Store, logger logging.Logger, meterProvider metric.MeterProvider, cfg Config) *indexbuilder.Builder {
				return indexbuilder.NewBuilder(store, rs, logger, meterProvider.Meter("index.builder"), cfg.ReadIndexConfig.BatchSize)
			},
			httpcompat.NewServer,
			func(cfg Config, logger logging.Logger, backend httpcompat.Backend, authCfg internalauth.AuthConfig) http.Handler {
				return httpcompat.NewHandler(logger, backend, authCfg)
			},
			func(node *node.Node, ctrl ctrl.Controller, hc *clusterhealth.HealthChecker) httpcompat.Backend {
				return httpcompat.NewDefaultBackend(node, ctrl, hc)
			},
			func(
				cfg Config,
				node *node.Node,
				store *dal.Store,
				logger logging.Logger,
				preloader *preload.Preloader,
				meterProvider metric.MeterProvider,
				hc *clusterhealth.HealthChecker,
				ks *keystore.KeyStore,
				ss *state.SharedState,
				receiptSigner *receipt.Signer,
				attrs *attributes.Attributes,
			) ctrl.Admission {
				var opts []func(*admission.Admission)
				if cfg.AdmissionMetrics {
					opts = append(opts, admission.WithMetrics())
				}

				if receiptSigner != nil {
					opts = append(opts, admission.WithReceiptSigner(receiptSigner))
				}

				return admission.NewAdmission(
					store,
					logger,
					node,
					preloader,
					meterProvider,
					hc,
					ks,
					ss,
					attrs,
					numscript.NewNumscriptCache(cfg.NumscriptCacheSize),
					opts...,
				)
			},
			func(
				logger logging.Logger,
				store *dal.Store,
				attrs *attributes.Attributes,
				machine *state.Machine,
				admissionHandler ctrl.Admission,
				raftNode *node.Node,
			) *state.Sealer {
				return state.NewSealer(logger, store, attrs, machine.SealRequestCh(), func(periodID uint64, sealingHash, stateHash []byte) {
					_, _ = admissionHandler.Admit(context.Background(), &servicepb.Request{
						Type: &servicepb.Request_SealPeriod{
							SealPeriod: &servicepb.SealPeriodRequest{
								PeriodId:    periodID,
								SealingHash: sealingHash,
								StateHash:   stateHash,
							},
						},
					})
				}, raftNode.IsLeader, machine)
			},
			func(cfg Config, logger logging.Logger) (coldstorage.ColdStorage, error) {
				switch cfg.ColdStorageConfig.Driver {
				case "s3":
					if cfg.ColdStorageConfig.S3Bucket == "" {
						return nil, errors.New("--cold-storage-s3-bucket is required when driver=s3")
					}

					logger.WithFields(map[string]any{
						"bucket":   cfg.ColdStorageConfig.S3Bucket,
						"region":   cfg.ColdStorageConfig.S3Region,
						"endpoint": cfg.ColdStorageConfig.S3Endpoint,
					}).Infof("Using S3 cold storage")

					return coldstorage.NewS3ColdStorage(
						cfg.ColdStorageConfig.S3Bucket,
						cfg.ColdStorageConfig.S3Region,
						cfg.ColdStorageConfig.S3Endpoint,
					)
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
				rs *readstore.Store,
			) ctrl.Controller {
				return NewRoutedController(
					ctrl.NewDefaultController(admission, store, logger, attrs, rs),
					raftNode,
					servicePool,
				)
			}, fx.ParamTags(``, `name:"service"`, ``, ``, ``, ``, ``)),
			func(serviceServer *grpcadp.ServiceServer, n *node.Node, hc *clusterhealth.HealthChecker) *clusterhealth.GRPCHealthUpdater {
				hs := health.NewServer()
				healthpb.RegisterHealthServer(serviceServer.GetServer(), hs)

				return clusterhealth.NewGRPCHealthUpdater(n, hc, hs)
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
				runtime *dal.Store,
				wal *wal.DefaultWAL,
				rs *readstore.Store,
				sp *spool.Default,
				cfg Config,
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
				lc.Append(fx.Hook{
					OnStop: func(_ context.Context) error {
						return rs.Close()
					},
				})
			},
			func(
				lc fx.Lifecycle,
				t *node.DefaultTransport,
				logger logging.Logger,
			) {
				var wait func()

				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						wait = otlplogs.GoWait(func() {
							t.Start(context.WithoutCancel(ctx))
						}, logger)

						return nil
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Stopping raft transport")

						err := t.Stop(ctx)
						wait()

						return err
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

				var waitRaft func()

				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						logger.Infof("Starting Raft gRPC server")

						listening := make(chan struct{})

						reflection.Register(raftServer.GetServer())
						waitRaft = otlplogs.GoWait(func() {
							err := raftServer.Start(listening)
							if err != nil {
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

							err := servicePool.AddPeer(peerEntry.ID, peerEntry.ServiceAddress)
							if err != nil {
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

							err := servicePool.AddPeer(nodeID, addr.ServiceAddress)
							if err != nil {
								logger.WithFields(map[string]any{"error": err}).Errorf("Failed to add recovered peer to service pool")
							}
						}

						return nil
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Stopping Raft gRPC server")

						err := raftServer.Stop()
						waitRaft()

						return err
					},
				})
			}, fx.ParamTags(``, ``, ``, ``, `name:"service"`, ``, ``, ``)),
			// Start Service server (external)
			func(
				lc fx.Lifecycle,
				serviceServer *grpcadp.ServiceServer,
				logger logging.Logger,
			) {
				var waitService func()

				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						logger.Infof("Starting Service gRPC server")

						listening := make(chan struct{})

						waitService = otlplogs.GoWait(func() {
							err := serviceServer.Start(listening)
							if err != nil {
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

						err := serviceServer.Stop()
						waitService()

						return err
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
				var waitNode func()

				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						ready := make(chan struct{})

						waitNode = otlplogs.GoWait(func() {
							err := node.Run(context.WithoutCancel(ctx), ready)
							if err != nil {
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

						err := node.Stop(ctx)
						if err != nil {
							return fmt.Errorf("shutting down raft cluster: %w", err)
						}

						waitNode()
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
			func(lc fx.Lifecycle, updater *clusterhealth.GRPCHealthUpdater) {
				lc.Append(worker.FxHook(updater))
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
			// Register Pebble read index metrics and unregister on stop.
			func(lc fx.Lifecycle, rs *readstore.Store, meterProvider metric.MeterProvider) error {
				reg, err := rs.RegisterMetrics(meterProvider.Meter("readindex"))
				if err != nil {
					return fmt.Errorf("registering readindex metrics: %w", err)
				}

				lc.Append(fx.Hook{
					OnStop: func(_ context.Context) error {
						return reg.Unregister()
					},
				})

				return nil
			},
			// Start and stop the index builder.
			// The builder has its own dedicated Notifications signal to receive
			// log-committed events from the FSM without competing with other consumers.
			fx.Annotate(func(lc fx.Lifecycle, builder *indexbuilder.Builder, notifications *signal.Notifications, raftNode *node.Node) {
				builder.SetNotifications(notifications)
				builder.SetProposer(NewNodeProposer(raftNode), raftNode.IsLeader)
				lc.Append(worker.FxHook(builder))
			}, fx.ParamTags(``, ``, `name:"index"`, ``)),
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

		err := servicePool.RemovePeer(e.NodeID)
		if err != nil {
			logger.WithFields(map[string]any{"error": err}).Errorf("Failed to remove peer from service pool")
		}
	}
}

// buildAuthConfig constructs an AuthConfig from the server configuration and optional OIDC KeySet.
// If Ed25519 keys are configured, it creates a composite KeySet that handles both OIDC and EdDSA tokens.
// Scope mapping is loaded from file, env var, or defaults to the backward-compatible mapping.
func buildAuthConfig(cfg Config, logger logging.Logger, oidcKeySet oidc.KeySet) (internalauth.AuthConfig, error) {
	authCfg := internalauth.AuthConfig{
		Enabled: cfg.AuthConfig.Enabled,
		Issuer:  cfg.AuthConfig.Issuer,
		Service: cfg.AuthConfig.Service,
	}

	if cfg.AuthConfig.Ed25519KeysFile != "" {
		ed25519KeySet, allowedScopes, err := internalauth.LoadEd25519KeySet(cfg.AuthConfig.Ed25519KeysFile)
		if err != nil {
			return authCfg, fmt.Errorf("loading Ed25519 keys: %w", err)
		}

		authCfg.KeySet = internalauth.NewCompositeKeySet(ed25519KeySet, oidcKeySet)
		authCfg.Ed25519AllowedScopes = allowedScopes

		logger.WithFields(map[string]any{
			"keys_count": len(allowedScopes),
			"enabled":    authCfg.Enabled,
		}).Infof("Ed25519 keys loaded")
	} else {
		authCfg.KeySet = oidcKeySet
	}

	// Load scope mapping: file > env var > default
	scopeMapping, err := loadScopeMapping(cfg, logger)
	if err != nil {
		return authCfg, err
	}

	authCfg.ScopeMapping = scopeMapping
	authCfg.ClusterSecret = cfg.ClusterSecret

	return authCfg, nil
}

// loadScopeMapping loads the scope mapping from file, env var JSON, or defaults.
func loadScopeMapping(cfg Config, logger logging.Logger) (internalauth.ScopeMapping, error) {
	if cfg.AuthConfig.ScopeMappingFile != "" {
		mapping, err := internalauth.LoadScopeMappingFromFile(cfg.AuthConfig.ScopeMappingFile)
		if err != nil {
			return nil, fmt.Errorf("loading scope mapping file: %w", err)
		}

		logger.WithFields(map[string]any{
			"file":       cfg.AuthConfig.ScopeMappingFile,
			"keys_count": len(mapping),
		}).Infof("Custom scope mapping loaded from file")

		return mapping, nil
	}

	if cfg.AuthConfig.ScopeMappingJSON != "" {
		mapping, err := internalauth.ParseScopeMappingJSON([]byte(cfg.AuthConfig.ScopeMappingJSON))
		if err != nil {
			return nil, fmt.Errorf("parsing AUTH_SCOPE_MAPPING env var: %w", err)
		}

		logger.WithFields(map[string]any{
			"keys_count": len(mapping),
		}).Infof("Custom scope mapping loaded from env var")

		return mapping, nil
	}

	return internalauth.DefaultMapping(cfg.AuthConfig.Service), nil
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
