package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	oidcclient "github.com/formancehq/go-libs/v5/pkg/authn/oidc/client"
	"github.com/formancehq/go-libs/v5/pkg/fx/transportfx"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	otlpmetrics "github.com/formancehq/go-libs/v5/pkg/observe/metrics"
	"github.com/formancehq/go-libs/v5/pkg/transport/httpserver"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	httpcompat "github.com/formancehq/ledger/v3/internal/adapter/http"
	"github.com/formancehq/ledger/v3/internal/application/admission"
	"github.com/formancehq/ledger/v3/internal/application/auditindexer"
	backupapp "github.com/formancehq/ledger/v3/internal/application/backup"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/application/events"
	"github.com/formancehq/ledger/v3/internal/application/indexbuilder"
	"github.com/formancehq/ledger/v3/internal/application/membership"
	"github.com/formancehq/ledger/v3/internal/application/mirror"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	clusterhealth "github.com/formancehq/ledger/v3/internal/infra/health"
	raftmembership "github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/monitoring/diskusage"
	"github.com/formancehq/ledger/v3/internal/infra/monitoring/flightrecorder"
	ledgermetrics "github.com/formancehq/ledger/v3/internal/infra/monitoring/metrics"
	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/receipt"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
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

// coldStorageModule conditionally provides ColdStorage, ColdReader, and Archiver
// when cold storage is enabled (driver != "none"). When disabled, these components
// are not added to the fx graph and archiving is rejected at the admission layer.
// ColdStorageModule conditionally provides ColdStorage, ColdReader, and Archiver
// when cold storage is enabled (driver != "none"). When disabled, these components
// are not added to the fx graph and archiving is rejected at the admission layer.
func ColdStorageModule(coldStorageDriver string) fx.Option {
	if coldStorageDriver == "none" {
		return fx.Options()
	}

	return fx.Options(
		fx.Provide(
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
			func(cfg Config, cold coldstorage.ColdStorage, logger logging.Logger) *coldstorage.ColdReader {
				bucketID := cfg.ColdStorageConfig.BucketID
				if bucketID == "" {
					bucketID = cfg.ClusterID
				}

				cacheDir := cfg.ColdStorageConfig.CacheDir
				if cacheDir == "" {
					cacheDir = filepath.Join(cfg.DataDir, "cold-cache")
				}

				return coldstorage.NewColdReader(cold, bucketID, cacheDir, 8, 10*time.Minute, logger)
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
					func(chapterID uint64) error {
						_, err := admissionHandler.Admit(internalauth.WithSystemActor(context.Background(), commands.ComponentChapterArchiver), servicepb.UnsignedApplyRequest("", &servicepb.Request{
							Type: &servicepb.Request_ConfirmArchiveChapter{
								ConfirmArchiveChapter: &servicepb.ConfirmArchiveChapterRequest{
									ChapterId: chapterID,
								},
							},
						}))

						return err
					},
					raftNode.IsLeader,
					machine,
					bucketID,
					machine.DispatchArchiveRequests,
				)
			},
		),
		fx.Invoke(
			func(lc fx.Lifecycle, coldReader *coldstorage.ColdReader) {
				lc.Append(fx.Hook{
					OnStop: func(_ context.Context) error {
						return coldReader.Close()
					},
				})
			},
			func(lc fx.Lifecycle, archiver *state.Archiver) {
				lc.Append(worker.FxHook(archiver))
			},
		),
	)
}

func Module() fx.Option {
	return fx.Options(
		transport.Module(),
		attributes.Module(),
		// Decorate the upstream MeterProvider so every instrument
		// our code creates is renamed according to --metrics-naming.
		// The decorator has no per-meter allowlist: anything the
		// application requests from this provider (admission, cache,
		// bloom, raft.*, pebble.*, numscript, …) goes through the
		// rewrite in `prom` mode. OTel semantic-convention auto-
		// instrumentation (go.*, process.*, system.*, http.*) targets
		// the *global* MeterProvider, which we leave as the raw SDK
		// provider — those metrics bypass this decorator entirely.
		fx.Decorate(func(cfg Config, inner metric.MeterProvider) metric.MeterProvider {
			naming, err := ledgermetrics.ParseNaming(cfg.MetricsNaming)
			if err != nil {
				// Config.Validate() has already rejected invalid values;
				// fall back to the default so a misconfigured test fixture
				// doesn't crash the fx graph.
				naming = ledgermetrics.DefaultNaming
			}

			return ledgermetrics.NewFactory(inner, naming)
		}),
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
			func(store *dal.Store, logger logging.Logger, machine *state.Machine) *dal.SmartCompactor {
				return dal.NewSmartCompactor(store, logger, machine.ColdCompactionCh())
			},
			func(cfg Config, logger logging.Logger, meterProvider metric.MeterProvider) (*wal.DefaultWAL, error) {
				return wal.New(cfg.RaftConfig.WalDir, logger.WithFields(map[string]any{
					"cmp": "wal",
				}), meterProvider.Meter("wal"))
			},
			func(cfg Config, logger logging.Logger) (*spool.Default, error) {
				return spool.NewDefault(spool.DefaultSpoolConfig{
					Dir:             filepath.Join(cfg.DataDir, "spool"),
					SegmentMaxBytes: cfg.SpoolSegmentMaxBytes,
					Logger: logger.WithFields(map[string]any{
						"cmp": "spool",
					}),
				})
			},
			func(cfg Config, transport *node.DefaultTransport) state.SnapshotFetcherProvider {
				return ctrl.GRPCSnapshotFetcherProvider(
					transport,
					cfg.SnapshotSyncConfig.Parallelism,
					cfg.SnapshotSyncConfig.RetryCount,
					cfg.SnapshotSyncConfig.FileRetryCount,
				)
			},
			node.NewLocalResponses,
			func(
				cfg node.NodeConfig,
				logger logging.Logger,
				store *dal.Store,
				meterProvider metric.MeterProvider,
				sp *spool.Default,
				w *wal.DefaultWAL,
				snapshotFetcherProvider state.SnapshotFetcherProvider,
				machine *state.Machine,
				recovery *state.Recovery,
				synchronizer *state.Synchronizer,
				membership *raftmembership.Membership,
				localResponses node.LocalResponses,
			) (*node.Applier, error) {
				return node.NewApplier(
					machine,
					recovery,
					synchronizer,
					sp,
					store,
					w,
					logger,
					meterProvider.Meter("raft.node"),
					cfg.CompactionMargin,
					cfg.ReplayBatchSize,
					snapshotFetcherProvider,
					membership.OnSnapshotInstalled,
					localResponses,
				)
			},
			// Recovery owns the Pebble read capability for boot/post-sync rehydrate.
			// The FSM Machine never holds this capability — the structural guarantee
			// of I1 depends on Recovery being the sole holder.
			//
			// We hydrate the Machine here (RecoverState) so the rest of the wiring
			// — applier, node, background workers — observes the FSM at its
			// recovered state. NewMachine deliberately skips this step so it does
			// not perform any Pebble read itself.
			func(machine *state.Machine, store *dal.Store) (*state.Recovery, error) {
				recovery := state.NewRecovery(machine, store)
				if err := recovery.RecoverState(); err != nil {
					return nil, fmt.Errorf("recovering FSM state at boot: %w", err)
				}

				return recovery, nil
			},
			// Synchronizer owns the IncomingRestoreFactory (prepare/activate/restore
			// sequence) and coordinates follower-sync from a leader checkpoint.
			func(machine *state.Machine, recovery *state.Recovery, store *dal.Store) *state.Synchronizer {
				return state.NewSynchronizer(machine, recovery, dal.NewIncomingRestoreFactory(store))
			},
			// PeerStore persists Raft cluster membership in Pebble under
			// [ZoneGlobal][SubGlobPeers] (EN-1413). Membership wraps it
			// with the in-memory cache, owns the transport / service-pool
			// wiring, and exposes the OnSnapshotInstalled /
			// WriteConfChange callbacks injected into Applier and Machine
			// via constructor.
			raftmembership.NewPeerStore,
			fx.Annotate(func(
				store *raftmembership.PeerStore,
				defaultTransport *node.DefaultTransport,
				servicePool *transport.ConnectionPool,
				cfg node.NodeConfig,
				logger logging.Logger,
			) (*raftmembership.Membership, error) {
				return raftmembership.NewMembership(store, defaultTransport, servicePool, cfg.NodeID, cfg.AdvertiseAddr, cfg.ServiceAdvertiseAddr, cfg.InstanceID, logger)
			}, fx.ParamTags(``, ``, `name:"service"`, ``, ``)),
			// Provide events.Proposer from the Raft node (used by event emitter to replicate cursor).
			// Events go through Builder.Run, which already holds the IndexTracker
			// mutex around its proposer.Propose call. Wrapping the node in a
			// LockedProposer would re-lock the same mutex (sync.Mutex is
			// non-reentrant) and deadlock the emitter.
			func(n *node.Node) events.Proposer {
				return n
			},
			func(cfg node.NodeConfig, store *dal.Store, meterProvider metric.MeterProvider) (*cache.Cache, error) {
				threshold := cfg.RotationThreshold
				if clusterState, err := query.ReadClusterState(store); err == nil && clusterState != nil {
					threshold = clusterState.GetConfig().GetRotationThreshold()
				}

				return cache.New(threshold, meterProvider.Meter("cache"))
			},
			func(cfg Config, meterProvider metric.MeterProvider) *bloom.FilterSet {
				return bloom.NewFilterSet(cfg.BloomConfig, meterProvider.Meter("bloom"))
			},
			func(cfg Config, n *node.Node, c *cache.Cache, attrs *attributes.Attributes, store *dal.Store, bloomFilters *bloom.FilterSet, logger logging.Logger) *plan.Builder {
				return plan.NewBuilder(n.IndexTracker(), c, attrs, store, bloomFilters, logger, cfg.MaxExecutionPlanSize)
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
				bloomFilters *bloom.FilterSet,
				membership *raftmembership.Membership,
			) (*state.Machine, error) {
				machineStart := time.Now()

				idempotencyTTLMicros := uint64(cfg.IdempotencyTTL.Microseconds())

				// Fan-out: Machine emits to a single Notifier; FanOut dispatches
				// to the per-consumer Notifications (events, mirror, index).
				fanOut := signal.NewFanOut(eventNotifications, mirrorNotifications, indexNotifications)

				// Sub-objects built in-line so NewMachine receives them pre-built.
				registry := state.NewStateRegistry(c, attrs, idempotencyTTLMicros)
				snapshotter := state.NewCacheSnapshotter(logger, registry, bloomFilters)

				m, err := state.NewMachine(
					logger,
					registry,
					snapshotter,
					store, // QueryCheckpoints
					dal.NewSentinelFactory(store, cfg.SentinelMode),
					meterProvider,
					ks,
					ss,
					fanOut,
					bloomFilters,
					cfg.ClusterID,
					cfg.NumscriptCacheSize,
					membership.WriteConfChange,
				)
				if err != nil {
					return nil, err
				}

				logger.WithFields(map[string]any{
					"duration": time.Since(machineStart).String(),
				}).Infof("FSM Machine created")

				return m, nil
			}, fx.ParamTags(``, ``, ``, ``, ``, ``, ``, ``, `name:"events"`, `name:"mirror"`, `name:"index"`, ``, ``)),
			func(
				params struct {
					fx.In

					NodeConfig     node.NodeConfig
					Logger         logging.Logger
					Transport      *node.DefaultTransport
					MeterProvider  metric.MeterProvider
					WAL            *wal.DefaultWAL
					Applier        *node.Applier
					Machine        *state.Machine
					Recovery       *state.Recovery
					Synchronizer   *state.Synchronizer
					Membership     *raftmembership.Membership
					LocalResponses node.LocalResponses
				},
			) (nodeProvideResult, error) {
				// Check WAL emptiness before NewNode writes the initial snapshot.
				snapshot, err := params.WAL.Snapshot()
				if err != nil {
					return nodeProvideResult{}, fmt.Errorf("reading WAL snapshot: %w", err)
				}

				freshStart := walFreshStart(len(snapshot.GetMetadata().GetConfState().GetVoters()) == 0)
				params.Logger.WithFields(map[string]any{
					"freshStart":    freshStart,
					"walVoters":     snapshot.GetMetadata().GetConfState().GetVoters(),
					"walLearners":   snapshot.GetMetadata().GetConfState().GetLearners(),
					"snapshotIndex": snapshot.GetMetadata().GetIndex(),
					"snapshotTerm":  snapshot.GetMetadata().GetTerm(),
				}).Infof("WAL fresh start detection")

				n, err := node.NewNode(
					params.NodeConfig,
					params.Transport,
					params.Applier,
					params.Logger,
					params.MeterProvider.Meter("raft.node"),
					params.WAL,
					params.Machine,
					params.Recovery,
					params.Synchronizer,
					params.Membership,
					params.LocalResponses,
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
			buildResponseSigner,
			func(cfg Config) (node.NodeConfig, error) {
				cfg.RaftConfig.DataDir = cfg.DataDir
				cfg.RaftConfig.ServiceAdvertiseAddr = cfg.ServiceAdvertiseAddr()
				cfg.RaftConfig.SetDefaults()

				// EN-1045: establish this peer's identity UUID before any
				// membership plumbing runs. First boot generates and
				// persists it in INSTANCE_ID under WalDir; later boots
				// return the same value.
				instanceID, err := wal.EnsureInstanceID(cfg.RaftConfig.WalDir)
				if err != nil {
					return node.NodeConfig{}, fmt.Errorf("ensuring instance id: %w", err)
				}

				cfg.RaftConfig.InstanceID = instanceID

				return cfg.RaftConfig, nil
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
			func(cfg Config, lc fx.Lifecycle, logger logging.Logger) (transport.TLSPolicy, error) {
				tlsCfg, reloader, err := ClientTLSConfig(cfg.TLSConfig)
				if err != nil {
					return transport.TLSPolicy{}, err
				}

				RegisterCertReloaderLifecycle(lc, reloader, logger)

				return transport.TLSPolicy{
					TLSConfig: tlsCfg,
					Strict:    cfg.TLSConfig.Mode != TLSModeOptional,
				}, nil
			},
			// RaftServer for internal inter-node communication (Raft transport + Snapshot)
			func(cfg Config, lc fx.Lifecycle, logger logging.Logger) (*grpcadp.RaftServer, error) {
				_, raftPort, err := net.SplitHostPort(cfg.RaftConfig.BindAddr)
				if err != nil {
					return nil, fmt.Errorf("invalid bind address format: %w", err)
				}

				port, err := strconv.Atoi(raftPort)
				if err != nil {
					return nil, fmt.Errorf("invalid port in bind address: %w", err)
				}

				tlsCfg, reloader, err := ServerTLSConfig(cfg.TLSConfig)
				if err != nil {
					return nil, fmt.Errorf("loading TLS config for raft server: %w", err)
				}

				RegisterCertReloaderLifecycle(lc, reloader, logger)

				return grpcadp.NewRaftServer(port, logger, tlsCfg, cfg.TLSConfig.Mode.AllowsPlaintext(), cfg.ClusterSecret)
			},
			// ServiceServer for external client-facing API
			func(cfg Config, lc fx.Lifecycle, logger logging.Logger) (*grpcadp.ServiceServer, error) {
				tlsCfg, reloader, err := ServerTLSConfig(cfg.TLSConfig)
				if err != nil {
					return nil, fmt.Errorf("loading TLS config for service server: %w", err)
				}

				RegisterCertReloaderLifecycle(lc, reloader, logger)

				return grpcadp.NewServiceServer("", cfg.GRPCPort, logger, cfg.Debug, cfg.GRPCSlowThreshold, tlsCfg, cfg.TLSConfig.Mode.AllowsPlaintext())
			},
			// Provide a single AuthConfig used by gRPC and HTTP handlers.
			fx.Annotate(buildAuthConfig, fx.ParamTags(``, ``, `optional:"true"`)),
			fx.Annotate(func(cfg Config, logger logging.Logger, c ctrl.Controller, localCtrl *ctrl.DefaultController, s *dal.Store, rs *readstore.Store, attrs *attributes.Attributes, ss *state.SharedState, signer *receipt.Signer, respSigner *signing.ResponseSigner, authCfg internalauth.AuthConfig, meterProvider metric.MeterProvider, n *node.Node, servicePool *transport.ConnectionPool, info version.Info) servicepb.BucketServiceServer {
				return grpcadp.NewBucketServiceServer(logger, c, localCtrl, s, rs, attrs, ss, signer, respSigner, authCfg, cfg.QueryProfileThreshold, cfg.ClusterID, cfg.IdempotencyTTL, meterProvider, n, servicePool, info)
			}, fx.ParamTags(``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, `name:"service"`, ``)),
			func(cfg Config, logger logging.Logger, s *dal.Store, fsm *state.Machine) snapshotpb.SnapshotServiceServer {
				return grpcadp.NewSnapshotServiceServer(logger, s, cfg.SnapshotSyncConfig.SessionTTL, fsm.WaitForApplied)
			},
			func(cfg Config, meterProvider metric.MeterProvider) *diskusage.Collector {
				return diskusage.NewCollector(
					cfg.RaftConfig.WalDir,
					cfg.DataDir,
					5*time.Second,
					meterProvider.Meter("storage"),
				)
			},
			fx.Annotate(func(n *node.Node, raftTransport *node.DefaultTransport, servicePool *transport.ConnectionPool, infraMembership *raftmembership.Membership, cfg Config, logger logging.Logger) *membership.Service {
				return membership.NewService(
					n, raftTransport, servicePool, infraMembership, logger,
					cfg.RaftConfig.AdvertiseAddr,
					cfg.ServiceAdvertiseAddr(),
				)
			}, fx.ParamTags(``, ``, `name:"service"`, ``, ``, ``)),
			func(builder *plan.Builder, n *node.Node, store *dal.Store, cfg Config, logger logging.Logger) *backupapp.Orchestrator {
				return backupapp.NewOrchestrator(newBackupProposer(builder, n), store, logger, n.GetNodeID(), backupapp.NewExecutorRegistry(), cfg.BackupMaxSegmentBytes)
			},
			func(builder *plan.Builder, n *node.Node, fsm *state.Machine, orchestrator *backupapp.Orchestrator, logger logging.Logger) *backupapp.Cleanup {
				return backupapp.NewCleanup(fsm.Registry.BackupJobs, newBackupProposer(builder, n), n, orchestrator.Registry(), logger)
			},
			fx.Annotate(func(n *node.Node, raftTransport *node.DefaultTransport, servicePool *transport.ConnectionPool, collector *diskusage.Collector, store *dal.Store, c *cache.Cache, ss *state.SharedState, ib *indexbuilder.Builder, rs *readstore.Store, adm ctrl.Admission, ms *membership.Service, bo *backupapp.Orchestrator, logger logging.Logger, cfg Config, authCfg internalauth.AuthConfig, info version.Info) clusterpb.ClusterServiceServer {
				return grpcadp.NewClusterServiceServer(n, raftTransport, servicePool, collector, store, c, ss, ib, rs, adm, ms, bo, logger,
					cfg.RaftConfig.AdvertiseAddr,
					cfg.ServiceAdvertiseAddr(),
					authCfg,
					cfg.ClusterID,
					info,
				)
			}, fx.ParamTags(``, ``, `name:"service"`, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``)),
			func(n *node.Node, raftTransport *node.DefaultTransport, ms *membership.Service, cfg Config, logger logging.Logger) clusterbootstrappb.ClusterBootstrapServiceServer {
				return grpcadp.NewClusterBootstrapServiceServer(
					n, raftTransport, ms, logger,
					cfg.ClusterID,
				)
			},
			fx.Annotate(func(n *node.Node, collector *diskusage.Collector, servicePool *transport.ConnectionPool, cfg Config, logger logging.Logger, meterProvider metric.MeterProvider) *clusterhealth.HealthChecker {
				return clusterhealth.NewHealthChecker(
					n, collector, servicePool,
					logger,
					cfg.HealthConfig.Interval,
					clusterhealth.Thresholds{
						WALBlock:   cfg.HealthConfig.WALThreshold,
						WALResume:  cfg.HealthConfig.WALResumeThreshold,
						DataBlock:  cfg.HealthConfig.DataThreshold,
						DataResume: cfg.HealthConfig.DataResumeThreshold,
					},
					cfg.HealthConfig.ClockSkewThreshold,
					meterProvider.Meter("health"),
				)
			}, fx.ParamTags(``, ``, `name:"service"`, ``, ``, ``)),
			keystore.NewKeyStore,
			state.NewSharedState,
			fx.Annotate(signal.NewNotifications, fx.ResultTags(`name:"events"`)),
			fx.Annotate(events.NewManager, fx.ParamTags(``, ``, ``, ``, ``, `name:"events"`)),
			fx.Annotate(signal.NewNotifications, fx.ResultTags(`name:"mirror"`)),
			fx.Annotate(signal.NewNotifications, fx.ResultTags(`name:"index"`)),
			fx.Annotate(func(store *dal.Store, proposer mirror.Proposer, builder *plan.Builder, logger logging.Logger, notifications *signal.Notifications, meterProvider metric.MeterProvider, cfg Config) *mirror.Manager {
				return mirror.NewManager(store, proposer, builder, logger, notifications, meterProvider, cfg.MirrorMaxBatchSize)
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
			func(store *dal.Store, rs *readstore.Store, attrs *attributes.Attributes, logger logging.Logger, meterProvider metric.MeterProvider, cfg Config) *indexbuilder.Builder {
				return indexbuilder.NewBuilder(store, rs, attrs, logger, meterProvider.Meter("index.builder"), cfg.ReadIndexConfig.BatchSize)
			},
			// Audit indexer — tails the Audit zone to populate the readstore audit index.
			func(store *dal.Store, rs *readstore.Store, logger logging.Logger, meterProvider metric.MeterProvider, cfg Config) *auditindexer.Indexer {
				return auditindexer.New(
					auditindexer.Config{
						BatchSize: cfg.AuditIndexConfig.BatchSize,
						Disabled:  cfg.AuditIndexConfig.Disabled,
					},
					store, rs, logger, meterProvider.Meter("audit.index"),
				)
			},
			httpcompat.NewServer,
			func(cfg Config, logger logging.Logger, backend httpcompat.Backend, authCfg internalauth.AuthConfig, info version.Info) http.Handler {
				return httpcompat.NewHandler(logger, backend, authCfg, info)
			},
			func(node *node.Node, ctrl ctrl.Controller) httpcompat.Backend {
				return httpcompat.NewDefaultBackend(node, ctrl)
			},
			func(
				cfg Config,
				node *node.Node,
				store *dal.Store,
				logger logging.Logger,
				builder *plan.Builder,
				meterProvider metric.MeterProvider,
				hc *clusterhealth.HealthChecker,
				ks *keystore.KeyStore,
				ss *state.SharedState,
				receiptSigner *receipt.Signer,
				attrs *attributes.Attributes,
				authCfg internalauth.AuthConfig,
			) ctrl.Admission {
				var opts []func(*admission.Admission)
				if cfg.AdmissionMetrics {
					opts = append(opts, admission.WithMetrics())
				}

				if receiptSigner != nil {
					opts = append(opts, admission.WithReceiptSigner(receiptSigner))
				}

				if cfg.ColdStorageConfig.Driver != "none" {
					opts = append(opts, admission.WithColdStorageEnabled())
				}

				if authCfg.Enabled {
					opts = append(opts, admission.WithAuthEnabled())
				}

				return admission.NewAdmission(
					store,
					logger,
					node,
					builder,
					meterProvider,
					hc,
					ks,
					ss,
					attrs,
					numscript.NewNumscriptCache(cfg.NumscriptCacheSize),
					node.WaitLeaderReady,
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
				return state.NewSealer(logger, store, attrs, machine.SealRequestCh(), func(chapterID uint64, sealingHash, stateHash []byte) error {
					_, err := admissionHandler.Admit(internalauth.WithSystemActor(context.Background(), commands.ComponentChapterSealer), servicepb.UnsignedApplyRequest("", &servicepb.Request{
						Type: &servicepb.Request_SealChapter{
							SealChapter: &servicepb.SealChapterRequest{
								ChapterId:   chapterID,
								SealingHash: sealingHash,
								StateHash:   stateHash,
							},
						},
					}))

					return err
				}, raftNode.IsLeader, machine)
			},
			func(
				logger logging.Logger,
				machine *state.Machine,
				admissionHandler ctrl.Admission,
				raftNode *node.Node,
			) *state.ChapterScheduler {
				return state.NewChapterScheduler(
					logger,
					raftNode.IsLeader,
					machine.ChapterSchedule,
					func() error {
						_, err := admissionHandler.Admit(internalauth.WithSystemActor(context.Background(), commands.ComponentChapterScheduler), servicepb.UnsignedApplyRequest("", &servicepb.Request{
							Type: &servicepb.Request_CloseChapter{
								CloseChapter: &servicepb.CloseChapterRequest{},
							},
						}))

						return err
					},
					machine.ScheduleChanged(),
				)
			},
			func(
				logger logging.Logger,
				machine *state.Machine,
				admissionHandler ctrl.Admission,
				raftNode *node.Node,
			) *state.QueryCheckpointScheduler {
				return state.NewQueryCheckpointScheduler(
					logger,
					raftNode.IsLeader,
					machine.QueryCheckpointSchedule,
					func() error {
						_, err := admissionHandler.Admit(internalauth.WithSystemActor(context.Background(), commands.ComponentQueryCheckpoint), servicepb.UnsignedApplyRequest("", &servicepb.Request{
							Type: &servicepb.Request_CreateQueryCheckpoint{
								CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{},
							},
						}))

						return err
					},
					machine.QueryCheckpointScheduleChanged(),
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
				coldReader *coldstorage.ColdReader,
				meterProvider metric.MeterProvider,
			) (ctrl.Controller, *ctrl.DefaultController) {
				defaultCtrl := ctrl.NewDefaultController(admission, store, logger, attrs, rs, coldReader, meterProvider.Meter("ctrl"))

				return NewRoutedController(
					defaultCtrl,
					raftNode,
					servicePool,
				), defaultCtrl
			}, fx.ParamTags(``, `name:"service"`, ``, ``, ``, ``, ``, `optional:"true"`, ``)),
			func(serviceServer *grpcadp.ServiceServer, n *node.Node) *clusterhealth.GRPCHealthUpdater {
				hs := health.NewServer()
				healthpb.RegisterHealthServer(serviceServer.GetServer(), hs)

				return clusterhealth.NewGRPCHealthUpdater(n, hs)
			},
		),
		fx.Decorate(func(
			params struct {
				fx.In

				Handler        http.Handler
				MeterProvider  *sdkmetric.MeterProvider      `optional:"true"`
				Exporter       *otlpmetrics.InMemoryExporter `optional:"true"`
				FlightRecorder *flightrecorder.Recorder      `optional:"true"`
			},
		) http.Handler {
			mux := http.NewServeMux()
			mux.Handle("/", params.Handler)

			if params.Exporter != nil && params.MeterProvider != nil {
				mux.Handle("/metrics", otlpmetrics.NewInMemoryExporterHandler(params.MeterProvider, params.Exporter))
			}

			if params.FlightRecorder != nil {
				mux.Handle("/debug/flight-recorder", flightrecorder.SnapshotHandler(params.FlightRecorder))
			}

			return mux
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
			// Bloom-rebuild dispatcher: the Machine signals on its
			// BloomRebuildCh when a cluster-config change requires a rebuild;
			// Recovery (which owns the Pebble reader) consumes that signal and
			// invokes StartAsyncBloomPopulate. Without this dispatcher, the
			// Machine would have to hold a reader itself, which would
			// re-introduce the I1 hot-path read leak. Registered here in the
			// main Module so it is wired regardless of cold-storage driver.
			func(lc fx.Lifecycle, recovery *state.Recovery) {
				stop := make(chan struct{})
				lc.Append(fx.Hook{
					OnStart: func(_ context.Context) error {
						go recovery.DispatchBloomRebuilds(stop)

						return nil
					},
					OnStop: func(_ context.Context) error {
						close(stop)

						return nil
					},
				})
			},
			// Join preflight (EN-1436): register this node as a learner on the
			// leader and fail fast on stale raft Progress. Registered FIRST
			// among the Raft startup hooks so its OnStart runs (and, on stale
			// Progress, aborts boot) BEFORE any hook that lets inbound Raft
			// traffic reach rawNode.Step — the Raft gRPC server start +
			// membership.Start() and node.Run() below. fx runs OnStart hooks in
			// Append order; keeping this closure ahead of those enforces the
			// ordering the fail-fast depends on. See joinPreflightHook for the
			// inbound-vs-outbound / no-deadlock rationale.
			func(
				lc fx.Lifecycle,
				cfg Config,
				logger logging.Logger,
			) {
				if !shouldRunJoinPreflight(cfg, logger) {
					return
				}

				lc.Append(joinPreflightHook(cfg, logger))
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
			func(raftServer *grpcadp.RaftServer, bootstrapServer clusterbootstrappb.ClusterBootstrapServiceServer) error {
				grpcadp.RegisterClusterBootstrapService(raftServer.GetServer(), bootstrapServer)

				return nil
			},
			func(lc fx.Lifecycle, raftServer *grpcadp.RaftServer, snapshotServiceServer snapshotpb.SnapshotServiceServer) error {
				grpcadp.RegisterSnapshotService(raftServer.GetServer(), snapshotServiceServer)
				lc.Append(fx.Hook{
					OnStop: func(_ context.Context) error {
						grpcadp.StopSnapshotService(snapshotServiceServer)

						return nil
					},
				})

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
			// Backup cleanup loop: leader-only, fails stale jobs so a
			// crashed executor doesn't permanently lock the destination.
			// Run is leader-aware itself so this just needs lifecycle.
			func(lc fx.Lifecycle, cleanup *backupapp.Cleanup, logger logging.Logger) {
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})

				lc.Append(fx.Hook{
					OnStart: func(_ context.Context) error {
						go func() {
							defer close(done)
							cleanup.Run(ctx)
						}()

						return nil
					},
					OnStop: func(stopCtx context.Context) error {
						cancel()

						// Wait for the loop to drain a propose-in-flight rather
						// than leak it. Bounded by the fx Stop deadline so we
						// never block forever.
						select {
						case <-done:
						case <-stopCtx.Done():
							logger.Infof("backup cleanup loop did not exit before stop deadline")
						}

						return nil
					},
				})
			},
			// Start Raft server (internal). Peer wiring (transport +
			// service pool) is owned by Membership; the initial wire
			// runs here in OnStart AFTER the local Raft gRPC server is
			// listening — that way ConnectionPool.AddPeer's optional-
			// TLS probe against a remote pod has a fair chance to
			// succeed. Post-Start, runtime Set / Remove wire inline.
			// (EN-1413)
			func(
				lc fx.Lifecycle,
				raftServer *grpcadp.RaftServer,
				logger logging.Logger,
				membership *raftmembership.Membership,
			) {
				var waitRaft func()

				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						logger.Infof("Starting Raft gRPC server")

						listening := make(chan struct{})

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

						membership.Start()

						return nil
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Stopping Raft gRPC server")

						err := raftServer.Stop()
						waitRaft()

						return err
					},
				})
			},
			// Wire Observer: handle LeadershipChange and LeaderReady events.
			// ConfChange events are no longer dispatched here — Membership
			// owns the transport / service-pool wiring and reacts directly
			// to peer mutations (EN-1413).
			func(
				n *node.Node,
				store *dal.Store,
				builder *plan.Builder,
				cfg Config,
				logger logging.Logger,
				eventsManager *events.Manager,
				mirrorManager *mirror.Manager,
				backupOrchestrator *backupapp.Orchestrator,
			) {
				n.SetObserver(node.NewObserver(func(event any) {
					switch e := event.(type) {
					case node.LeadershipChangeEvent:
						// The backup orchestrator's OnLeadershipChange is cheap
						// (it just swaps a context) and must observe leadership
						// transitions IN ORDER: a stale goroutine landing
						// OnLeadershipChange(false) after a newer (true) would
						// leave leaderCtx cancelled while node.IsLeader()==true,
						// failing every subsequent Backup RPC with ctx
						// cancellation. Apply it inline.
						backupOrchestrator.OnLeadershipChange(e.IsLeader)

						// The events / mirror reconcile is dispatched off the
						// observer thread because it does a full Pebble
						// attribute scan that can take minutes; running it
						// synchronously would stall processReady and the
						// readiness probe.
						go handleLeadershipChangeEvent(e, eventsManager, mirrorManager, logger)
					case node.LeaderReadyEvent:
						proposeClusterConfigIfNeeded(n, builder, store, cfg, logger)
					default:
						logger.Errorf("Unknown observer event type: %T", event)
					}
				}))
			},
			func(lc fx.Lifecycle, node *node.Node, defaultTransport *node.DefaultTransport, logger logging.Logger) (*node.Node, error) {
				var cancelRun context.CancelFunc

				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						ready := make(chan struct{})

						// Use a dedicated context for node.Run that survives
						// the OnStart return (unlike ctx which expires). On
						// startup failure we cancel it to abandon the goroutine;
						// on graceful shutdown node.Stop is the signal, NOT this
						// cancel — see the OnStop hook below for the rationale.
						var runCtx context.Context
						runCtx, cancelRun = context.WithCancel(context.Background())

						otlplogs.Go(func() {
							err := node.Run(runCtx, ready)
							if err != nil {
								panic(err)
							}
						}, logger)

						select {
						case <-ctx.Done():
							cancelRun()

							return ctx.Err()
						case <-ready:
							logger.Infof("Raft cluster started successfully")

							return nil
						}
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Shutting down raft cluster")

						// Do NOT cancel peer connections here. node.Stop's
						// first move is tryTransferLeadershipBeforeShutdown,
						// which needs the priority send queue of the elected
						// transferee to still be wired up so the MsgTimeoutNow
						// reaches it. Killing peer loops up-front broke the
						// transfer and forced the cluster through a full
						// election timeout on every graceful shutdown (#314).
						//
						// The transport's own fx OnStop runs AFTER this hook
						// (fx invokes OnStop in reverse registration order) and
						// already calls CancelPeerConnections inside t.Stop().
						//
						// Do NOT cancel runCtx here either. node.Run's outer
						// select watches stopChannel and tasks.err() — it does
						// not watch ctx. The tasks (applier.Run, processReadies)
						// likewise select only on their stop channel. Cancelling
						// runCtx would only propagate into the FSM calls those
						// tasks make (PrepareEntries, CommitPreparedBatch,
						// InstallSnapshot) and cause them to return
						// context.Canceled mid-batch — which surfaces as a "task
						// pool error" from Node.Run, panics the bootstrap
						// goroutine, and crashes the process mid-shutdown
						// instead of returning a clean nil (#345).
						// Defer the cancel so it runs on EVERY return path,
						// including the error path below. If node.Stop returns
						// ctx.Err() (e.g. fx stop timeout expired during the
						// leadership transfer or stopChannel handshake), the
						// Run goroutine is still alive and waiting; without
						// this cancel the goroutine would outlive OnStop while
						// downstream fx hooks tear down transport and Pebble
						// underneath it. The cancel propagates into the tasks'
						// FSM calls (PrepareEntries / CommitPreparedBatch /
						// InstallSnapshot) so the bootstrap goroutine exits
						// via a logged task-pool error rather than racing with
						// concurrent infrastructure teardown.
						defer cancelRun()

						err := node.Stop(ctx)
						if err != nil {
							return fmt.Errorf("shutting down raft cluster: %w", err)
						}

						logger.Infof("Raft cluster stopped successfully")

						return nil
					},
				})

				return node, nil
			},
			// Start Service server (external).
			// Registered AFTER the Raft node so that fx stops it BEFORE the node
			// (fx runs OnStop in reverse order). This ensures the gRPC server
			// stops accepting client requests before the Raft node begins its
			// shutdown sequence, preventing requests from being processed while
			// the node is draining.
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
			// Join mode preflight is registered EARLIER (before the Raft
			// transport/server/node startup hooks) so it runs before any inbound
			// Raft traffic can be stepped — see the joinPreflightHook closure
			// above and its doc comment for the EN-1436 ordering rationale.
			func(lc fx.Lifecycle, cfg Config, handler http.Handler) {
				lc.Append(transportfx.FXHook(httpserver.NewHook(handler,
					httpserver.WithAddress(fmt.Sprintf(":%d", cfg.HTTPPort)),
				)))
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
			func(lc fx.Lifecycle, scheduler *state.ChapterScheduler) {
				lc.Append(worker.FxHook(scheduler))
			},
			func(lc fx.Lifecycle, cfg Config, logger logging.Logger, raftNode *node.Node, store *dal.Store, machine *state.Machine, builder *plan.Builder) {
				if cfg.IdempotencyTTL > 0 && cfg.IdempotencyEvictionInterval > 0 {
					scheduler := state.NewIdempotencyEvictionScheduler(
						logger,
						raftNode.IsLeader,
						func(ctx context.Context, cutoffMicros uint64, lastScannedTimeIndexKey []byte, pebbleKeyHashes [][]byte) {
							proposal := commands.NewCommand()
							proposal.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentIdempotencyEvict)
							proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
								Kind: &raftcmdpb.TechnicalUpdate_IdempotencyEviction{
									IdempotencyEviction: &raftcmdpb.IdempotencyEviction{
										CutoffMicros:            cutoffMicros,
										PebbleKeyHashes:         pebbleKeyHashes,
										LastScannedTimeIndexKey: lastScannedTimeIndexKey,
									},
								},
							}}
							// ctx comes from the scheduler's loop and is
							// cancelled by Stop(). No bounded timeout: a
							// timeout firing after Raft has accepted the
							// proposal would force a retry on the next tick
							// with the same Pebble main-key hashes; the
							// applied FSM uses SingleDelete, whose
							// write-once/delete-once contract forbids
							// re-deleting an already-deleted key.
							//
							// applyIdempotencyEviction works through
							// Registry.Idempotency.Evict (no cache-keyed
							// Registry.Get); no preload needed. One
							// WriteOperation with nil Coverage so the runner
							// takes the fast path.
							operations := []plan.WriteOperation{{
								SetCoverage: func(bits []byte) {
									proposal.GetTechnicalUpdates()[0].CoverageBits = bits
								},
							}}

							if err := proposeTechnical(ctx, builder, raftNode, proposal, operations); err != nil {
								logger.Errorf("Failed to propose idempotency eviction: %v", err)
							}
						},
						store,
						machine.Registry.Idempotency,
						cfg.IdempotencyEvictionInterval,
						cfg.IdempotencyTTL,
					)
					lc.Append(fx.Hook{
						OnStart: func(_ context.Context) error {
							scheduler.Start()

							return nil
						},
						OnStop: func(_ context.Context) error {
							scheduler.Stop()

							return nil
						},
					})
				}
			},
			func(lc fx.Lifecycle, scheduler *state.QueryCheckpointScheduler) {
				lc.Append(worker.FxHook(scheduler))
			},
			func(lc fx.Lifecycle, machine *state.Machine) {
				lc.Append(fx.Hook{
					OnStop: func(_ context.Context) error {
						machine.Close()

						return nil
					},
				})
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
			fx.Annotate(func(lc fx.Lifecycle, indexBuilder *indexbuilder.Builder, notifications *signal.Notifications) {
				indexBuilder.SetNotifications(notifications)
				lc.Append(worker.FxHook(indexBuilder))
			}, fx.ParamTags(``, ``, `name:"index"`)),
			// Start and stop the audit indexer.
			func(lc fx.Lifecycle, auditIndexer *auditindexer.Indexer) {
				lc.Append(worker.FxHook(auditIndexer))
			},
		),
	)
}

// JoinAuthError is returned when a joining node is rejected by the target
// cluster's inter-node RaftServer with codes.Unauthenticated. This is always
// a cluster-secret misconfiguration (EN-1080): the join flow carries no user
// JWT, so the only credential in play is the shared cluster secret sent as a
// Bearer token. The error is fatal and never retried — looping on the same
// (mis)configuration cannot succeed.
//
// It covers both inter-node join RPCs: peer discovery
// (ClusterBootstrapService.GetPeers, before the fx app starts, where PeerID
// is not yet known and stays 0) and learner registration
// (ClusterBootstrapService.JoinAsLearner, which carries the target PeerID).
type JoinAuthError struct {
	// PeerID identifies the rejecting cluster member. It is 0 during peer
	// discovery, where the joining node only knows the --join address.
	PeerID      uint64
	PeerAddress string
	// HasSecret reports whether this node was started with --cluster-secret.
	// It drives the actionable hint: a missing secret vs a mismatched one.
	HasSecret bool
	// Detail is the raw gRPC status message from the target for diagnostics.
	Detail string
}

func (e *JoinAuthError) Error() string {
	var hint string
	if e.HasSecret {
		hint = "this node was started with --cluster-secret, but the target cluster rejected it: " +
			"verify the secret matches the value configured on the existing cluster nodes"
	} else {
		hint = "this node was started without --cluster-secret, but the target cluster requires one: " +
			"set --cluster-secret to the value configured on the existing cluster nodes (and --tls-mode, which --cluster-secret requires)"
	}

	target := e.PeerAddress
	if e.PeerID != 0 {
		target = fmt.Sprintf("peer %d (%s)", e.PeerID, e.PeerAddress)
	}

	return fmt.Sprintf(
		"cluster join rejected by %s: inter-node authentication failed (%s); %s",
		target, e.Detail, hint,
	)
}

// shouldRunJoinPreflight reports whether the join preflight (tryAddLearner)
// must run for this boot. It runs only for a node that is joining an existing
// cluster and has not yet recorded a successful join:
//
//   - Bootstrap nodes and nodes with no configured peers never join — they
//     ARE the seed of the cluster.
//   - A node whose WAL already carries the CLUSTER_JOINED marker has joined on
//     a previous boot; re-running tryAddLearner would propose a redundant
//     AddLearner for a node that is already a voter, racing leadership state.
//     The operator's StatefulSet entrypoint already gates --join on the same
//     marker; this check makes the safety hold regardless of how the binary is
//     invoked (e.g. an E2E framework that reuses instruments across restarts).
func shouldRunJoinPreflight(cfg Config, logger logging.Logger) bool {
	if cfg.RaftConfig.Bootstrap || len(cfg.RaftConfig.Peers) == 0 {
		return false
	}

	if wal.IsClusterJoined(cfg.RaftConfig.WalDir) {
		logger.Infof("CLUSTER_JOINED marker present, skipping learner registration")

		return false
	}

	return true
}

// joinPreflightHook builds the OnStart hook that registers this node as a
// learner on the leader (and fails fast on stale raft Progress, EN-1436).
//
// EN-1436 ordering (flemzord review on #1478): this hook MUST run BEFORE the
// node starts accepting/stepping inbound Raft traffic — i.e. before the Raft
// gRPC transport server begins serving, before membership.Start() wires the
// leader connection, and before node.Run() drains the recv queues into
// rawNode.Step. Otherwise, once the leader can reach this pod, it may deliver a
// stale MsgApp/heartbeat whose Commit points past our (empty) log and trip
// etcd-raft's "tocommit out of range" panic BEFORE this preflight returns the
// STALE_RAFT_PROGRESS fail-fast — losing exactly the race the fail-fast exists
// to prevent. Registering this hook earlier in the fx lifecycle guarantees its
// OnStart runs (and, on stale Progress, aborts boot) before any inbound Raft
// message can be stepped.
//
// This does NOT deadlock: the preflight reaches the leader over an OUTBOUND,
// short-lived gRPC client it dials itself (see tryAddLearner) — it depends on
// neither the local Raft gRPC server (inbound serving) nor the transport
// connection pool that membership.Start() wires. Only INBOUND stepping is
// gated behind the preflight; OUTBOUND dialing is always available because it
// is self-contained. Gating inbound-serving on an outbound-only preflight is
// therefore safe.
func joinPreflightHook(cfg Config, logger logging.Logger) fx.Hook {
	return fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.WithFields(map[string]any{
				"nodeID":         cfg.RaftConfig.NodeID,
				"raftAddress":    cfg.RaftConfig.AdvertiseAddr,
				"serviceAddress": cfg.ServiceAdvertiseAddr(),
			}).Infof("Join mode: requesting peer to add this node as learner (preflight, before Raft traffic)")

			return tryAddLearner(ctx, cfg, cfg.TLSConfig, logger)
		},
	}
}

// tryAddLearner attempts to register this node as a learner on an existing
// cluster, using the inter-node ClusterBootstrapService exposed on the
// RaftServer. It retries on transient errors (Unavailable, no leader)
// with exponential backoff, and treats AlreadyExists as success
// (idempotent join).
//
// It dials each peer's Raft address directly with a short-lived
// connection rather than reusing raftTransport's connection pool: the
// pool is populated by a separate fx OnStart hook whose ordering
// relative to ours is not guaranteed, and a missing pool entry would
// otherwise leave us spinning on every retry.
func tryAddLearner(ctx context.Context, cfg Config, tlsCfg TLSConfig, logger logging.Logger) error {
	peers := cfg.RaftConfig.Peers

	// EN-1045: attach this peer's identity UUID to the join RPC so the
	// leader can persist it in the peer row and refuse a rejoin from a
	// blacklisted (nodeID, instanceID) tuple later on.
	instanceID, err := wal.EnsureInstanceID(cfg.RaftConfig.WalDir)
	if err != nil {
		return fmt.Errorf("ensuring instance id for JoinAsLearner: %w", err)
	}

	req := &clusterbootstrappb.JoinAsLearnerRequest{
		NodeId:         cfg.RaftConfig.NodeID,
		RaftAddress:    cfg.RaftConfig.AdvertiseAddr,
		ServiceAddress: cfg.ServiceAdvertiseAddr(),
		InstanceId:     instanceID,
	}

	creds, _, err := ClientTransportCredentials(tlsCfg)
	if err != nil {
		return fmt.Errorf("loading TLS credentials for learner registration: %w", err)
	}

	dialOpts := []ggrpc.DialOption{ggrpc.WithTransportCredentials(creds)}
	if cfg.ClusterSecret != "" {
		dialOpts = append(dialOpts, transport.BearerTokenDialOption(cfg.ClusterSecret))
	}

	backoff := 500 * time.Millisecond
	maxBackoff := 5 * time.Second

	if cfg.ClusterID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, node.MetadataKeyClusterID, cfg.ClusterID)
	}

	for {
		// Try each peer in order until one succeeds.
		for _, peer := range peers {
			conn, err := ggrpc.NewClient(peer.Address, dialOpts...)
			if err != nil {
				logger.WithFields(map[string]any{
					"peer":    peer.ID,
					"address": peer.Address,
					"error":   err,
				}).Infof("Failed to dial peer for learner registration, will retry")

				continue
			}

			client := clusterbootstrappb.NewClusterBootstrapServiceClient(conn)
			_, err = client.JoinAsLearner(ctx, req)
			_ = conn.Close()

			if err == nil {
				logger.WithFields(map[string]any{
					"peer": peer.ID,
				}).Infof("Successfully registered as learner on the cluster")

				if markErr := wal.MarkClusterJoined(cfg.RaftConfig.WalDir); markErr != nil {
					return fmt.Errorf("marking cluster joined after learner registration: %w", markErr)
				}

				return nil
			}

			st, ok := status.FromError(err)
			// EN-1436: the leader detected this nodeID in its raft Progress
			// with a non-zero Match but we (the caller) have no CLUSTER_JOINED
			// marker on our WAL. The leader's Match for us points at state we
			// don't have, and the next MsgApp/heartbeat would trigger
			// etcd-raft's "tocommit out of range" panic. Fail fast with the
			// operator-actionable server message rather than retry, mark, or
			// silently crash-loop. The server message already contains the
			// exact remediation command; surface it verbatim as the fatal
			// reason.
			//
			// Match on the STALE_RAFT_PROGRESS ErrorInfo reason, not on the
			// bare FailedPrecondition code: the removed-member blacklist
			// rejection (EN-1045) is also FailedPrecondition but needs
			// `forget-removed`, not `remove-node --force`. Conflating the two
			// would print misleading remediation. A blacklist rejection falls
			// through to the generic fatal handler below with its own message.
			if ok && st.Code() == codes.FailedPrecondition && isStaleRaftProgress(st) {
				logger.WithFields(map[string]any{
					"peer":   peer.ID,
					"nodeID": cfg.RaftConfig.NodeID,
					"reason": st.Message(),
				}).Errorf("STALE MEMBERSHIP on leader — operator action required to unblock rejoin")

				return fmt.Errorf("stale raft membership on the leader: %s", st.Message())
			}

			// AlreadyExists path kept for rolling-upgrade compatibility with
			// leaders that predate EN-1436's server-side fail-fast. Once every
			// live cluster is past that version, this branch can be dropped —
			// the invariant it silently patches (idempotent join treating
			// stale Progress as success) is exactly the bug EN-1436 fixes.
			if ok && st.Code() == codes.AlreadyExists {
				logger.WithFields(map[string]any{
					"nodeID": cfg.RaftConfig.NodeID,
				}).Infof("Already a cluster member, skipping learner registration (deprecated pre-EN-1436 semantics)")

				if markErr := wal.MarkClusterJoined(cfg.RaftConfig.WalDir); markErr != nil {
					return fmt.Errorf("marking cluster joined after AlreadyExists: %w", markErr)
				}

				return nil
			}

			// Unavailable is transient (no leader, node syncing, etc.) — retry.
			if ok && st.Code() == codes.Unavailable {
				logger.WithFields(map[string]any{
					"peer":  peer.ID,
					"error": err,
				}).Infof("JoinAsLearner unavailable, will retry")

				continue
			}

			// Unauthenticated is a hard configuration error, never transient:
			// the target cluster's RaftServer rejected our cluster-secret
			// bearer (missing, wrong, or malformed). Retrying with the same
			// (mis)configuration can only loop forever, so fail fast with an
			// actionable message instead of leaking the opaque gRPC status
			// ("missing authorization metadata on Raft RPC", etc.) up the
			// bootstrap chain. EN-1080.
			if ok && st.Code() == codes.Unauthenticated {
				return &JoinAuthError{
					PeerID:      peer.ID,
					PeerAddress: peer.Address,
					HasSecret:   cfg.ClusterSecret != "",
					Detail:      st.Message(),
				}
			}

			// Non-transient error — fatal.
			return fmt.Errorf("failed to register as learner via peer %d (%s): %w", peer.ID, peer.Address, err)
		}

		// All peers returned transient errors. Back off and retry.
		logger.WithFields(map[string]any{
			"backoff": backoff.String(),
		}).Infof("All peers unavailable, retrying after backoff")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, maxBackoff)
	}
}

// isStaleRaftProgress reports whether a FailedPrecondition status carries the
// EN-1436 STALE_RAFT_PROGRESS ErrorInfo reason, distinguishing a stale-raft-
// progress join rejection (remediation: `remove-node --force`) from the
// removed-member blacklist rejection (remediation: `forget-removed`), which is
// also FailedPrecondition. Falls back to false when no matching detail is
// present so unrelated FailedPrecondition responses keep their generic handling.
func isStaleRaftProgress(st *status.Status) bool {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == node.StaleRaftProgressReason {
			return true
		}
	}

	return false
}

// proposeClusterConfigIfNeeded reads the persisted cluster state from Pebble
// and proposes an update if the CLI-desired config differs. Called when the
// node becomes leader and the FSM is caught up (LeaderReadyEvent).
func proposeClusterConfigIfNeeded(n *node.Node, builder *plan.Builder, store *dal.Store, cfg Config, logger logging.Logger) {
	clusterState, _ := query.ReadClusterState(store)

	desiredCfg := cfg.BloomConfig
	desiredCfg.RotationThreshold = cfg.RaftConfig.RotationThreshold

	if clusterState != nil {
		persistedCfg := clusterState.GetConfig()

		if persistedCfg.GetRotationThreshold() == desiredCfg.GetRotationThreshold() &&
			persistedCfg.GetHashAlgorithm() == desiredCfg.GetHashAlgorithm() &&
			bloom.BloomConfigEqual(persistedCfg, desiredCfg) {
			return
		}
	}

	logger.Infof("Proposing cluster config update on leadership acquisition")

	proposal := commands.NewCommand()
	proposal.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterConfig)
	proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: desiredCfg},
	}}

	// Bounded timeout: the observer event handler runs without a
	// stop-derived context, and a shutdown / leadership loss landing
	// between Raft acceptance and FSM apply would otherwise pin this
	// goroutine forever on result.FSMFuture.WaitContext.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// applyClusterConfig reads cache-level configuration (Cache.GenerationThreshold,
	// Cache.Epoch) but no keyed Registry.X.Get; no preload needed.
	operations := []plan.WriteOperation{{
		SetCoverage: func(bits []byte) {
			proposal.GetTechnicalUpdates()[0].CoverageBits = bits
		},
	}}

	if err := proposeTechnical(ctx, builder, n, proposal, operations); err != nil {
		logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Failed to propose cluster config update")
	}
}

// buildResponseSigner returns the Ed25519 response signer for the gRPC
// service plane.
//
//   - If --response-signing-key is not set, returns (nil, nil) — response
//     signing is opt-in and absence is a legitimate configuration.
//   - If the file IS configured but unreadable / invalid, returns
//     (nil, err) so fx aborts startup. Returning nil silently here would
//     fail-open: every response goes unsigned, clients running
//     VerifyResponseSignatures lose the authenticity guarantee, and the
//     deployment error (wrong path, bad permissions, corrupt seed) goes
//     unnoticed apart from one log line (#325).
//
// Mirrors the buildAuthConfig contract for Ed25519 auth keys.
func buildResponseSigner(cfg Config, logger logging.Logger) (*signing.ResponseSigner, error) {
	if cfg.ResponseSigningKeyFile == "" {
		return nil, nil //nolint:nilnil // no signer configured is a legitimate state
	}

	seed, err := signing.LoadSeedFromFile(cfg.ResponseSigningKeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading response signing key from %q: %w", cfg.ResponseSigningKeyFile, err)
	}

	signer := signing.NewResponseSigner(seed)
	logger.WithFields(map[string]any{
		"key_id": signer.KeyID(),
	}).Infof("Response signing enabled")

	return signer, nil
}

// discoveryContext returns a context bounded by timeout when timeout > 0,
// or a background context with a no-op cancel when timeout <= 0 (preserving
// the legacy unbounded behavior for operators who opt out explicitly).
func discoveryContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.Background(), func() {}
	}

	return context.WithTimeout(context.Background(), timeout)
}

// TimeoutHTTPClient returns an *http.Client with Timeout set when timeout > 0,
// otherwise http.DefaultClient (which has no timeout). buildAuthConfig uses it
// to bound the JWKS reads performed by the OIDC remote keyset.
func TimeoutHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		return http.DefaultClient
	}

	return &http.Client{Timeout: timeout}
}

// buildAuthConfig constructs an AuthConfig from the server configuration and optional OIDC KeySet.
// If Ed25519 keys are configured, it creates a composite KeySet that handles both OIDC and EdDSA tokens.
// When auth is enabled with an issuer and no external KeySet is injected, it discovers the OIDC
// configuration and creates a remote KeySet automatically.
// Scope mapping is loaded from file, env var, or defaults to the backward-compatible mapping.
func buildAuthConfig(cfg Config, logger logging.Logger, oidcKeySet oidc.KeySet) (internalauth.AuthConfig, error) {
	authCfg := internalauth.AuthConfig{
		Enabled: cfg.AuthConfig.Enabled,
		Issuer:  cfg.AuthConfig.Issuer,
		Service: cfg.AuthConfig.Service,
	}

	// When auth is enabled and an issuer is configured but no external KeySet was injected,
	// discover the OIDC configuration and create a remote KeySet. Both the discovery
	// call and the keyset's JWKS reads are bounded by OIDCDiscoveryTimeout so a slow or
	// blackholed issuer cannot hang the process: the context deadline bounds
	// oidc.Discover, and the keyset's http.Client.Timeout bounds JWKS fetches.
	if oidcKeySet == nil && cfg.AuthConfig.Enabled && cfg.AuthConfig.Issuer != "" {
		ctx, cancel := discoveryContext(cfg.AuthConfig.OIDCDiscoveryTimeout)
		discovery, err := oidc.Discover(ctx, cfg.AuthConfig.Issuer, oidc.DiscoveryEndpoint)
		cancel()
		if err != nil {
			return authCfg, fmt.Errorf("discovering OIDC configuration for issuer %q: %w", cfg.AuthConfig.Issuer, err)
		}

		oidcKeySet = oidcclient.NewRemoteKeySet(TimeoutHTTPClient(cfg.AuthConfig.OIDCDiscoveryTimeout), discovery.JwksURI)

		logger.WithFields(map[string]any{
			"issuer":   cfg.AuthConfig.Issuer,
			"jwks_uri": discovery.JwksURI,
		}).Infof("OIDC remote keyset configured via discovery")
	}

	if cfg.AuthConfig.Ed25519KeysFile != "" {
		result, err := internalauth.LoadEd25519KeySet(cfg.AuthConfig.Ed25519KeysFile)
		if err != nil {
			return authCfg, fmt.Errorf("loading Ed25519 keys: %w", err)
		}

		authCfg.KeySet = internalauth.NewCompositeKeySet(result.KeySet, oidcKeySet)
		authCfg.Ed25519AllowedScopes = result.AllowedScopes
		authCfg.Ed25519GodKeys = result.GodKeys

		logger.WithFields(map[string]any{
			"keys_count": len(result.AllowedScopes),
			"god_keys":   len(result.GodKeys),
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

	if err := applyAnonymousScopes(scopeMapping, cfg.AuthConfig.AnonymousScopes, logger); err != nil {
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

// applyAnonymousScopes merges the --auth-anonymous-scopes CSV (if any) into
// the loaded scope mapping under the reserved "anonymous" key. Wildcards
// "*:read" / "*:write" are expanded to the matching granular scopes.
// An explicit "anonymous" entry already present in the mapping is overridden.
func applyAnonymousScopes(mapping internalauth.ScopeMapping, raw string, logger logging.Logger) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	scopes := make([]internalauth.Scope, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		if expanded, ok := internalauth.ExpandWildcardScope(p); ok {
			scopes = append(scopes, expanded...)

			continue
		}

		scope := internalauth.Scope(p)
		if _, ok := internalauth.AllGranularScopes[scope]; !ok {
			return fmt.Errorf("unknown granular scope %q in --auth-anonymous-scopes", p)
		}

		scopes = append(scopes, scope)
	}

	mapping[internalauth.ScopeMappingAnonymousKey] = scopes

	logger.WithFields(map[string]any{
		"scopes_count": len(scopes),
	}).Infof("Anonymous scopes configured (requests without a bearer token will receive these)")

	return nil
}

// handleLeadershipChangeEvent reconciles event emitter and mirror
// workers on leadership transitions. Runs in a goroutine — see the
// observer callback above for the dispatch and the reason
// (event/mirror reconcile can take minutes).
//
// The backup orchestrator's OnLeadershipChange is intentionally NOT
// called here: it must observe transitions in order and inline, so
// a leadership flap cannot interleave an old-(false) update behind
// a newer-(true) update. See the observer's LeadershipChangeEvent
// branch.
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
