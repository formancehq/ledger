package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/formancehq/go-libs/v3/httpclient"
	"github.com/formancehq/go-libs/v3/logging"
	ledgerclient "github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/jamiealquiza/tachymeter"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// Environment types
type EnvConfig struct {
	ClientID     string
	ClientSecret string
	LedgerURL    string
	AuthURL      string
}

type Env interface {
	Client() *ledgerclient.Formance
	Stop(ctx context.Context) error
}

type EnvFactory interface {
	Create() (Env, error)
}

type RemoteLedgerEnv struct {
	client *ledgerclient.Formance
}

func (r *RemoteLedgerEnv) Client() *ledgerclient.Formance {
	return r.client
}

func (r *RemoteLedgerEnv) Stop(_ context.Context) error {
	return nil
}

type RemoteLedgerEnvFactory struct {
	httpClient *http.Client
	ledgerURL  string
}

func (r *RemoteLedgerEnvFactory) Create() (Env, error) {
	return &RemoteLedgerEnv{
		client: ledgerclient.New(
			ledgerclient.WithClient(r.httpClient),
			ledgerclient.WithServerURL(r.ledgerURL),
		),
	}, nil
}

func getHTTPClient(config EnvConfig) *http.Client {

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxConnsPerHost:     100,
			MaxIdleConnsPerHost: 100,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	if os.Getenv("DEBUG") == "true" {
		httpClient.Transport = httpclient.NewDebugHTTPTransport(httpClient.Transport)
	}

	if config.ClientID != "" {
		httpClient = (&clientcredentials.Config{
			ClientID:     config.ClientID,
			ClientSecret: config.ClientSecret,
			TokenURL:     config.AuthURL + "/oauth/token",
			Scopes:       []string{"ledger:read", "ledger:write"},
		}).
			Client(context.WithValue(context.Background(), oauth2.HTTPClient, httpClient))
	}

	return httpClient
}

func initializeFactory(config EnvConfig) (EnvFactory, error) {
	var factory EnvFactory

	switch {
	case config.LedgerURL != "":
		httpClient := getHTTPClient(config)
		factory = &RemoteLedgerEnvFactory{
			httpClient: httpClient,
			ledgerURL:  config.LedgerURL,
		}
	default:
		return nil, fmt.Errorf("must specify either stack.url or ledger.url")
	}

	return factory, nil
}

// Benchmark types
type BenchmarkConfig struct {
	Script         string
	ReportFile     string
	Parallelism    int64
	Duration       time.Duration
	Iterations     int
	LedgerName     string
	LedgerURL      string
	Logger         logging.Logger
	CPUProfileURL  string
	CPUProfileFile string
}

type Result struct {
	Start           time.Time
	End             time.Time
	Metrics         *tachymeter.Metrics
	Name            string
	TPS             float64
	InternalMetrics map[string]any
}

type report struct {
	mu *sync.Mutex

	Start time.Time
	End   time.Time

	Tachymeter *tachymeter.Tachymeter

	Scenario        string
	InternalMetrics map[string]any
}

func (r *report) GetResult() Result {
	return Result{
		Start:           r.Start,
		End:             r.End,
		Metrics:         r.Tachymeter.Calc(),
		InternalMetrics: r.InternalMetrics,
		Name:            r.Scenario,
		TPS:             r.TPS(),
	}
}

func (r *report) TPS() float64 {
	if r.End.Sub(r.Start) == 0 {
		return 0
	}
	return (float64(r.Tachymeter.Count) / float64(r.End.Sub(r.Start))) * float64(time.Second)
}

func (r *report) registerTransactionLatency(latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Tachymeter.AddTime(latency)
}

func (r *report) reset() {
	r.Start = time.Now()
	r.Tachymeter.Reset()
}

func newReport(scenario string) *report {
	ret := &report{
		Scenario: scenario,
		mu:       &sync.Mutex{},
		Tachymeter: tachymeter.New(&tachymeter.Config{
			Size: 10000,
		}),
	}
	ret.reset()
	return ret
}

// ActionProvider interface is now defined in generate.go

type Runner struct {
	config BenchmarkConfig
}

func NewRunner(config BenchmarkConfig) *Runner {
	return &Runner{
		config: config,
	}
}

func (r *Runner) Run(ctx context.Context, envFactory EnvFactory) (map[string]Result, error) {
	results := make(map[string]Result)

	// CPU profiling will be started just before transactions and stopped just after
	cpuProfileData := make(chan []byte, 1)
	profilingCtx, profilingCancel := context.WithCancel(ctx)
	defer profilingCancel() // Ensure cleanup

	// Load scripts
	var scriptFactories map[string]ActionProviderFactory
	var err error

	if r.config.Script != "" {
		// Load single script from file
		factory, err := LoadScriptFromFile(r.config.Script)
		if err != nil {
			return nil, fmt.Errorf("loading script from file: %w", err)
		}
		scriptFactories = map[string]ActionProviderFactory{
			filepath.Base(r.config.Script): factory,
		}
	} else {
		// Load all scripts from embedded filesystem
		scriptFactories, err = LoadScriptsFromEmbed()
		if err != nil {
			return nil, fmt.Errorf("loading scripts: %w", err)
		}
	}

	if len(scriptFactories) == 0 {
		return nil, fmt.Errorf("no scripts found")
	}

	scenarioNames := make([]string, 0, len(scriptFactories))
	for name := range scriptFactories {
		scenarioNames = append(scenarioNames, name)
	}
	sort.Strings(scenarioNames)

	for _, scenario := range scenarioNames {
		r.config.Logger.Infof("Running benchmark: %s", scenario)

		ledgerName := r.config.LedgerName
		if ledgerName == "" {
			return nil, fmt.Errorf("ledger name is required, use --ledger.name flag")
		}

		// Create environment
		benchEnv, err := envFactory.Create()
		if err != nil {
			return nil, fmt.Errorf("creating environment: %w", err)
		}

		// Create report
		report := newReport(scenario)

		// Run benchmark
		var wg sync.WaitGroup
		globalIteration := atomic.Int64{}
		stopChan := make(chan struct{})
		var stopOnce sync.Once

		// Setup timeout - duration is always set due to validation
		go func() {
			<-time.After(r.config.Duration)
			stopOnce.Do(func() {
				close(stopChan)
			})
		}()

		// Setup iterations limit
		if r.config.Iterations > 0 {
			go func() {
				for globalIteration.Load() < int64(r.config.Iterations) {
					time.Sleep(10 * time.Millisecond)
				}
				stopOnce.Do(func() {
					close(stopChan)
				})
			}()
		}

		// Setup cancellation
		go func() {
			<-ctx.Done()
			stopOnce.Do(func() {
				close(stopChan)
			})
		}()

		parallelism := int(r.config.Parallelism)
		if parallelism == 0 {
			parallelism = 1
		}

		// Start CPU profiling just before transactions begin for precise results
		if r.config.CPUProfileURL != "" {
			r.config.Logger.Infof("Starting CPU profiling collection from %s", r.config.CPUProfileURL)

			// Calculate duration for profile collection (use benchmark duration + buffer)
			profileDuration := r.config.Duration
			if profileDuration <= 0 {
				// If using iterations, estimate duration (will be cancelled when benchmark ends)
				profileDuration = 5 * time.Minute // Long enough for most benchmarks
			}
			// Add a small buffer to ensure we capture everything
			profileDuration = profileDuration + 2*time.Second

			// Start collecting profile for the duration of the benchmark
			// Use a longer duration to ensure we capture everything, we'll stop it manually
			profileURL := fmt.Sprintf("%s?seconds=%d", r.config.CPUProfileURL, int(profileDuration.Seconds()))
			client := &http.Client{
				Timeout: profileDuration + 10*time.Second,
			}

			// Collect profile in background, will be cancelled when context is done
			profileStarted := make(chan struct{})
			go func() {
				// Signal that we're starting the HTTP request
				close(profileStarted)

				resp, err := client.Get(profileURL)
				if err != nil {
					select {
					case <-profilingCtx.Done():
						// Context cancelled, this is expected
					default:
						r.config.Logger.Errorf("Failed to collect CPU profile: %v", err)
					}
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					r.config.Logger.Errorf("CPU profile endpoint returned status %d", resp.StatusCode)
					return
				}

				data, err := io.ReadAll(resp.Body)
				if err != nil {
					r.config.Logger.Errorf("Failed to read CPU profile: %v", err)
					return
				}
				cpuProfileData <- data

				r.config.Logger.Infof("Collected CPU profile (%d bytes)", len(data))
			}()

			// Wait a tiny bit to ensure the HTTP request has been initiated
			// This ensures profiling starts just before transactions
			<-profileStarted
			time.Sleep(50 * time.Millisecond)
		}

		for i := 0; i < parallelism; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				iteration := atomic.Int64{}

				// Create action provider from factory
				factory := scriptFactories[scenario]
				actionProvider, err := factory.Create()
				if err != nil {
					panic(fmt.Errorf("creating action provider for %s: %w", scenario, err))
				}

				for {
					select {
					case <-stopChan:
						return
					default:
					}

					globalIter := int(globalIteration.Add(1))
					if r.config.Iterations > 0 && globalIter > r.config.Iterations {
						return
					}

					iter := int(iteration.Add(1))

					bulkElement, err := actionProvider.Get(globalIter, iter)
					if err != nil {
						r.config.Logger.Errorf("Error getting action: %v", err)
						continue
					}

					now := time.Now()

					// Apply action using bulk operations
					err = ApplyAction(ctx, benchEnv.Client(), ledgerName, bulkElement)
					if err != nil {
						//TODO: log errors
						r.config.Logger.Errorf("Error applying action: %v", err)
						continue
					}

					report.registerTransactionLatency(time.Since(now))
				}
			}(i)
		}

		wg.Wait()
		report.End = time.Now()

		// Stop CPU profiling immediately after transactions complete
		if r.config.CPUProfileURL != "" {
			r.config.Logger.Infof("Stopping CPU profiling collection...")
			profilingCancel() // Cancel context to stop profiling goroutine
		}

		// Compute final results
		result := report.GetResult()

		// Fetch metrics from /metrics endpoint
		if r.config.LedgerURL != "" {
			metricsURL := strings.TrimSuffix(r.config.LedgerURL, "/") + "/metrics"
			metrics, err := fetchMetrics(ctx, metricsURL, getHTTPClient(EnvConfig{LedgerURL: r.config.LedgerURL}))
			if err != nil {
				r.config.Logger.Infof("WARN: Failed to fetch metrics from %s: %v", metricsURL, err)
			} else {
				if result.InternalMetrics == nil {
					result.InternalMetrics = make(map[string]any)
				}
				result.InternalMetrics["server_metrics"] = metrics
				r.config.Logger.Infof("Metrics fetched from %s", metricsURL)
			}
		}

		if report.Tachymeter.Count > 0 {
			results[scenario] = result
			r.config.Logger.Infof("Benchmark %s completed: TPS=%.2f, Avg Latency=%.2fms, Count=%d",
				scenario,
				result.TPS,
				result.Metrics.Time.Avg.Seconds()*1000,
				report.Tachymeter.Count,
			)
		}

		// Cleanup
		if err := benchEnv.Stop(ctx); err != nil {
			r.config.Logger.Infof("WARN: Error stopping environment: %v", err)
		}
	}

	// Write report
	if r.config.ReportFile != "" {
		if err := os.MkdirAll(filepath.Dir(r.config.ReportFile), 0755); err != nil {
			return nil, fmt.Errorf("creating report directory: %w", err)
		}

		f, err := os.Create(r.config.ReportFile)
		if err != nil {
			return nil, fmt.Errorf("creating report file: %w", err)
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(results); err != nil {
			return nil, fmt.Errorf("encoding report: %w", err)
		}

		r.config.Logger.Infof("Report written to: %s", r.config.ReportFile)
	}

	// Wait for CPU profile collection to complete and save it
	if r.config.CPUProfileURL != "" {
		// Wait for profile data with timeout
		select {
		case cpuProfileData := <-cpuProfileData:
			if len(cpuProfileData) > 0 && r.config.CPUProfileFile != "" {
				if err := os.MkdirAll(filepath.Dir(r.config.CPUProfileFile), 0755); err != nil {
					r.config.Logger.Errorf("Failed to create CPU profile directory: %v", err)
				} else {
					if err := os.WriteFile(r.config.CPUProfileFile, cpuProfileData, 0644); err != nil {
						r.config.Logger.Errorf("Failed to write CPU profile: %v", err)
					} else {
						r.config.Logger.Infof("CPU profile written to: %s (%d bytes)", r.config.CPUProfileFile, len(cpuProfileData))
					}
				}
			}
		case <-time.After(5 * time.Second):
			r.config.Logger.Infof("WARN: Timeout waiting for CPU profile collection")
		}
	}

	return results, nil
}

// fetchMetrics retrieves metrics from the /metrics endpoint
func fetchMetrics(ctx context.Context, metricsURL string, httpClient *http.Client) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	var metrics map[string]any
	if err := json.Unmarshal(body, &metrics); err != nil {
		return nil, fmt.Errorf("unmarshaling metrics: %w", err)
	}

	return metrics, nil
}

func main() {
	rootCmd := newRootCommand()
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "performance",
		Short: "Performance benchmarking tool for ledger-v3-poc",
		Long:  "A standalone performance benchmarking tool that can run benchmarks against local or remote ledger instances",
		RunE:  runBenchmark,
	}

	// Environment flags
	rootCmd.Flags().String("stack.url", "", "Stack URL")
	rootCmd.Flags().String("client.id", "", "Client ID")
	rootCmd.Flags().String("client.secret", "", "Client secret")
	rootCmd.Flags().String("ledger.url", "", "Ledger URL")
	rootCmd.Flags().String("auth.url", "", "Auth URL (ignored if --stack.url is specified)")

	// Benchmark flags
	rootCmd.Flags().String("script", "", "Script to run (default: all scripts in scripts directory)")
	rootCmd.Flags().String("report.file", "", "Location to write report file")
	rootCmd.Flags().Int64("parallelism", 1, "Parallelism (default 1). Value is multiplied by GOMAXPROCS")
	rootCmd.Flags().Duration("duration", 10*time.Second, "Duration to run the benchmark (required if iterations is not set)")
	rootCmd.Flags().Int("iterations", 0, "Number of iterations (0 = run for duration, requires duration to be set)")
	rootCmd.Flags().String("ledger.name", "", "Ledger name (required)")
	rootCmd.Flags().Bool("debug", false, "Enable debug logging")
	rootCmd.Flags().String("cpu-profile.file", "cpu.prof", "Output file for CPU profile (CPU profiling URL is automatically deduced from ledger URL)")

	return rootCmd
}

func runBenchmark(cmd *cobra.Command, args []string) error {
	// Setup logger first
	logger := logging.NewDefaultLogger(cmd.OutOrStdout(), true, false, false)
	ctx := logging.ContextWithLogger(cmd.Context(), logger)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	// Parse flags
	clientID, _ := cmd.Flags().GetString("client.id")
	clientSecret, _ := cmd.Flags().GetString("client.secret")
	ledgerURL, _ := cmd.Flags().GetString("ledger.url")
	authURL, _ := cmd.Flags().GetString("auth.url")
	scriptFlag, _ := cmd.Flags().GetString("script")
	reportFile, _ := cmd.Flags().GetString("report.file")
	parallelism, _ := cmd.Flags().GetInt64("parallelism")
	duration, _ := cmd.Flags().GetDuration("duration")
	iterations, _ := cmd.Flags().GetInt("iterations")
	ledgerName, _ := cmd.Flags().GetString("ledger.name")
	cpuProfileFile, _ := cmd.Flags().GetString("cpu-profile.file")
	cpuProfileURL := strings.TrimSuffix(ledgerURL, "/") + "/debug/pprof/profile"

	// Validate that duration is explicitly set (required)
	if duration <= 0 {
		return fmt.Errorf("--duration must be explicitly set to define test duration")
	}

	// Log CPU profile URL if it was deduced
	if cpuProfileURL != "" {
		logger.Infof("CPU profiling enabled: %s (output: %s)", cpuProfileURL, cpuProfileFile)
	}

	// Initialize environment factory
	envFactory, err := initializeFactory(EnvConfig{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		LedgerURL:    ledgerURL,
		AuthURL:      authURL,
	})
	if err != nil {
		return fmt.Errorf("initializing environment factory: %w", err)
	}

	// Create benchmark runner
	runner := NewRunner(BenchmarkConfig{
		Script:         scriptFlag,
		ReportFile:     reportFile,
		Parallelism:    parallelism,
		Duration:       duration,
		Iterations:     iterations,
		LedgerName:     ledgerName,
		LedgerURL:      ledgerURL,
		Logger:         logger,
		CPUProfileURL:  cpuProfileURL,
		CPUProfileFile: cpuProfileFile,
	})

	// Run benchmark
	results, err := runner.Run(ctx, envFactory)
	if err != nil {
		return fmt.Errorf("running benchmark: %w", err)
	}

	// Print summary
	logger.Infof("Benchmark completed successfully")
	logger.Infof("Results: %d scenarios", len(results))
	for scenario, result := range results {
		logger.Infof("  Scenario %s: TPS=%.2f, Avg Latency=%.2fms",
			scenario,
			result.TPS,
			result.Metrics.Time.Avg.Seconds()*1000,
		)
	}

	return nil
}
