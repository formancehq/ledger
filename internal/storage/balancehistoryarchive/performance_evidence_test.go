package balancehistoryarchive

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

type archivePerfLatency struct {
	Name                 string  `json:"name"`
	Backend              string  `json:"backend"`
	Samples              int     `json:"samples"`
	MinMS                float64 `json:"minMs"`
	P50MS                float64 `json:"p50Ms"`
	P95MS                float64 `json:"p95Ms"`
	P99MS                float64 `json:"p99Ms"`
	MaxMS                float64 `json:"maxMs"`
	MeanMS               float64 `json:"meanMs"`
	OperationsPerSecond  float64 `json:"operationsPerSecond"`
	MiBPerSecond         float64 `json:"mibPerSecond"`
	AllocatedBytesPerOp  float64 `json:"allocatedBytesPerOp"`
	AllocationsPerOp     float64 `json:"allocationsPerOp"`
	GoCPUSeconds         float64 `json:"goCpuSeconds"`
	GoCPUMicrosecondsPer float64 `json:"goCpuMicrosecondsPerOp"`
	SamplesNS            []int64 `json:"samplesNs"`
}

type archivePerfReport struct {
	SchemaVersion int                `json:"schemaVersion"`
	GeneratedAt   string             `json:"generatedAt"`
	Profile       string             `json:"profile"`
	GitCommit     string             `json:"gitCommit"`
	GitTree       string             `json:"gitTree"`
	WorkingTree   string             `json:"workingTree"`
	Machine       string             `json:"machine"`
	GoVersion     string             `json:"goVersion"`
	GOOS          string             `json:"goos"`
	GOARCH        string             `json:"goarch"`
	RecordCount   int                `json:"recordCount"`
	PayloadBytes  uint64             `json:"payloadBytes"`
	EncodedBytes  uint64             `json:"encodedBytes"`
	Archive       archivePerfLatency `json:"archive"`
	ColdMiss      archivePerfLatency `json:"coldMiss"`
	CacheHit      archivePerfLatency `json:"cacheHit"`
	Limitations   []string           `json:"limitations"`
}

type archivePerfRuntime struct {
	allocatedBytes uint64
	allocations    uint64
	cpuSeconds     float64
}

// TestArchiveLocalPerformanceEvidence measures the integrated immutable-run
// codec and verified cache boundary. The filesystem backend is intentionally
// local and does not stand in for S3/network latency; that remains a deployed
// measurement. The test is opt-in because it streams hundreds of MiB.
func TestArchiveLocalPerformanceEvidence(t *testing.T) {
	if os.Getenv("PIT_PERF") != "1" {
		t.Skip("set PIT_PERF=1 to run the balance-history archive performance harness")
	}

	profile, recordCount, valueBytes, samples := archiveSelectedPerfProfile(t)
	records := make([]Record, recordCount)
	payloadBytes := uint64(0)
	for index := range recordCount {
		key := fmt.Appendf(nil, "run/account/%012d", index)
		value := make([]byte, valueBytes)
		for byteIndex := range value {
			value[byteIndex] = byte(index*31 + byteIndex*17)
		}
		records[index] = Record{Key: key, Value: value}
		payloadBytes += uint64(len(key) + len(value))
	}

	root := t.TempDir()
	cold := coldstorage.NewFilesystemStorage(filepath.Join(root, "cold"))
	uploadStore, err := New(cold, Config{
		BaseBucketID:  "pit-performance",
		OwnerID:       "node-1",
		CacheDir:      filepath.Join(root, "upload-cache"),
		CacheMaxBytes: int64(payloadBytes * 2),
	}, noop.NewMeterProvider().Meter("balance-history-archive-upload-performance"))
	if err != nil {
		t.Fatalf("opening archive upload performance store: %v", err)
	}
	uploadBefore := captureArchivePerfRuntime()
	uploadStarted := time.Now()
	ref, err := uploadStore.Archive(context.Background(), NewSliceStream(records))
	uploadDuration := time.Since(uploadStarted).Nanoseconds()
	uploadAfter := captureArchivePerfRuntime()
	if err != nil {
		_ = uploadStore.Close()
		t.Fatalf("archiving performance run: %v", err)
	}
	if err := uploadStore.Close(); err != nil {
		t.Fatalf("closing archive upload performance store: %v", err)
	}
	archiveMeasurement := summarizeArchivePerfLatency(
		"archive_encode_publish_verify",
		[]int64{uploadDuration},
		ref.Size,
		uploadBefore,
		uploadAfter,
	)

	missStore, err := New(cold, Config{
		BaseBucketID:  "pit-performance",
		OwnerID:       "node-1",
		CacheDir:      filepath.Join(root, "miss-cache"),
		CacheMaxBytes: int64(ref.Size - 1),
	}, noop.NewMeterProvider().Meter("balance-history-archive-miss-performance"))
	if err != nil {
		t.Fatalf("opening cold-miss performance store: %v", err)
	}
	// The cache is smaller than the object. Closing every lease evicts it, so
	// every measured lookup executes the cold fetch and verified admission path.
	missMeasurement := measureArchivePerfFetch(t, missStore, ref, samples, "cold_fetch_cache_miss")
	if err := missStore.Close(); err != nil {
		t.Fatalf("closing cold-miss performance store: %v", err)
	}

	hitStore, err := New(cold, Config{
		BaseBucketID:  "pit-performance",
		OwnerID:       "node-1",
		CacheDir:      filepath.Join(root, "hit-cache"),
		CacheMaxBytes: int64(ref.Size * 2),
	}, noop.NewMeterProvider().Meter("balance-history-archive-hit-performance"))
	if err != nil {
		t.Fatalf("opening cache-hit performance store: %v", err)
	}
	if _, readErr := readArchivePerfLease(hitStore, ref); readErr != nil {
		_ = hitStore.Close()
		t.Fatalf("warming archive cache: %v", readErr)
	}
	hitMeasurement := measureArchivePerfFetch(t, hitStore, ref, samples, "verified_local_cache_hit")
	if err := hitStore.Close(); err != nil {
		t.Fatalf("closing cache-hit performance store: %v", err)
	}

	report := archivePerfReport{
		SchemaVersion: 1,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Profile:       profile,
		GitCommit:     archivePerfValueOrUnknown(os.Getenv("PIT_PERF_GIT_COMMIT")),
		GitTree:       archivePerfValueOrUnknown(os.Getenv("PIT_PERF_GIT_TREE")),
		WorkingTree:   archivePerfValueOrUnknown(os.Getenv("PIT_PERF_WORKTREE")),
		Machine:       archivePerfValueOrUnknown(os.Getenv("PIT_PERF_MACHINE")),
		GoVersion:     runtime.Version(),
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
		RecordCount:   recordCount,
		PayloadBytes:  payloadBytes,
		EncodedBytes:  ref.Size,
		Archive:       archiveMeasurement,
		ColdMiss:      missMeasurement,
		CacheHit:      hitMeasurement,
		Limitations: []string{
			"filesystem backend: excludes S3 request, network, TLS, and cross-AZ latency",
			"operating-system page cache is not flushed between cold-miss samples",
			"measures immutable-run fetch and full verification, not an end-to-end PIT API query",
		},
	}
	writeArchivePerfEvidence(t, report)
}

func archiveSelectedPerfProfile(t *testing.T) (string, int, int, int) {
	t.Helper()

	switch profile := os.Getenv("PIT_PERF_PROFILE"); profile {
	case "", "local":
		return "local", 2_048, 4 << 10, 60
	case "smoke":
		return "smoke", 256, 4 << 10, 10
	case "full":
		return "full", 8_192, 4 << 10, 200
	default:
		t.Fatalf("unknown PIT_PERF_PROFILE %q; expected smoke, local, or full", profile)

		return "", 0, 0, 0
	}
}

func measureArchivePerfFetch(
	t *testing.T,
	store *Store,
	ref Ref,
	samples int,
	name string,
) archivePerfLatency {
	t.Helper()

	for range min(3, samples) {
		if _, err := readArchivePerfLease(store, ref); err != nil {
			t.Fatalf("warming archive fetch %s: %v", name, err)
		}
	}
	runtimeBefore := captureArchivePerfRuntime()
	durations := make([]int64, samples)
	for index := range samples {
		started := time.Now()
		readBytes, err := readArchivePerfLease(store, ref)
		durations[index] = time.Since(started).Nanoseconds()
		if err != nil {
			t.Fatalf("reading archive performance sample %s/%d: %v", name, index, err)
		}
		if readBytes != ref.Size {
			t.Fatalf("archive performance sample %s/%d read %d bytes, want encoded size %d", name, index, readBytes, ref.Size)
		}
	}
	runtimeAfter := captureArchivePerfRuntime()

	return summarizeArchivePerfLatency(name, durations, ref.Size, runtimeBefore, runtimeAfter)
}

func readArchivePerfLease(store *Store, ref Ref) (uint64, error) {
	lease, err := store.Fetch(context.Background(), ref)
	if err != nil {
		return 0, err
	}
	reader, err := lease.Open()
	if err != nil {
		_ = lease.Close()

		return 0, err
	}
	readBytes := uint64(headerSize + trailerSize)
	for reader.Next() {
		record := reader.Record()
		readBytes += recordHeaderSize + uint64(len(record.Key)+len(record.Value))
	}
	readErr := reader.Err()
	closeReaderErr := reader.Close()
	closeLeaseErr := lease.Close()
	if readErr != nil {
		return 0, readErr
	}
	if closeReaderErr != nil {
		return 0, closeReaderErr
	}
	if closeLeaseErr != nil {
		return 0, closeLeaseErr
	}

	return readBytes, nil
}

func captureArchivePerfRuntime() archivePerfRuntime {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	cpuSamples := []runtimemetrics.Sample{
		{Name: "/cpu/classes/user:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/scavenge/total:cpu-seconds"},
	}
	runtimemetrics.Read(cpuSamples)
	cpuSeconds := float64(0)
	for _, sample := range cpuSamples {
		cpuSeconds += sample.Value.Float64()
	}

	return archivePerfRuntime{
		allocatedBytes: memory.TotalAlloc,
		allocations:    memory.Mallocs,
		cpuSeconds:     cpuSeconds,
	}
}

func summarizeArchivePerfLatency(
	name string,
	durations []int64,
	bytesPerOperation uint64,
	before, after archivePerfRuntime,
) archivePerfLatency {
	ordered := slices.Clone(durations)
	slices.Sort(ordered)
	total := int64(0)
	for _, duration := range durations {
		total += duration
	}
	operations := float64(len(durations))
	elapsedSeconds := float64(total) / float64(time.Second)
	cpuSeconds := after.cpuSeconds - before.cpuSeconds
	toMilliseconds := func(nanoseconds int64) float64 {
		return float64(nanoseconds) / float64(time.Millisecond)
	}

	return archivePerfLatency{
		Name:                 name,
		Backend:              "local-filesystem",
		Samples:              len(durations),
		MinMS:                toMilliseconds(ordered[0]),
		P50MS:                toMilliseconds(archivePerfPercentile(ordered, 0.50)),
		P95MS:                toMilliseconds(archivePerfPercentile(ordered, 0.95)),
		P99MS:                toMilliseconds(archivePerfPercentile(ordered, 0.99)),
		MaxMS:                toMilliseconds(ordered[len(ordered)-1]),
		MeanMS:               toMilliseconds(total / int64(len(durations))),
		OperationsPerSecond:  operations / elapsedSeconds,
		MiBPerSecond:         float64(bytesPerOperation) * operations / (1 << 20) / elapsedSeconds,
		AllocatedBytesPerOp:  float64(after.allocatedBytes-before.allocatedBytes) / operations,
		AllocationsPerOp:     float64(after.allocations-before.allocations) / operations,
		GoCPUSeconds:         cpuSeconds,
		GoCPUMicrosecondsPer: cpuSeconds * float64(time.Second/time.Microsecond) / operations,
		SamplesNS:            slices.Clone(durations),
	}
}

func archivePerfPercentile(ordered []int64, quantile float64) int64 {
	index := int(float64(len(ordered))*quantile+0.999999999) - 1
	index = max(0, min(index, len(ordered)-1))

	return ordered[index]
}

func writeArchivePerfEvidence(t *testing.T, report archivePerfReport) {
	t.Helper()

	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshaling archive performance evidence: %v", err)
	}
	output := os.Getenv("PIT_ARCHIVE_PERF_OUTPUT")
	if output == "" {
		t.Logf("PIT_ARCHIVE_PERF_OUTPUT is unset; raw evidence follows:\n%s", encoded)

		return
	}
	if !filepath.IsAbs(output) {
		cwd, cwdErr := os.Getwd()
		if cwdErr != nil {
			t.Fatalf("resolving archive performance output path: %v", cwdErr)
		}
		output = filepath.Join(cwd, output)
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o750); err != nil {
		t.Fatalf("creating archive performance output directory: %v", err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0o600); err != nil {
		t.Fatalf("writing archive performance evidence %s: %v", output, err)
	}
	t.Logf("wrote archive performance evidence to %s", output)
}

func archivePerfValueOrUnknown(value string) string {
	if value == "" {
		return "unknown"
	}

	return value
}

func TestArchivePerfPercentileUsesNearestRank(t *testing.T) {
	t.Parallel()

	samples := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	if got := archivePerfPercentile(samples, 0.50); got != 10 {
		t.Fatalf("p50 = %d, want 10", got)
	}
	if got := archivePerfPercentile(samples, 0.95); got != 19 {
		t.Fatalf("p95 = %d, want 19", got)
	}
	if got := archivePerfPercentile(samples, 0.99); got != 20 {
		t.Fatalf("p99 = %d, want 20", got)
	}
}
