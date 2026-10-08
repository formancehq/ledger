package numscript

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestNewNumscriptCache_DefaultSize(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(0)
	require.NotNil(t, c)
	require.Equal(t, 1024, c.maxSize)
}

func TestNewNumscriptCache_NegativeSize(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(-5)
	require.NotNil(t, c)
	require.Equal(t, 1024, c.maxSize)
}

func TestNewNumscriptCache_CustomSize(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(10)
	require.NotNil(t, c)
	require.Equal(t, 10, c.maxSize)
}

func TestNumscriptCache_GetOrParse_Miss(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(10)
	script := `send [USD/2 100] (source = @world destination = @users:alice)`

	result, err := c.GetOrParse(script)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestNumscriptCache_GetOrParse_Hit(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(10)
	script := `send [USD/2 100] (source = @world destination = @users:alice)`

	// First call - cache miss
	result1, err1 := c.GetOrParse(script)
	require.NoError(t, err1)

	// Second call - cache hit
	result2, err2 := c.GetOrParse(script)
	require.NoError(t, err2)

	// Both should return equivalent results
	require.Len(t, result2.GetParsingErrors(), len(result1.GetParsingErrors()))
}

func TestNumscriptCache_GetOrParse_Eviction(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(2) // Very small cache

	// Fill the cache with 2 entries
	_, err := c.GetOrParse(`send [USD/2 100] (source = @a destination = @b)`)
	require.NoError(t, err)

	_, err = c.GetOrParse(`send [EUR/2 200] (source = @c destination = @d)`)
	require.NoError(t, err)

	// Third entry should cause eviction
	_, err = c.GetOrParse(`send [GBP/2 300] (source = @e destination = @f)`)
	require.NoError(t, err)

	// Cache should still be at max size
	require.Equal(t, 2, c.order.Len())
}

func TestNumscriptCache_GetOrParse_ParseError(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(10)
	script := `send [USD/2 invalid] (source = @world destination = @users:alice)`

	_, err := c.GetOrParse(script)
	require.Error(t, err)

	var parseErr *domain.ErrNumscriptParse
	require.ErrorAs(t, err, &parseErr)
	require.NotEmpty(t, parseErr.Details)
}

func TestNumscriptCache_GetOrParse_ParseErrorCached(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(10)
	script := `send [USD/2 invalid] (source = @world destination = @users:alice)`

	// First call - parse error
	_, err1 := c.GetOrParse(script)
	require.Error(t, err1)

	// Second call - should return the same cached error
	_, err2 := c.GetOrParse(script)
	require.Error(t, err2)
}

func TestHashScript_Deterministic(t *testing.T) {
	t.Parallel()

	h1 := HashScript("hello world")
	h2 := HashScript("hello world")
	require.Equal(t, h1, h2)
}

func TestHashScript_Different(t *testing.T) {
	t.Parallel()

	h1 := HashScript("hello")
	h2 := HashScript("world")
	require.NotEqual(t, h1, h2)
}

func TestNumscriptCache_InitCacheMetrics(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(10)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter("test")

	err := c.InitCacheMetrics(meter)
	require.NoError(t, err)
	require.NotNil(t, c.sizeGauge)
}

func TestNumscriptCache_RecordSize_NilGauge(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(10)
	// Should not panic when sizeGauge is nil
	c.recordSize(cacheSideParsed, 5)
}

func TestNumscriptCache_RecordSize_WithGauge(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(10)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter("test")

	require.NoError(t, c.InitCacheMetrics(meter))

	// Parse a script to trigger recordSize
	_, _ = c.GetOrParse(`send [USD/2 100] (source = @world destination = @users:alice)`)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.NotEmpty(t, rm.ScopeMetrics)
}

// TestNumscriptCache_RecordSize_ReportsBothSides pins that numscript.cache.size
// reports the compiled-artifact LRU next to the parse LRU: the compiled side
// holds the warm VMs, so a gauge fed by the parse side alone under-reports the
// cache's footprint.
func TestNumscriptCache_RecordSize_ReportsBothSides(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(10)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	require.NoError(t, c.InitCacheMetrics(provider.Meter("test")))

	entry := c.getOrParseEntry(`send [USD/2 100] (source = @world destination = @users:alice)`)
	require.Nil(t, entry.script.err)

	compiled := mustCompile(t, entry, nil)
	vars, decErr := numscriptlib.DecodeVars(compiled.Vars)
	require.NoError(t, decErr)

	_, err := c.getOrDecodeCompiled(compiled.ScriptHash, compiled.Program, &vars)
	require.Nil(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	sizes := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "numscript.cache.size" {
				continue
			}

			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)

			for _, point := range gauge.DataPoints {
				side, found := point.Attributes.Value(attribute.Key("cache"))
				require.True(t, found)
				sizes[side.AsString()] = point.Value
			}
		}
	}

	require.Equal(t, map[string]int64{cacheSideParsed: 1, cacheSideCompiled: 1}, sizes)
}

func TestLruEntry_CompileParsed_AlreadyCompiledAcrossEviction(t *testing.T) {
	t.Parallel()

	c := NewNumscriptCache(1)
	script := `send [USD/2 100] (source = @world destination = @users:alice)`
	other := `send [USD/2 1] (source = @a destination = @b)`

	entry := c.getOrParseEntry(script)
	_, err, alreadyCompiled := entry.compileParsed()
	require.Nil(t, err)
	require.False(t, alreadyCompiled)

	// Evict the entry by parsing a second script against a cache of size 1.
	c.getOrParseEntry(other)
	require.NotSame(t, entry, c.getOrParseEntry(script), "the original entry was evicted")

	// The new entry for the same script text starts cold again: an evicted
	// incarnation's "already compiled" history does not carry over, which is
	// the conservative (never-assume-cached) answer.
	_, err, alreadyCompiled = c.getOrParseEntry(script).compileParsed()
	require.Nil(t, err)
	require.False(t, alreadyCompiled)
}
