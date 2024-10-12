interface MetricsTime {
    Cumulative: string // Cumulative time of all sampled events.
    HMean: string // Event duration harmonic mean.
    Avg:        string // Event duration average.
    P50:        string // Event duration nth percentiles ..
    P75:        string
    P95:        string
    P99:        string
    P999:       string
    Long5p:     string // Average of the longest 5% event durations.
    Short5p:    string // Average of the shortest 5% event durations.
    Max:        string // Highest event duration.
    Min:        string // Lowest event duration.
    StdDev:     string // Standard deviation.
    Range:      string // Event duration range (Max-Min).
}

interface MetricsRate {
    Second: number
}

interface Metrics {
    Time:  MetricsTime,
    Rate: MetricsRate,
    Histogram: Map<string, number>[]    // Frequency distribution of event durations in len(Histogram) bins of HistogramBinSize.
    HistogramBinSize: string // The width of a histogram bin in time.
    Samples:          number           // Number of events included in the sample set.
    Count:            number           // Total number of events observed.
}

interface Configuration {
    Name: string,
    FeatureSet: Map<string, string>
}

interface Report {
    Start: string,
    End: string,
    Metrics: Metrics,
    Scenario: string,
    Configuration: Configuration,
    TPS: number
}

interface BenchmarkResult {
    [key: string]: Report[];
}