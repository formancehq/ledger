package main

import (
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Per-sonde hit counts, enabled with MODEL_COVERAGE_STATS=1. The SDK emits one
// line for an assertion's first pass and one for its first failure, so the run
// output says whether a sonde fired but never how often or how evenly — which
// is what separates a sonde that holds a rate from one that passed on a single
// transient window.

// covBucket is the resolution of the per-sonde hit timeline.
const covBucket = 5 * time.Second

type covStat struct {
	hits    int
	buckets []int
}

var (
	covStatsStart = time.Now()
	covStatsOn    = os.Getenv("MODEL_COVERAGE_STATS") != ""
	covStatsMu    sync.Mutex
	covStatsData  = map[string]*covStat{}
)

// recordCoverageHit counts one satisfied sonde evaluation. Registrations
// (hit=false) and unsatisfied evaluations carry no rate information.
func recordCoverageHit(msg string, cond, hit bool) {
	if !covStatsOn || !hit || !cond {
		return
	}

	covStatsMu.Lock()
	defer covStatsMu.Unlock()

	stat, ok := covStatsData[msg]
	if !ok {
		stat = &covStat{}
		covStatsData[msg] = stat
	}

	stat.hits++

	slot := int(time.Since(covStatsStart) / covBucket)
	for len(stat.buckets) <= slot {
		stat.buckets = append(stat.buckets, 0)
	}
	stat.buckets[slot]++
}

// startCoverageStats dumps periodically: the harness SIGTERMs the driver at the
// deadline, so an end-of-run dump would never be written.
func startCoverageStats() {
	if !covStatsOn {
		return
	}

	go func() {
		for range time.Tick(covBucket) {
			dumpCoverageStats()
		}
	}()
}

func dumpCoverageStats() {
	if !covStatsOn {
		return
	}

	covStatsMu.Lock()
	defer covStatsMu.Unlock()

	messages := make([]string, 0, len(covStatsData))
	for msg := range covStatsData {
		messages = append(messages, msg)
	}
	sort.Strings(messages)

	log.Printf("coverage-stats: message | hits | hits per %s", covBucket)
	for _, msg := range messages {
		stat := covStatsData[msg]
		slots := make([]string, len(stat.buckets))
		for i, n := range stat.buckets {
			slots[i] = strconv.Itoa(n)
		}
		log.Printf("coverage-stats: %s | %d | %s", msg, stat.hits, strings.Join(slots, ","))
	}
}
