package main

import (
	"context"
	"fmt"
	"log"
	"maps"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Chapter lifecycle coverage. The driver emits CloseChapter and ArchiveChapter
// into the modeled bulk stream and lets the oracle decide which the server should
// accept; it never judges an order's legality itself.
//
// What makes that possible despite the server advancing chapters on its own: only
// two transitions carry no request (the Sealer's CLOSING -> CLOSED, the Archiver's
// ARCHIVING -> ARCHIVED), and neither is followed by another, so the model is at
// most one step behind on any chapter. A prediction that turns on a pending step
// therefore has at most a handful of answers, enumerated per order rather than
// forked across the serialization search — an unasked transition touches no
// business state, so it has no business in candidateBases.
//
// A chapter order travels in a bulk of its own for two reasons. A rejected order
// rejects its whole bulk, so mixing would throw away the business work alongside
// it; and candidateBases prunes a fold it cannot yet justify, which costs nothing
// for a chapter-only bulk but would drop business effects with it.

// --- Predicting a chapter order -----------------------------------------------

// applyWithAutonomy applies bulk to base, retrying under each registry the
// server's pending unasked transitions could have produced, and returns the first
// result accept approves. The state it returns carries the registry that explained
// the server's answer, so a caller that adopts the result also adopts the
// hypothesis the answer just confirmed — which is how a committed archive teaches
// the model that the seal, or the preceding confirm, had already landed.
//
// This is for the drained-commit path, which has no search to fold those
// transitions into: a drained bulk's predecessor is exactly modelState, and its
// prediction has to be resolved rather than merely tolerated. candidateBases folds
// them as ordinary moves instead.
//
// A bulk with no chapter order costs exactly one prediction.
func applyWithAutonomy(base oracle.GlobalState, bulk oracle.Bulk, accept func(oracle.ApplyResult) bool) (oracle.ApplyResult, bool) {
	for _, chapters := range chapterVariants(base.Chapters(), archiveTargets(bulk)) {
		if res := base.WithChapters(chapters).Apply(bulk); accept(res) {
			return res, true
		}
	}

	return oracle.ApplyResult{}, false
}

// applyCommitted accepts a prediction that the bulk commits.
func applyCommitted(res oracle.ApplyResult) bool { return res.OK }

// chapterVariants returns the registries the server could hold given the model's
// pinned one, restricted to the unasked transitions that could change the outcome
// of an archive of ids: the seal of each addressed chapter, and the confirm of the
// archiving chapter, which moves the prefix every archive is measured against. The
// pinned registry comes first.
//
// The steps are independent, so the variants are their subsets. Chapter orders
// travel alone, so ids holds at most one and this is at most four registries.
func chapterVariants(pinned oracle.Chapters, ids []uint64) []oracle.Chapters {
	if len(ids) == 0 {
		return []oracle.Chapters{pinned}
	}

	steps := make([]func(oracle.Chapters) (oracle.Chapters, bool), 0, len(ids)+1)
	for _, id := range ids {
		steps = append(steps, func(c oracle.Chapters) (oracle.Chapters, bool) { return c.WithSealed(id) })
	}
	if pinned.Archiving() {
		steps = append(steps, func(c oracle.Chapters) (oracle.Chapters, bool) { return c.WithConfirmed() })
	}

	variants := make([]oracle.Chapters, 0, 1<<len(steps))
	variants = append(variants, pinned)

	for mask := 1; mask < 1<<len(steps); mask++ {
		variant, ok := pinned, true

		for i, step := range steps {
			if mask&(1<<i) == 0 {
				continue
			}
			if variant, ok = step(variant); !ok {
				break
			}
		}

		if ok {
			variants = append(variants, variant)
		}
	}

	return variants
}

// chaptersInPlay returns the chapters some order in play asks to archive: the
// buffered and in-flight bulks the search folds, plus the ids the caller named.
// Sorted and deduplicated so the search's branch order is stable.
func chaptersInPlay(pending, inflight []oracle.Bulk, addressed []uint64) []uint64 {
	ids := append([]uint64(nil), addressed...)

	for _, bulks := range [][]oracle.Bulk{pending, inflight} {
		for _, b := range bulks {
			ids = append(ids, archiveTargets(b)...)
		}
	}

	return distinctIDs(ids)
}

// distinctIDs sorts and deduplicates chapter ids, so a set built from them does not
// depend on the order the bulks were walked in.
func distinctIDs(ids []uint64) []uint64 {
	seen := map[uint64]bool{}
	for _, id := range ids {
		seen[id] = true
	}

	return slices.Sorted(maps.Keys(seen))
}

// archiveTargets returns the chapters bulk asks to archive. Only an archive's
// outcome turns on an unasked transition — a close needs no chapter to be in any
// particular state.
func archiveTargets(bulk oracle.Bulk) []uint64 {
	var ids []uint64

	for _, req := range bulk.Requests {
		if r, ok := req.GetType().(*servicepb.Request_ArchiveChapter); ok {
			ids = append(ids, r.ArchiveChapter.GetChapterId())
		}
	}

	return ids
}

// --- Generating chapter orders ------------------------------------------------

// chapterOrderKind is which chapter order a bulk should carry, if any.
type chapterOrderKind uint8

const (
	chapterOrderNone chapterOrderKind = iota
	chapterOrderClose
	chapterOrderArchive
)

// generateChapterBulk plans one chapter order. It makes no legality judgement:
// which orders the server should accept is the oracle's call. It shapes only the
// distribution, so the ids that matter come up often — the archived prefix's
// successor, the chapters past it, and one already inside the prefix.
func generateChapterBulk(g oracle.GlobalState, kind chapterOrderKind) oracle.Bulk {
	if kind == chapterOrderClose {
		return oracle.Bulk{Requests: []*servicepb.Request{actions.CloseChapterAction()}}
	}

	return oracle.Bulk{Requests: []*servicepb.Request{actions.ArchiveChapterAction(archiveTarget(g.Chapters()))}}
}

// archiveTarget picks the chapter to ask for: usually the archived prefix's
// successor — the one the server should accept once the Sealer has sealed it —
// and otherwise a chapter past the successor or one already archived. Those two
// are the rejections the ordering rule exists for, and their reason is
// single-valued only away from the frontier, which is what the weighting buys.
func archiveTarget(c oracle.Chapters) uint64 {
	through := c.ArchivedThrough()

	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7}) {
	case 0:
		if through == 0 {
			return 1
		}

		// Inside the archived prefix: a re-archive, which the prefix gate must
		// reject the same way whether or not the chapter's rows are still resident.
		return 1 + uint64(random.RandomChoice(indexPool(int(through))))
	case 1:
		return through + 2
	case 2:
		return through + 3
	default:
		return through + 1
	}
}

// enableChapterOrders starts emitting chapter orders, no more often than the given
// gap per kind. Archival is gated on the run's cold-storage configuration
// (ArchiveChapter is refused at admission without it, on node-local config the
// oracle cannot see), so the orders are enabled together with it.
func (c *Checker) enableChapterOrders(closeGap, archiveGap time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.chapterCloseGap = closeGap
	c.chapterArchiveGap = archiveGap
	c.nextCloseGap = sampleGap(closeGap)
	c.nextArchiveGap = sampleGap(archiveGap)
}

// sampleGap draws one wait from a log-normal distribution whose median is the
// configured gap, by inverting the normal CDF over a uniform Antithesis draw. The
// median is the configured value, so the pacing knob keeps meaning what it says
// while the tail supplies the occasional long stretch the registry shapes depend
// on. Draws beyond chapterGapMaxMultiple are clamped.
func sampleGap(median time.Duration) time.Duration {
	if median <= 0 {
		return median
	}

	// (0,1) exclusive: erfInv is infinite at either end.
	u := (float64(random.GetRandom()>>11) + 0.5) / (1 << 53)
	multiple := math.Exp(chapterGapSigma * math.Sqrt2 * math.Erfinv(2*u-1))

	return time.Duration(float64(median) * math.Min(multiple, chapterGapMaxMultiple))
}

// seedChapters replaces the model's registry with the server's. Chapters are
// bucket-global: they outlive the per-run ledger fleet, so a reused bucket starts
// with chapters — and an archived prefix — already in place. Acquires c.mu.
func (c *Checker) seedChapters(ctx context.Context, client servicepb.BucketServiceClient) error {
	chapters, err := actions.ListAllChapters(ctx, client)
	if err != nil {
		return fmt.Errorf("listing chapters: %w", err)
	}

	seeded, err := observedChapters(chapters)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.modelState = c.modelState.WithChapters(seeded)

	return nil
}

// takeChapterOrderLocked reports which chapter order this bulk should carry, if
// any, consuming that kind's pacing budget. Paced rather than drawn per bulk
// because the workers' bulk rate depends on the run's concurrency and the machine,
// not on anything the test controls. Each gap is redrawn as it is consumed, so the
// registry passes through shapes a fixed cadence never reaches — see sampleGap.
// Caller holds c.mu.
//
// An archive order is withheld when the chapters already named by orders in play
// have reached maxChaptersInPlay: candidateBases branches over each of their
// unobserved seals, so the bound holds because the driver never emits past it. A
// close names no chapter to archive and is never withheld.
func (c *Checker) takeChapterOrderLocked(now time.Time) chapterOrderKind {
	if c.chapterArchiveGap == 0 {
		return chapterOrderNone
	}

	if now.Sub(c.lastChapterClose) >= c.nextCloseGap {
		c.lastChapterClose = now
		c.nextCloseGap = sampleGap(c.chapterCloseGap)

		return chapterOrderClose
	}

	if now.Sub(c.lastChapterArchive) < c.nextArchiveGap {
		return chapterOrderNone
	}

	// Conservative: a new order might name a chapter already in the set, but the id
	// is the generator's to pick, so at the ceiling the slot is withheld outright.
	if _, archives := c.outstandingChapterOrders(); len(distinctIDs(archives)) >= maxChaptersInPlay {
		if !c.chapterSlotWithheld {
			c.chapterSlotWithheld = true
			log.Printf("chapter orders: %d chapters already in play, withholding archive requests until they resolve", maxChaptersInPlay)
		}

		return chapterOrderNone
	}

	c.lastChapterArchive = now
	c.nextArchiveGap = sampleGap(c.chapterArchiveGap)

	return chapterOrderArchive
}

// selectArchivalInterval returns the run's archival pacing base, or 0 when the
// run did not configure cold storage (MODEL_ARCHIVE_INTERVAL unset) — in which
// case ArchiveChapter is refused at admission and the driver emits no chapter
// orders at all.
func selectArchivalInterval() time.Duration {
	raw := os.Getenv("MODEL_ARCHIVE_INTERVAL")
	if raw == "" {
		return 0
	}

	return time.Duration(envInt("MODEL_ARCHIVE_INTERVAL", defaultArchiveIntervalSecs)) * time.Second
}

// --- Reading the registry -----------------------------------------------------

// runChapterRead lists the chapters, checks the registry against the model, and
// pins the model to what it saw.
//
// The check is the lifecycle assertion. A chapter's status never moves backwards,
// and it only moves with no order behind it along the two transitions the server
// makes unasked — so a rewound status, an archive nobody asked for, or a chapter
// out of nowhere is a divergence. That last one is a finding because this driver
// owns the chapter timeline: it runs in its own test template, sets no chapter
// schedule, and nothing else in the run closes a chapter. The registry's shape is checked too:
// ChaptersFrom rejects a set the lifecycle cannot produce, which is how a
// non-contiguous archived prefix surfaces. Because a restore rebuilds the registry
// from the backup's logs, this is also the check that a restored store came back
// with the chapter history it had.
//
// The pin is what keeps later predictions single-valued: with the frontier known
// exactly, an archive away from it has one legal reason instead of two. It is sound
// only with no chapter order outstanding — otherwise the observation is newer than
// the model, and folding it in would swallow that order's own effect when it
// drains. The read holds a ticket for its whole window, so the model cannot drain
// past what the read saw either.
func runChapterRead(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	// ListAllChapters pages at 1000, so a run's registry comes back in a single
	// page — one read handle, one snapshot.
	chapters, err := actions.ListAllChapters(ctx, client)
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		assert.Unreachable("singleton_driver_model: listing chapters returned unexpected error", internal.Details{
			"error": err.Error(),
		})

		return
	}

	observed, err := observedChapters(chapters)
	if err != nil {
		dbg("CHAPTER READ FINDING: unreachable shape: %v", err)
		assert.Unreachable("singleton_driver_model: chapter registry is not a shape the lifecycle can produce", internal.Details{
			"error":    err.Error(),
			"observed": describeServerChapters(chapters),
		})

		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	pinned := c.modelState.Chapters()
	closes, archives := c.outstandingChapterOrders()

	if violation, ok := chapterAdvanceExplained(pinned, observed, closes, archives); !ok {
		dbg("CHAPTER READ FINDING: %s (model=%s observed=%s)", violation, pinned, observed)
		assert.Unreachable("singleton_driver_model: observed chapter registry unreachable from the model", internal.Details{
			"violation": violation,
			"model":     pinned.String(),
			"observed":  observed.String(),
			"closes":    closes,
			"archives":  fmt.Sprint(archives),
		})

		return
	}

	dbg("CHAPTER READ OK: model=%s observed=%s", pinned, observed)

	if closes == 0 && len(archives) == 0 {
		c.modelState = c.modelState.WithChapters(observed)
		noteChapterProgress(pinned, observed)
	}
}

// observedChapters folds the server's chapter rows into the model's registry,
// failing on a status the model does not represent or a set the lifecycle cannot
// produce.
func observedChapters(chapters []*commonpb.Chapter) (oracle.Chapters, error) {
	statuses := make(map[uint64]oracle.ChapterStatus, len(chapters))

	for _, ch := range chapters {
		status, ok := oracle.ChapterStatusFromProto(ch.GetStatus())
		if !ok {
			return oracle.NewChapters(), fmt.Errorf("chapter %d has unmodeled status %s", ch.GetId(), ch.GetStatus())
		}

		if _, duplicate := statuses[ch.GetId()]; duplicate {
			return oracle.NewChapters(), fmt.Errorf("chapter %d listed twice", ch.GetId())
		}

		statuses[ch.GetId()] = status
	}

	return oracle.ChaptersFrom(statuses)
}

// outstandingChapterOrders reports the chapter orders whose effect the model has
// not folded yet: dispatched bulks with no response, and committed ones still
// buffered in the re-order queue. Either can already be reflected in what a read
// returns. Orders dispatched after the read returned are counted too, which the
// read cannot be showing — that only widens the advance the check allows and
// withholds the pin, both the safe direction. Caller holds c.mu.
func (c *Checker) outstandingChapterOrders() (closes int, archives []uint64) {
	count := func(b oracle.Bulk) {
		for _, req := range b.Requests {
			switch r := req.GetType().(type) {
			case *servicepb.Request_CloseChapter:
				closes++
			case *servicepb.Request_ArchiveChapter:
				archives = append(archives, r.ArchiveChapter.GetChapterId())
			}
		}
	}

	for _, b := range c.inflight {
		count(b)
	}
	for _, pe := range c.pending {
		count(pe.obs.bulk)
	}

	return closes, archives
}

// chapterAdvanceExplained reports whether observed is reachable from the model's
// registry: every chapter moved forward or not at all, every move the server does
// not make unasked is backed by an outstanding order, and the chapters that
// appeared are covered by outstanding closes. The returned string names the first
// violation, for the finding's details.
func chapterAdvanceExplained(pinned, observed oracle.Chapters, closes int, archives []uint64) (string, bool) {
	if observed.ArchivedThrough() < pinned.ArchivedThrough() {
		return fmt.Sprintf("archived prefix rewound from %d to %d", pinned.ArchivedThrough(), observed.ArchivedThrough()), false
	}

	if observed.LastID() < pinned.LastID() {
		return fmt.Sprintf("chapter %d no longer exists", pinned.LastID()), false
	}

	if appeared := observed.LastID() - pinned.LastID(); appeared > uint64(closes) {
		return fmt.Sprintf("%d chapters appeared with %d close orders outstanding", appeared, closes), false
	}

	requested := map[uint64]bool{}
	for _, id := range archives {
		requested[id] = true
	}

	for id := uint64(1); id <= observed.LastID(); id++ {
		got, ok := observed.StatusOf(id)
		if !ok {
			return fmt.Sprintf("chapter %d missing from the observed registry", id), false
		}

		was, existed := pinned.StatusOf(id)
		if !existed {
			// A chapter an outstanding close created — the count above bounded how
			// many. It can only be the new open one, or one an earlier close in the
			// same window left CLOSING.
			if got != oracle.ChapterClosing && got != oracle.ChapterOpen {
				return fmt.Sprintf("chapter %d appeared as %s", id, got), false
			}

			continue
		}

		if got < was {
			return fmt.Sprintf("chapter %d moved back from %s to %s", id, was, got), false
		}

		if violation, backed := advanceBacked(id, was, got, closes, requested); !backed {
			return violation, false
		}
	}

	return "", true
}

// advanceBacked walks the transitions between was and got, requiring each to be
// one the server makes unasked or one an outstanding order asked for.
func advanceBacked(id uint64, was, got oracle.ChapterStatus, closes int, requested map[uint64]bool) (string, bool) {
	for status := was; status != got; {
		if next, autonomous := oracle.AutonomousNext(status); autonomous {
			status = next

			continue
		}

		switch status {
		case oracle.ChapterOpen:
			if closes == 0 {
				return fmt.Sprintf("chapter %d closed with no close order outstanding", id), false
			}

			status = oracle.ChapterClosing
		case oracle.ChapterClosed:
			if !requested[id] {
				return fmt.Sprintf("chapter %d started archiving with no archive order outstanding for it", id), false
			}

			status = oracle.ChapterArchiving
		default:
			return fmt.Sprintf("chapter %d reached %s from %s by no known transition", id, got, was), false
		}
	}

	return "", true
}

// noteChapterProgress reports a completed archival: the confirm extended the
// archived prefix, which is the point the chapter's logs and audit entries were
// purged from hot storage. The log line is what the local runner's report counts;
// the Sometimes carries the same proof into an Antithesis run, where a history
// that never archived exercised none of this.
func noteChapterProgress(before, after oracle.Chapters) {
	for id := before.ArchivedThrough() + 1; id <= after.ArchivedThrough(); id++ {
		assert.Sometimes(true, "singleton_driver_model: chapter archived", internal.Details{})
		log.Printf("archival cycle: chapter %d archived + purged", id)
	}
}

// describeServerChapters renders the server's rows in id order, for a finding
// whose registry the model could not fold.
func describeServerChapters(chapters []*commonpb.Chapter) string {
	sorted := make([]*commonpb.Chapter, len(chapters))
	copy(sorted, chapters)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].GetId() < sorted[j].GetId() })

	parts := make([]string, 0, len(sorted))
	for _, ch := range sorted {
		parts = append(parts, fmt.Sprintf("%d:%s", ch.GetId(), ch.GetStatus()))
	}

	return strings.Join(parts, " ")
}
