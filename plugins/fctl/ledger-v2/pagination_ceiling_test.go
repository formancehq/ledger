package ledgerv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// The host validates that an all-pages request carries the canonical ceilings,
// so a ceiling boundary cannot be exercised through Plugin.Execute. These tests
// drive collectGeneratedPages directly, which is where the ceiling is applied.

// pagedFetch serves `pages` pages of `count` items each and counts its calls.
func pagedFetch(pages, count int) (func(*string) ([]string, *string, bool, error), []string, *int) {
	// Non-nil so the expected encoding of an empty traversal is "[]", which is
	// what collectGeneratedPages emits; a nil slice would encode as "null".
	all := make([]string, 0)
	calls := 0
	return func(cursor *string) ([]string, *string, bool, error) {
			page := calls
			calls++
			items := make([]string, 0, count)
			for item := range count {
				items = append(items, fmt.Sprintf("item-%d-%d", page, item))
			}
			if page+1 < pages {
				next := fmt.Sprintf("opaque-%d", page+1)
				return items, &next, true, nil
			}
			return items, nil, false, nil
		}, func() []string {
			for page := range pages {
				for item := range count {
					all = append(all, fmt.Sprintf("item-%d-%d", page, item))
				}
			}
			return all
		}(), &calls
}

func allPagesControl(maxItems uint32, maxBytes uint64) sdk.ContinuationControl {
	control := sdk.AllPagesContinuationControl()
	control.MaxItems, control.MaxBytes = maxItems, maxBytes
	return control
}

// The byte ceiling is measured against the marshalled accumulation, so it must
// hold to the byte: the exact encoded length is accepted and one byte less is
// refused. Nothing covered this, and the check is the only thing standing
// between a large product collection and the host frame limit.
// The byte ceiling is measured against the marshalled accumulation, so it must
// hold to the byte: the exact encoded length is accepted and one byte less is
// refused. That exactness is also what proves the incremental byte counter is
// byte-identical to marshalling the whole accumulation, so the shapes below
// vary page count and page size, including an empty trailing page.
func TestCollectGeneratedPagesEnforcesTheByteCeilingToTheExactByte(t *testing.T) {
	t.Parallel()

	for _, shape := range []struct{ pages, count int }{
		{pages: 1, count: 0},
		{pages: 1, count: 1},
		{pages: 3, count: 2},
		{pages: 5, count: 1},
		{pages: 4, count: 7},
	} {
		t.Run(fmt.Sprintf("%dx%d", shape.pages, shape.count), func(t *testing.T) {
			t.Parallel()

			_, all, _ := pagedFetch(shape.pages, shape.count)
			encoded, err := json.Marshal(all)
			if err != nil {
				t.Fatalf("encode accumulation: %v", err)
			}
			exact := uint64(len(encoded))

			fetch, _, _ := pagedFetch(shape.pages, shape.count)
			items, page, err := collectGeneratedPages(allPagesControl(10000, exact), nil, fetch)
			if err != nil {
				t.Fatalf("collectGeneratedPages() = %v, want the exact-length accumulation accepted", err)
			}
			if page != nil {
				t.Fatalf("page info = %#v, want nil for a complete traversal", page)
			}
			got, err := json.Marshal(items)
			if err != nil {
				t.Fatalf("encode result: %v", err)
			}
			if uint64(len(got)) != exact {
				t.Fatalf("accumulation = %d bytes, want %d", len(got), exact)
			}

			fetch, _, _ = pagedFetch(shape.pages, shape.count)
			if _, _, err := collectGeneratedPages(allPagesControl(10000, exact-1), nil, fetch); !isBudgetExhausted(err) {
				t.Fatalf("collectGeneratedPages() = %v, want %s one byte under the exact length", err, sdk.FailureBudgetExhausted)
			}
		})
	}
}

func TestCollectGeneratedPagesEnforcesTheItemCeilingToTheExactItem(t *testing.T) {
	t.Parallel()

	fetch, all, _ := pagedFetch(3, 2)
	if len(all) != 6 {
		t.Fatalf("fixture accumulates %d items, want 6", len(all))
	}
	if _, _, err := collectGeneratedPages(allPagesControl(6, 1<<20), nil, fetch); err != nil {
		t.Fatalf("collectGeneratedPages() = %v, want the exact-count accumulation accepted", err)
	}

	fetch, _, _ = pagedFetch(3, 2)
	if _, _, err := collectGeneratedPages(allPagesControl(5, 1<<20), nil, fetch); !isBudgetExhausted(err) {
		t.Fatalf("collectGeneratedPages() = %v, want %s one item over the ceiling", err, sdk.FailureBudgetExhausted)
	}
}

// An over-ceiling accumulation must be refused as soon as it is over, not after
// the remaining pages have been fetched: the request budget is the host's, and
// a doomed traversal must not keep spending it.
func TestCollectGeneratedPagesStopsFetchingOnceTheCeilingIsExceeded(t *testing.T) {
	t.Parallel()

	fetch, _, calls := pagedFetch(10, 2)
	if _, _, err := collectGeneratedPages(allPagesControl(3, 1<<20), nil, fetch); !isBudgetExhausted(err) {
		t.Fatalf("collectGeneratedPages() = %v, want %s", err, sdk.FailureBudgetExhausted)
	}
	if *calls != 2 {
		t.Fatalf("fetched %d pages, want 2 — the page that crossed the ceiling and no more", *calls)
	}
}

func isBudgetExhausted(err error) bool {
	var failure sdk.Failure
	return errors.As(err, &failure) && failure.Code == string(sdk.FailureBudgetExhausted)
}
