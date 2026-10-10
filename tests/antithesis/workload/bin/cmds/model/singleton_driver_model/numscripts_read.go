package main

import (
	"context"
	"slices"
	"sort"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The numscript library reads. Each is held to the library of some candidate
// base, so a save racing the read is explained by the base it landed in.

func runNumscriptRead(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, _ := pickLedgerReadTarget(c.liveLedgerNamesSnapshot(), 5)
	name := random.RandomChoice(numscriptNames)
	if oneIn(16) {
		name = "absent"
	}

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()

	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	responseFrontier := c.beginResponseFrontier()

	switch random.RandomChoice(indexPool(4)) {
	case 0:
		selector := random.RandomChoice([]string{"", "latest", "1", "1.1", "0.2", "1.0.0", "01.0.0", "9.9.9", "x"})
		info, err := client.GetNumscript(readCtx, &servicepb.GetNumscriptRequest{Ledger: ledger, Name: name, Version: selector})
		c.validateGetNumscript(responseFrontier(), ledger, name, selector, info, err)
	case 1:
		stream, err := client.ListNumscripts(readCtx, &servicepb.ListNumscriptsRequest{Ledger: ledger})

		var served []*commonpb.NumscriptInfo
		if err == nil {
			served, err = drainStream(stream)
		}

		c.validateListNumscripts(responseFrontier(), ledger, served, err)
	case 2:
		resp, err := client.ListNumscriptVersions(readCtx, &servicepb.ListNumscriptVersionsRequest{Ledger: ledger, Name: name})
		c.validateListNumscriptVersions(responseFrontier(), ledger, name, resp, err)
	default:
		usage, err := client.GetTemplateUsage(readCtx, &servicepb.GetTemplateUsageRequest{Ledger: ledger, Name: name})
		c.validateTemplateUsage(responseFrontier(), ledger, name, usage, err)
	}
}

// numscriptReadError settles a failed library read: transient and shutdown
// errors carry nothing, NotFound is legal iff some base explains it, anything
// else is a finding. handled is false when there was no error.
func (c *Checker) numscriptReadError(maxTicket uint64, ledger, operation string, err error, notFound func(base oracle.GlobalState) bool) (handled bool) {
	if err == nil {
		return false
	}

	if internal.IsTransient(err) || isShutdownError(err) {
		return true
	}

	if status.Code(err) == codes.NotFound && c.matchesModel(maxTicket, operation+" NOT_FOUND", func(base oracle.GlobalState) bool {
		if lc, exists := base.Lifecycle(ledger); !exists || lc.Deleted {
			return true
		}

		return notFound != nil && notFound(base)
	}) {
		return true
	}

	assert.Unreachable("singleton_driver_model: numscript library read error outside model", internal.Details{
		"ledger":    ledger,
		"operation": operation,
		"error":     err.Error(),
	})

	return true
}

func (c *Checker) validateGetNumscript(maxTicket uint64, ledger, name, selector string, info *commonpb.NumscriptInfo, err error) {
	if c.numscriptReadError(maxTicket, ledger, "GetNumscript", err, func(base oracle.GlobalState) bool {
		_, _, ok := base.Ledger(ledger).NumscriptSelect(name, selector)

		return !ok
	}) {
		return
	}

	if c.matchesModel(maxTicket, "GETNUMSCRIPT", func(base oracle.GlobalState) bool {
		if lc, exists := base.Lifecycle(ledger); !exists || lc.Deleted {
			return false
		}

		version, content, ok := base.Ledger(ledger).NumscriptSelect(name, selector)

		return ok && info.GetName() == name && info.GetVersion() == version &&
			info.GetContent() == content && info.GetCreatedAt() != nil
	}) {
		assert.Reachable("singleton_driver_model: numscript read matched the library", internal.Details{"selector": selector})

		return
	}

	assert.Unreachable("singleton_driver_model: numscript read outside model", internal.Details{
		"ledger":   ledger,
		"name":     name,
		"selector": selector,
		"served":   info.GetVersion(),
	})
}

func (c *Checker) validateListNumscripts(maxTicket uint64, ledger string, served []*commonpb.NumscriptInfo, err error) {
	if c.numscriptReadError(maxTicket, ledger, "ListNumscripts", err, nil) {
		return
	}

	if c.matchesModel(maxTicket, "LISTNUMSCRIPTS", func(base oracle.GlobalState) bool {
		if lc, exists := base.Lifecycle(ledger); !exists || lc.Deleted {
			return false
		}

		return latestListingMatches(base.Ledger(ledger), served)
	}) {
		if len(served) > 0 {
			assert.Reachable("singleton_driver_model: numscript listing matched the library", internal.Details{"rows": len(served)})
		}

		return
	}

	assert.Unreachable("singleton_driver_model: numscript listing outside model", internal.Details{
		"ledger": ledger,
		"rows":   len(served),
	})
}

// latestListingMatches holds a ListNumscripts page — the greatest version of
// each name, ordered by name — to one base's library.
func latestListingMatches(ls oracle.LedgerState, served []*commonpb.NumscriptInfo) bool {
	names := map[string]struct{}{}
	for k := range ls.Numscripts() {
		names[k.Name] = struct{}{}
	}

	want := slices.Sorted(func(yield func(string) bool) {
		for n := range names {
			if !yield(n) {
				return
			}
		}
	})

	if len(served) != len(want) {
		return false
	}

	for i, name := range want {
		version := ls.NumscriptLatest(name)
		content, _ := ls.NumscriptContent(name, version)

		got := served[i]
		if got.GetName() != name || got.GetVersion() != version || got.GetContent() != content || got.GetCreatedAt() == nil {
			return false
		}
	}

	return true
}

func (c *Checker) validateListNumscriptVersions(maxTicket uint64, ledger, name string, resp *servicepb.ListNumscriptVersionsResponse, err error) {
	if c.numscriptReadError(maxTicket, ledger, "ListNumscriptVersions", err, nil) {
		return
	}

	served := make([]string, 0, len(resp.GetVersions()))
	for _, v := range resp.GetVersions() {
		if v.GetCreatedAt() == nil {
			served = nil

			break
		}

		served = append(served, v.GetVersion())
	}

	if served != nil && c.matchesModel(maxTicket, "NUMSCRIPTVERSIONS", func(base oracle.GlobalState) bool {
		if lc, exists := base.Lifecycle(ledger); !exists || lc.Deleted {
			return false
		}

		ls := base.Ledger(ledger)

		return resp.GetLatestVersion() == ls.NumscriptLatest(name) && slices.Equal(served, versionsHighestFirst(ls, name))
	}) {
		if len(served) > 1 {
			assert.Reachable("singleton_driver_model: numscript version history matched the library", internal.Details{"versions": len(served)})
		}

		return
	}

	assert.Unreachable("singleton_driver_model: numscript version history outside model", internal.Details{
		"ledger": ledger,
		"name":   name,
		"latest": resp.GetLatestVersion(),
		"served": served,
	})
}

func versionsHighestFirst(ls oracle.LedgerState, name string) []string {
	versions := storedVersions(ls, name)
	sort.SliceStable(versions, func(i, j int) bool {
		vi, _ := semver.Parse(versions[i])
		vj, _ := semver.Parse(versions[j])

		return vi.Compare(vj) > 0
	})

	return versions
}

// validateTemplateUsage bounds a template's usage counter by the model. The
// counter is folded from the audit chain asynchronously, so any prefix of it is
// legal, but never more than the creates that ran the template.
func (c *Checker) validateTemplateUsage(maxTicket uint64, ledger, name string, usage *commonpb.TemplateUsage, err error) {
	if c.numscriptReadError(maxTicket, ledger, "GetTemplateUsage", err, nil) {
		return
	}

	if c.matchesModel(maxTicket, "TEMPLATEUSAGE", func(base oracle.GlobalState) bool {
		if lc, exists := base.Lifecycle(ledger); !exists || lc.Deleted {
			return false
		}

		return usage.GetCount() <= base.Ledger(ledger).TemplateUsage(name)
	}) {
		if usage.GetCount() > 0 {
			assert.Reachable("singleton_driver_model: template usage within the model", internal.Details{"count": usage.GetCount()})
		}

		return
	}

	assert.Unreachable("singleton_driver_model: template usage above the model", internal.Details{
		"ledger": ledger,
		"name":   name,
		"count":  usage.GetCount(),
	})
}
