package http

import (
	"math/big"
	"net/http"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// aggregateVolumesResponseJSON is the camelCase JSON DTO for AggregateResult.
type aggregateVolumesResponseJSON struct {
	Volumes []*aggregatedVolumeJSON       `json:"volumes"`
	Groups  []*groupedAggregateResultJSON `json:"groups,omitempty"`
}

type aggregatedVolumeJSON struct {
	Asset string `json:"asset"`
	// Color is always emitted (even when empty) so clients can distinguish
	// the uncolored bucket from an older response shape that didn't carry
	// the color dimension at all.
	Color   string `json:"color"`
	Input   string `json:"input"`
	Output  string `json:"output"`
	Balance string `json:"balance"`
}

type groupedAggregateResultJSON struct {
	Prefix  string                  `json:"prefix"`
	Volumes []*aggregatedVolumeJSON `json:"volumes"`
}

func toAggregatedVolumeJSON(v *commonpb.AggregatedVolume) *aggregatedVolumeJSON {
	input := v.GetInput().ToBigInt()
	output := v.GetOutput().ToBigInt()
	balance := new(big.Int).Sub(input, output)

	return &aggregatedVolumeJSON{
		Asset:   v.GetAsset(),
		Color:   v.GetColor(),
		Input:   input.String(),
		Output:  output.String(),
		Balance: balance.String(),
	}
}

func toAggregateVolumesJSON(result *commonpb.AggregateResult) *aggregateVolumesResponseJSON {
	resp := &aggregateVolumesResponseJSON{}

	resp.Volumes = make([]*aggregatedVolumeJSON, 0, len(result.GetVolumes()))
	for _, v := range result.GetVolumes() {
		resp.Volumes = append(resp.Volumes, toAggregatedVolumeJSON(v))
	}

	if len(result.GetGroups()) > 0 {
		resp.Groups = make([]*groupedAggregateResultJSON, 0, len(result.GetGroups()))
		for _, g := range result.GetGroups() {
			group := &groupedAggregateResultJSON{
				Prefix:  g.GetPrefix(),
				Volumes: make([]*aggregatedVolumeJSON, 0, len(g.GetVolumes())),
			}
			for _, v := range g.GetVolumes() {
				group.Volumes = append(group.Volumes, toAggregatedVolumeJSON(v))
			}

			resp.Groups = append(resp.Groups, group)
		}
	}

	return resp
}

// handleAggregateVolumes handles GET /{ledgerName}/volumes.
func (s *Server) handleAggregateVolumes(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	useMaxPrecision := queryParamBool(r, "useMaxPrecision")
	collapseColors := queryParamBool(r, "collapseColors")

	var groupByPrefixes []string
	if g := r.URL.Query().Get("groupByPrefixes"); g != "" {
		groupByPrefixes = strings.Split(g, ",")
	}

	// The `filter` query parameter accepts either the textual filterexpr grammar
	// or the structured v2 JSON DSL (EN-1511) and is the sole account selector.
	// It is compiled for the Accounts target and forwarded unchanged; aggregation
	// options (precision, grouping, color collapsing) stay independent of it.
	filter, ok := parseListFilter(w, r, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	if !ok {
		return
	}

	ctx, profile := query.WithProfile(r.Context())

	result, err := s.backend.AggregateVolumes(ctx, ledgerName, filter, query.AggregateOptions{
		UseMaxPrecision: useMaxPrecision,
		CollapseColors:  collapseColors,
		GroupByPrefixes: groupByPrefixes,
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	if wantsHTTPProfile(r) {
		writeProfileHeader(w, profile)
	}

	writeOK(w, toAggregateVolumesJSON(result))
}
