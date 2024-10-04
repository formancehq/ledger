//go:build it

package performance_test

import (
	"encoding/csv"
	"fmt"
	. "github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

var scripts = map[string]func(int) (string, map[string]string){
	"world->bank":         worldToBank,
	"world->any":          worldToAny,
	"any(unbounded)->any": anyUnboundedToAny,
	"any(bounded)->any": anyBoundedToAny,
}

func worldToBank(_ int) (string, map[string]string) {
	return `
send [USD/2 100] (
	source = @world
	destination = @bank
)`, nil
}

func worldToAny(id int) (string, map[string]string) {
	return `
vars {
	account $destination
}
send [USD/2 100] (
	source = @world
	destination = $destination
)`, map[string]string{
			"destination": fmt.Sprintf("dst:%d", id),
		}
}

func anyUnboundedToAny(id int) (string, map[string]string) {
	return `
vars {
	account $source
	account $destination
}
send [USD/2 100] (
	source = $source allowing unbounded overdraft
	destination = $destination
)`, map[string]string{
			"source":      fmt.Sprintf("src:%d", id),
			"destination": fmt.Sprintf("dst:%d", id),
		}
}

func anyBoundedToAny(id int) (string, map[string]string) {
	return fmt.Sprintf(`
vars {
	account $source
	account $destination
}
send [USD/2 100] (
	source = $source allowing overdraft up to [USD/2 %d]
	destination = $destination
)`, (id+1)*100), map[string]string{
			"source":      fmt.Sprintf("src:%d", id),
			"destination": fmt.Sprintf("dst:%d", id),
		}
}

// todo: add response time
func BenchmarkWrite(b *testing.B) {

	// Execute benchmarks
	reports := New(b, envFactories, scripts).Run(logging.TestingContext())

	// Write report
	if reportDir != "" {
		require.NoError(b, os.MkdirAll(reportDir, 0755))

		featureComparisonCSVLocation := filepath.Join(reportDir, "features_comparison_tps.csv")
		featureComparisonCSVFile, err := os.Create(featureComparisonCSVLocation)
		require.NoError(b, err)

		w := csv.NewWriter(featureComparisonCSVFile)
		w.Comma = ' '

		scripts := Keys(reports)
		sort.Strings(scripts)

		csvLine := make([]string, 0)
		csvLine = append(csvLine, "scenario")
		csvLine = append(csvLine, scripts...)
		require.NoError(b, w.Write(csvLine))

		for line := 0 ; line < len(reports[scripts[0]]) ; line++ {
			csvLine = make([]string, 0)
			csvLine = append(csvLine, reports[scripts[0]][line].Configuration.Name)
			for j := 0 ; j < len(scripts) ; j++ {
				csvLine = append(csvLine, fmt.Sprint(reports[scripts[j]][line].TPS()))
			}
			require.NoError(b, w.Write(csvLine))
		}

		w.Flush()
	}
}