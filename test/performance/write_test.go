//go:build it

package performance_test

import (
	"bufio"
	"bytes"
	"embed"
	"encoding/csv"
	"fmt"
	"github.com/Arafatk/glot"
	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"text/template"
)

//go:embed plot
var plotFiles embed.FS

var scripts = map[string]func(int) (string, map[string]string){
	"world->bank":         worldToBank,
	"world->any":          worldToAny,
	"any(unbounded)->any": anyUnboundedToAny,
	"any(bounded)->any": anyBoundedToAny,
}

var graphsScriptsOrder = []string{
	"any(unbounded)->any",
	"any(bounded)->any",
	"world->any",
	"world->bank",
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

func BenchmarkWrite(b *testing.B) {

	// Execute benchmarks
	reports := New(b, envFactory, scripts).Run(logging.TestingContext())

	// Write report
	if reportDir != "" {
		require.NoError(b, os.MkdirAll(reportDir, 0755))

		writeFeatureComparison(b, reports)

		for _, script := range graphsScriptsOrder {
			writeScriptLatencies(b, script, reports[script])
		}
	}
}

func writeScriptLatencies(b *testing.B, name string, reports []Report) {
	datafileName := fmt.Sprintf("features_comparison_latencies_%s", name)
	featureLatenciesCSVLocation := filepath.Join(b.TempDir(), datafileName + ".csv")
	featureLatenciesCSVFile, err := os.Create(featureLatenciesCSVLocation)
	require.NoError(b, err)

	w := csv.NewWriter(featureLatenciesCSVFile)
	w.Comma = ' '

	csvLine := make([]string, 0)
	csvLine = append(csvLine, "scenario", "p99", "p95")
	require.NoError(b, w.Write(csvLine))

	for _, report := range reports {
		metrics := report.Tachymeter.Calc()

		csvLine = make([]string, 0)
		csvLine = append(csvLine, report.Configuration.Name)
		csvLine = append(csvLine, metrics.Time.P99.String())
		csvLine = append(csvLine, metrics.Time.P95.String())
		require.NoError(b, w.Write(csvLine))
	}

	w.Flush()

	writePNGUsingGNUPlotScript(b, "features_comparison_latency.gp", map[string]string{
		"Datafile": featureLatenciesCSVLocation,
		"OutputFile": datafileName + ".png",
	})
}

func writeFeatureComparison(b *testing.B, reports map[string][]Report) {

	// write csv file
	datafileName := "features_comparison_tps.csv"
	datafileLocation := filepath.Join(b.TempDir(), datafileName)
	datafile, err := os.Create(datafileLocation)
	require.NoError(b, err)

	w := csv.NewWriter(datafile)
	w.Comma = ' '

	csvLine := make([]string, 0)
	csvLine = append(csvLine, "scenario")
	csvLine = append(csvLine, graphsScriptsOrder...)
	require.NoError(b, w.Write(csvLine))

	for line := 0 ; line < len(reports[graphsScriptsOrder[0]]) ; line++ {
		csvLine = make([]string, 0)
		csvLine = append(csvLine, reports[graphsScriptsOrder[0]][line].Configuration.Name)
		for j := 0 ; j < len(scripts) ; j++ {
			csvLine = append(csvLine, fmt.Sprint(reports[graphsScriptsOrder[j]][line].TPS()))
		}
		require.NoError(b, w.Write(csvLine))
	}

	w.Flush()

	writePNGUsingGNUPlotScript(
		b,
		"features_comparison_tps.gp",
		map[string]string{
			"Datafile": datafileLocation,
			"OutputFile": "features_comparison_tps.png",
		},
	)
}

var plotFilesTemplates = template.Must(
	template.New("").ParseFS(plotFiles, "plot/*.gp"),
)

func writePNGUsingGNUPlotScript(b *testing.B, tpl string, data any) {

	output := bytes.NewBuffer(nil)

	err := plotFilesTemplates.ExecuteTemplate(output, tpl, data)
	require.NoError(b, err)

	plot, err := glot.NewPlot(2, false, false)
	require.NoError(b, err)

	scanner := bufio.NewScanner(output)
	for scanner.Scan() {
		require.NoError(b, plot.Cmd(scanner.Text()))
	}
	require.NoError(b, scanner.Err())
	require.NoError(b, plot.Close())
}