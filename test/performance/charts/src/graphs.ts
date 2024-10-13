import {NAMED_COLORS} from "./colors";
import ChartJsImage from "chartjs-to-image";
import {ChartConfiguration, ChartDataset, Chart} from "chart.js";
import annotationPlugin from 'chartjs-plugin-annotation';

Chart.register(annotationPlugin);

export const exportTPSGraph = async (configuration: {output: string}, result: BenchmarkResult) => {

    const scripts = [];
    for (let script in result) {
        scripts.push(script);
    }

    const reportsForAnyScript = result[scripts[0]];
    if (!reportsForAnyScript) {
        throw new Error("no data");
    }

    const datasets = scripts.map(((script, index): ChartDataset => {
        return {
            label: script,
            data: result[script].map(r => r.TPS),
            backgroundColor: NAMED_COLORS[index % scripts.length],
        }
    }));

    const config: ChartConfiguration = {
        type: 'bar',
        data: {
            labels: reportsForAnyScript
                .map(r => r.Configuration.Name),
            datasets: datasets
        },
        options: {
            plugins: {
                title: {
                    display: true,
                    text: 'TPS'
                },
            },
            responsive: true,
            interaction: {
                intersect: false,
            },
            scales: {
                x: {
                    stacked: true,
                },
                y: {
                    stacked: true
                }
            }
        }
    };

    const chart = new ChartJsImage();
    chart.setConfig(config);
    await chart.toFile(configuration.output);
}

export const exportLatencyGraph = async (configuration: {output: string}, key: keyof MetricsTime, result: BenchmarkResult) => {
    const scripts = [];
    for (let script in result) {
        scripts.push(script);
    }

    const reportsForAnyScript = result[scripts[0]];
    if (!reportsForAnyScript) {
        throw new Error("no data");
    }

    const datasets = scripts.map(((script, index): ChartDataset => {
        return {
            label: script,
            data: result[script].map(r => parseFloat(r.Metrics.Time[key].substring(0, r.Metrics.Time[key].length-2))),
            backgroundColor: NAMED_COLORS[index % scripts.length],
        }
    }));

    const config: ChartConfiguration = {
        type: 'bar',
        data: {
            labels: reportsForAnyScript
                .map(r => r.Configuration.Name),
            datasets: datasets
        },
        options: {
            plugins: {
                title: {
                    display: true,
                    text: 'TPS'
                },
            },
            interaction: {
                intersect: false,
            },
            scales: {
                x: {
                    stacked: true,
                },
                y: {
                    stacked: true
                }
            }
        }
    };

    const chart = new ChartJsImage();
    chart.setConfig(config);
    await chart.toFile(configuration.output);
}

export const exportDatabaseStats = async (
    output: string,
    result: BenchmarkResult,
) => {

    const scope = 'github.com/uptrace/opentelemetry-go-extra/otelsql';

    const scripts = [];
    for (let script in result) {
        scripts.push(script);
    }

    const reportsForAnyScript = result[scripts[0]];
    if (!reportsForAnyScript) {
        throw new Error("no data");
    }

    const datasets = scripts.map(((script, index): ChartDataset => {
        return {
            label: script,
            data: result[script].map(r => r.InternalMetrics.ScopeMetrics
                .find(scopeMetric => scopeMetric.Scope.Name == scope)!
                .Metrics
                .find(metric => metric.Name == 'go.sql.connections_open')!
                .Data
                .DataPoints[0]
                .Value
            ),
            backgroundColor: NAMED_COLORS[index % scripts.length],
        }
    }));

    const maxConnection = reportsForAnyScript[0].InternalMetrics.ScopeMetrics
        .find(scopeMetric => scopeMetric.Scope.Name == scope)!
        .Metrics
        .find(metric => metric.Name == 'go.sql.connections_max_open')!
        .Data
        .DataPoints[0]
        .Value

    const config: ChartConfiguration = {
        type: 'bar',
        data: {
            labels: reportsForAnyScript.map(r => r.Configuration.Name),
            datasets: datasets
        },
        options: {
            plugins: {
                title: {
                    display: true,
                    text: 'Database connections'
                },
                annotation: {
                    annotations: {
                        line1: {
                            type: 'line',
                            yMin: maxConnection,
                            yMax: maxConnection,
                            borderColor: 'rgb(255, 99, 132)',
                            borderWidth: 2,
                        }
                    }
                }
            },
            interaction: {
                intersect: false,
            },
            scales: {
                x: {
                    stacked: false,
                },
                y: {
                    stacked: false
                }
            }
        }
    };

    const chart = new ChartJsImage();
    chart.setConfig(config);
    chart.setChartJsVersion('4')
    await chart.toFile(output);
}