import {NAMED_COLORS} from "./colors";
import ChartJsImage from "chartjs-to-image";

export const exportTPSGraph = async (configuration: {output: string}, result: BenchmarkResult) => {

    const scripts = [];
    for (let script in result) {
        scripts.push(script);
    }

    const reportsForAnyScript = result[scripts[0]];
    if (!reportsForAnyScript) {
        throw new Error("no data");
    }

    const datasets = scripts.map(((script, index) => {
        return {
            label: script,
            data: result[script].map(r => r.tps),
            backgroundColor: NAMED_COLORS[index % scripts.length],
        }
    }));

    const config = {
        type: 'bar',
        data: {
            labels: reportsForAnyScript
                .map(r => r.configuration.name),
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

    const datasets = scripts.map(((script, index) => {
        return {
            label: script,
            data: result[script].map(r => r.metrics.Time[key].substring(0, r.metrics.Time[key].length-2)),
            backgroundColor: NAMED_COLORS[index % scripts.length],
        }
    }));

    const config = {
        type: 'bar',
        data: {
            labels: reportsForAnyScript
                .map(r => r.configuration.name),
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