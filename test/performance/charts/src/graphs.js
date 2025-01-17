"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.exportDatabaseStats = exports.exportLatencyGraph = exports.exportTPSGraph = void 0;
const colors_1 = require("./colors");
const chartjs_to_image_1 = __importDefault(require("chartjs-to-image"));
const chart_js_1 = require("chart.js");
const chartjs_plugin_annotation_1 = __importDefault(require("chartjs-plugin-annotation"));
chart_js_1.Chart.register(chartjs_plugin_annotation_1.default);
const exportTPSGraph = (configuration, result) => __awaiter(void 0, void 0, void 0, function* () {
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
            data: result[script].map(r => r.TPS),
            backgroundColor: colors_1.NAMED_COLORS[index % scripts.length],
        };
    }));
    const config = {
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
    const chart = new chartjs_to_image_1.default();
    chart.setConfig(config);
    yield chart.toFile(configuration.output);
});
exports.exportTPSGraph = exportTPSGraph;
const exportLatencyGraph = (configuration, key, result) => __awaiter(void 0, void 0, void 0, function* () {
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
            data: result[script].map(r => parseFloat(r.Metrics.Time[key].substring(0, r.Metrics.Time[key].length - 2))),
            backgroundColor: colors_1.NAMED_COLORS[index % scripts.length],
        };
    }));
    const config = {
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
    const chart = new chartjs_to_image_1.default();
    chart.setConfig(config);
    yield chart.toFile(configuration.output);
});
exports.exportLatencyGraph = exportLatencyGraph;
const exportDatabaseStats = (output, result) => __awaiter(void 0, void 0, void 0, function* () {
    const scope = 'github.com/uptrace/opentelemetry-go-extra/otelsql';
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
            data: result[script].map(r => r.InternalMetrics.ScopeMetrics
                .find(scopeMetric => scopeMetric.Scope.Name == scope)
                .Metrics
                .find(metric => metric.Name == 'go.sql.connections_open')
                .Data
                .DataPoints[0]
                .Value),
            backgroundColor: colors_1.NAMED_COLORS[index % scripts.length],
        };
    }));
    const maxConnection = reportsForAnyScript[0].InternalMetrics.ScopeMetrics
        .find(scopeMetric => scopeMetric.Scope.Name == scope)
        .Metrics
        .find(metric => metric.Name == 'go.sql.connections_max_open')
        .Data
        .DataPoints[0]
        .Value;
    const config = {
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
    const chart = new chartjs_to_image_1.default();
    chart.setConfig(config);
    chart.setChartJsVersion('4');
    yield chart.toFile(output);
});
exports.exportDatabaseStats = exportDatabaseStats;
