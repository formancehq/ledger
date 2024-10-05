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
exports.exportLatencyGraph = exports.exportTPSGraph = void 0;
const colors_1 = require("./colors");
const chartjs_to_image_1 = __importDefault(require("chartjs-to-image"));
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
            data: result[script].map(r => r.tps),
            backgroundColor: colors_1.NAMED_COLORS[index % scripts.length],
        };
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
            data: result[script].map(r => r.metrics.Time[key].substring(0, r.metrics.Time[key].length - 2)),
            backgroundColor: colors_1.NAMED_COLORS[index % scripts.length],
        };
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
    const chart = new chartjs_to_image_1.default();
    chart.setConfig(config);
    yield chart.toFile(configuration.output);
});
exports.exportLatencyGraph = exportLatencyGraph;
