import * as fs from 'fs';
import 'chartjs-to-image';
import {exportLatencyGraph, exportTPSGraph} from "./src/graphs";

const main = async () => {
    let buffer = fs.readFileSync('../report/report.json', 'utf-8');
    let reports = JSON.parse(buffer);
    await exportTPSGraph({
        output: 'tps.png',
    }, reports);

    const ps: (keyof MetricsTime)[] = ['P99', 'P95', 'P75', 'Avg']
    for (let p of ps) {
        await exportLatencyGraph({
            output: p.toLowerCase() + '.png'
        }, p, reports);
    }
}

main();

