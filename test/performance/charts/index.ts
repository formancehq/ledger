import * as fs from 'fs';
import 'chartjs-to-image';
import {exportDatabaseStats, exportLatencyGraph, exportTPSGraph} from "./src/graphs";

const main = async () => {
    let buffer = fs.readFileSync('../report/report.json', 'utf-8');
    let reports = JSON.parse(buffer);
    await exportTPSGraph({
        output: '../report/tps.png',
    }, reports);

    await exportDatabaseStats('../report/database_connections.png', reports);

    const ps: (keyof MetricsTime)[] = ['P99', 'P95', 'P75', 'Avg']
    for (let p of ps) {
        await exportLatencyGraph({
            output: '../report/' + p.toLowerCase() + '.png'
        }, p, reports);
    }
}

main();

