import * as fs from 'fs';
import 'chartjs-to-image';
import {exportDatabaseStats, exportLatencyGraph, exportTPSGraph} from "./src/graphs";

const main = async () => {
    let buffer = fs.readFileSync('../report/writes/report.json', 'utf-8');
    let reports = JSON.parse(buffer);
    await exportTPSGraph({
        output: '../report/writes/tps.png',
    }, reports);

    await exportDatabaseStats('../report/writes/database_connections.png', reports);

    const ps: (keyof MetricsTime)[] = ['P99', 'P95', 'P75', 'Avg']
    for (let p of ps) {
        await exportLatencyGraph({
            output: '../report/writes/' + p.toLowerCase() + '.png'
        }, p, reports);
    }
}

main();

