// Match the old Build-PR-Image needs list, not optional image/coverage checks.
const requiredJobs = [
  'Dirty', 'Tests', 'Tests-Operator', 'Tests-E2E-Business',
  'Tests-E2E-Cluster', 'Tests-Scenarios', 'Tests-Antithesis-Workload',
  'Tests-Model', 'Tests-Schemathesis', 'Build',
];

async function waitForCI({ github, context, core,
  sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
  attempts = 60, intervalMs = 60_000 }) {
  const expected = context.payload.pull_request;
  let cancelledObservations = 0;
  for (let attempt = 0; attempt < attempts; attempt++) {
    const { data: pr } = await github.rest.pulls.get({
      ...context.repo, pull_number: expected.number,
    });
    if (pr.state !== 'open' || pr.head.sha !== expected.head.sha ||
        pr.base.sha !== expected.base.sha ||
        !pr.labels.some(label => label.name === 'build-images')) {
      throw new Error('The PR revision or build-images opt-in changed; refusing to publish.');
    }

    const { data } = await github.rest.actions.listWorkflowRuns({
      ...context.repo, workflow_id: 'main.yml', event: 'pull_request',
      head_sha: expected.head.sha, per_page: 100,
    });
    // A successful run for another PR or merge base is not validation of this
    // request. Only the newest matching run/attempt can authorize publication.
    const run = data.workflow_runs
      .filter(run => run.head_sha === expected.head.sha &&
        run.pull_requests.some(candidate => candidate.number === expected.number &&
          candidate.base.sha === expected.base.sha &&
          candidate.head.sha === expected.head.sha))
      .sort((a, b) => b.run_number - a.run_number)[0];
    if (run) {
      const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {
        ...context.repo, run_id: run.id, filter: 'latest', per_page: 100,
      });
      const prerequisites = requiredJobs.map(name => jobs.find(job => job.name === name));
      const failed = prerequisites.find(job => job?.status === 'completed' && job.conclusion !== 'success');
      // A superseding run can appear shortly after the old one is cancelled.
      // Allow two polls for it to become visible, without accepting old success.
      if ((run.conclusion === 'cancelled' || failed?.conclusion === 'cancelled') &&
          ++cancelledObservations <= 2) {
        core.info(`Waiting for a replacement of cancelled Default CI: ${run.html_url}`);
        if (attempt + 1 < attempts) await sleep(intervalMs);
        continue;
      }
      cancelledObservations = 0;
      if (failed) {
        throw new Error(`Default CI prerequisite ${failed.name}: ${failed.conclusion} (${run.html_url})`);
      }
      if (prerequisites.every(job => job?.status === 'completed' && job.conclusion === 'success')) {
        core.info(`Validated all image prerequisites: ${run.html_url}`);
        return run.id;
      }
      if (run.status === 'completed') {
        throw new Error(`Default CI finished without all image prerequisites succeeding (${run.html_url})`);
      }
      core.info(`Waiting for Default CI prerequisites: ${run.html_url}`);
    } else {
      core.info(`Waiting for Default CI to start for PR #${expected.number} at ${expected.head.sha}`);
    }
    if (attempt + 1 < attempts) await sleep(intervalMs);
  }
  throw new Error('Timed out waiting for Default CI; no image was published.');
}

module.exports = { requiredJobs, waitForCI };
