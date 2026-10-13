const test = require('node:test');
const assert = require('node:assert/strict');
const { requiredJobs, waitForCI } = require('./pr-image-ci.cjs');

function fixture() {
  const expected = { number: 42, head: { sha: 'head' }, base: { sha: 'base' } };
  const pr = { ...expected, state: 'open', labels: [{ name: 'build-images' }] };
  const run = { id: 123, run_number: 12, head_sha: 'head', status: 'completed',
    html_url: 'https://github.com/example/run/123', pull_requests: [expected] };
  const jobs = requiredJobs.map(name => ({ name, status: 'completed', conclusion: 'success' }));
  const state = { pr, runs: [run], jobs, calls: 0, sleeps: 0 };
  const listJobsForWorkflowRun = () => {};
  const options = {
    context: { repo: { owner: 'formancehq', repo: 'ledger' }, payload: { pull_request: expected } },
    github: {
      rest: {
        pulls: { get: async () => ({ data: state.pr }) },
        actions: { listWorkflowRuns: async args => {
          assert.equal(args.workflow_id, 'main.yml');
          assert.equal(args.event, 'pull_request');
          assert.equal(args.head_sha, 'head');
          return { data: { workflow_runs: state.runs } };
        }, listJobsForWorkflowRun },
      },
      paginate: async (method, args) => {
        assert.equal(method, listJobsForWorkflowRun);
        assert.equal(args.filter, 'latest');
        state.calls++;
        return state.jobs;
      },
    },
    core: { info() {} },
    sleep: async () => { state.sleeps++; }, attempts: 3,
  };
  return { state, options };
}

test('publishes only after every original prerequisite succeeds', async () => {
  const { options } = fixture();
  assert.equal(await waitForCI(options), 123);
});

test('waits for CI to start, then observes its success', async () => {
  const { state, options } = fixture();
  const runs = state.runs;
  state.runs = [];
  options.sleep = async () => { state.runs = runs; };
  assert.equal(await waitForCI(options), 123);
});

test('waits for pending jobs, then observes their success', async () => {
  const { state, options } = fixture();
  state.runs[0].status = 'in_progress';
  state.jobs[0].status = 'in_progress';
  options.sleep = async () => { state.jobs[0].status = 'completed'; };
  assert.equal(await waitForCI(options), 123);
});

for (const conclusion of ['failure', 'cancelled', 'skipped', 'neutral']) {
  test(`rejects a ${conclusion} prerequisite`, async () => {
    const { state, options } = fixture();
    state.jobs[0].conclusion = conclusion;
    await assert.rejects(waitForCI(options), new RegExp(`Dirty: ${conclusion}`));
  });
}

test('missing prerequisite in a completed run fails closed', async () => {
  const { state, options } = fixture();
  state.jobs.pop();
  await assert.rejects(waitForCI(options), /without all image prerequisites/);
});

for (const mutation of ['head', 'base', 'label', 'closed']) {
  test(`rejects changed PR ${mutation}`, async () => {
    const { state, options } = fixture();
    state.pr = structuredClone(state.pr);
    if (mutation === 'head' || mutation === 'base') state.pr[mutation].sha = 'changed';
    if (mutation === 'label') state.pr.labels = [];
    if (mutation === 'closed') state.pr.state = 'closed';
    await assert.rejects(waitForCI(options), /refusing to publish/);
    assert.equal(state.calls, 0);
  });
}

for (const mismatch of ['number', 'head', 'base']) {
  test(`does not accept a run for another PR ${mismatch}`, async () => {
    const { state, options } = fixture();
    const candidate = structuredClone(state.runs[0].pull_requests[0]);
    if (mismatch === 'number') candidate.number = 43;
    else candidate[mismatch].sha = 'other';
    state.runs[0].pull_requests = [candidate];
    await assert.rejects(waitForCI(options), /Timed out/);
    assert.equal(state.calls, 0);
  });
}

test('a newer failed run cannot fall back to an older successful one', async () => {
  const { state, options } = fixture();
  state.runs.push({ ...state.runs[0], id: 124, run_number: 13 });
  options.github.paginate = async (_, args) => {
    assert.equal(args.run_id, 124);
    return state.jobs.map(job => ({ ...job, conclusion: 'failure' }));
  };
  await assert.rejects(waitForCI(options), /Dirty: failure/);
});

test('does not wait on Coverage, which was not an image prerequisite', async () => {
  const { state, options } = fixture();
  state.jobs.push({ name: 'Coverage', status: 'completed', conclusion: 'failure' });
  assert.equal(await waitForCI(options), 123);
});

test('bounded wait fails when CI never starts', async () => {
  const { state, options } = fixture();
  state.runs = [];
  await assert.rejects(waitForCI(options), /Timed out/);
  assert.equal(state.sleeps, 2);
});


test('a superseding run can appear after cancellation before it is selected', async () => {
  const { state, options } = fixture();
  state.runs[0].conclusion = 'cancelled';
  options.sleep = async () => {
    state.runs = [{ ...state.runs[0], id: 124, run_number: 13, conclusion: 'success' }];
  };
  assert.equal(await waitForCI(options), 124);
});

test('image gate retains the same prerequisites as Release-Latest', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const workflow = fs.readFileSync(path.join(__dirname, '../workflows/main.yml'), 'utf8');
  const release = workflow.split('  Release-Latest:\n')[1];
  assert.ok(release, 'Release-Latest must retain its prerequisite contract');
  const needs = release.match(/    needs:\s*\[([\s\S]*?)\]/);
  assert.ok(needs, 'Release-Latest must declare its needs list');
  const names = needs[1].split(',').map(name => name.trim()).filter(Boolean);
  assert.deepEqual([...requiredJobs].sort(), names.sort());
});
