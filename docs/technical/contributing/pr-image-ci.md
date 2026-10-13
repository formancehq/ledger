# PR image CI isolation

`Default` owns the existing Dirty, test, build, coverage, and nightly-release
jobs. Adding a PR label never starts or cancels this workflow. The same trigger
change applies to `Repository Invariants`. Its PR triggers
are `assigned`, `opened`, `synchronize`, and `reopened`; push and merge-group
triggers remain unchanged.

`PR Images` owns the optional `Build-PR-Image` job. Adding exactly
`build-images` requests publication of the Ledger and Ledger Operator images.
On assignment, open, reopen, or synchronize, an existing `build-images` label requests
images for the new PR revision. An unrelated label skips the image workflow's
jobs before they enter any concurrency group. It can still appear as a skipped
run in Actions. The image workflow is not a required merge check.

## Validation before publication

The old image job depended on ten jobs in `Default`: Dirty, Tests,
Tests-Operator, Tests-E2E-Business, Tests-E2E-Cluster, Tests-Scenarios,
Tests-Antithesis-Workload, Tests-Model, Tests-Schemathesis, and Build.
`Wait-for-CI` preserves that dependency across workflows: it reads the newest
`main.yml` PR run for the same PR number, head SHA, and base SHA, and requires
every one of these jobs to finish with `success`. It does not run tests again.
Coverage is not an image prerequisite, as before.

The wait checks every 60 seconds for at most one hour on a small GitHub-hosted
runner. Failed, cancelled, skipped, missing, or timed-out prerequisites do not
authorize publication. A cancelled run gets two polls for a replacement to appear.
A newer failed CI run cannot fall back to an older
successful run. Closing the PR, changing its head or base, or removing the
label invalidates the request. The image job checks the PR again before
checkout and registry login. Image publication itself is not transactional;
a change after that final check can still finish publishing an image under
the previous revision's SHA-pinned tag.

Wait and publication jobs have separate concurrency groups from `Default`.
A new image request can supersede an older image request; it cannot cancel CI.
Image publication is restricted to PR branches in the same repository.
Existing image repositories, build commands, action pins, and
`pr-<number>-<head-sha7>` tags are preserved. No deployment is added.

## Review and rollout

The example targets `release/v3.0`. Merge-required CI job names stay unchanged.
The image checks move to a separate workflow and must not be added to required
checks. Unlike a `workflow_run` solution, this PR-event workflow can be tried
on a release-branch PR without first installing it on the default branch.

The cross-workflow wait is the additional mechanism introduced by the split.
Before organization-wide rollout, decide whether to share it in `formancehq/ci`
or use the existing bot to dispatch only relevant label events. Repositories
with no label-driven build or deployment need only remove `labeled` from CI.
Repositories with deployments must also preserve their tested-revision gate.

Validate the gate with the Nix-provided Node runtime:

```bash
nix develop .#ci --command node --test .github/scripts/pr-image-ci.test.cjs
```

Live verification on a test PR must cover: CI running during unrelated label
adds, `build-images` added before and after CI completes, a failing prerequisite,
a new push with the label retained, label removal, and a cancelled/re-run CI.
The invariants workflow executes the gate tests, including a prerequisite-list
drift check against `Release-Latest`. Local mocked API tests do not prove GitHub's scheduling or live check behavior.
