# Continuous integration contract

The `Default` workflow emits the stable `Required CI` check for ordinary pull
requests. It runs with `if: always()` and fails unless every job in its `needs`
graph concludes exactly `success`; failures, cancellations, skips, missing
results, and unknown conclusions are rejected.

Branch rules should require `Required CI`, not the more volatile producer job
names. Repository workflow changes do not enable that external rule: in
particular, `release/v3.0` is not enforced until a separate GitHub ruleset
change is made after the emitted check has been observed on real pull requests.

When changing CI topology:

1. Add every mandatory pull-request validation job to `Required-CI.needs` in
   `.github/workflows/main.yml`.
2. Classify an intentionally optional, informational, opt-in, or publication
   job in `.github/required-ci.json`, using its qualified
   `<workflow-path>#<job-id>` and a specific reason.
3. Do not place a mandatory pull-request producer in another workflow because
   GitHub Actions cannot express cross-workflow `needs` dependencies.

`scripts/check-repo-invariants` fails with `UNAGGREGATED_PR_JOB` when a job is
neither aggregated nor explicitly classified. It also verifies the exact
check name, the unconditional aggregate, pull-request emission without branch
or path filters, valid producer references, and the fail-closed result input.
