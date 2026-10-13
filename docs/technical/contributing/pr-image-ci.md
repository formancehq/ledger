# Label-triggered PR images

The `Default` and `Repository Invariants` workflows do not listen for PR label events. Labels added by review bots therefore do not restart or cancel normal validation.

## Separate image workflow

`PR Images` builds and publishes Ledger and Ledger Operator images through GoReleaser. It runs independently of `Default`: it does not wait for tests or run them again. An image may therefore be available even when normal CI fails. Publishing an image does not validate the PR or authorize a merge.

- Adding `build-images` to a same-repository PR starts the image job.
- Opening, assigning, reopening, or pushing to a PR with `build-images` still present also starts the job.
- Other label events skip the image job before it joins concurrency, so review labels cannot cancel an active build.
- Fork PRs do not run the publishing job.
- A new eligible request cancels the previous image job for the same PR. Image concurrency is separate from normal CI.

The workflow checks out the exact PR head commit and publishes both images with `pr-<number>-<first-seven-head-SHA-characters>`. It respects the existing `IMAGE_REPOSITORY` and `OPERATOR_IMAGE_REPOSITORY` repository variables. Removing the label prevents subsequent requests; it does not revoke an image or stop an already-running job.

## Build and publication

GoReleaser builds the binaries and publishes Linux amd64 and arm64 images. Buildx remains its container builder, as required by the multiarchitecture Docker configuration. There is no separate direct-Docker PR build pipeline. The job requires the existing `GORELEASER_KEY` secret and the registry login.

Ledger inherits the canonical GoReleaser release builds, including the `databricks` tag. The previous direct-Docker PR recipe used `azure` instead. PR images now exercise the release feature set (`kafka,nats,clickhouse,databricks,s3,pyroscope`). The operator has its own GoReleaser PR configuration because its existing release image recipe uses Docker directly.

The PR configuration disables GitHub releases, and the recipe explicitly skips the release pipe and removes GitHub token environment variables. It publishes no `latest` image and invokes no Helm publishing recipe. The first authenticated run must confirm that the Pro nightly path does not attempt to update the shared GitHub nightly release. Both expected image references must resolve to non-empty digests before the job succeeds. PR images are for testing the requested commit.

There are no custom JavaScript polling scripts and no additional Node dependency in the CI shell. Tests remain in the normal validation workflow; their job names and dependencies are unchanged.

## Validation and scope

This is a pilot for `release/v3.0`. Other branches and repositories retain their existing workflows. Local checks cover workflow syntax, label routing, the unchanged normal CI jobs, and the GoReleaser configuration. Actual authenticated multiarchitecture image publication requires a live `build-images` run; local validation alone does not establish that publication succeeded.
