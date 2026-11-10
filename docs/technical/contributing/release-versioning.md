# Release Versioning and Version-Line Markers

Ledger development builds carry SemVer-valid versions, as specified by RFC-0013
(SemVer-Valid Development Versions for Stack Modules,
`formancehq/internal-rfcs` `rfcs/0013-semver-valid-development-versions.md`).
This page explains how those versions are computed and what to do when you cut
a release branch.

## Development versions

`Release-Latest` in `.github/workflows/main.yml` runs
[`formancehq/ci/actions/dev-version`](https://github.com/formancehq/ci/blob/main/docs/actions/dev-version.md)
and passes the result to GoReleaser as `DEV_VERSION`. The nightly images and
archives on `main` are versioned from it:

| Highest SemVer tag reachable from the commit | Nightly version |
|---|---|
| stable `vX.Y.Z` | `vX.Y.(Z+1)-dev.N.g<sha>` |
| prerelease `vX.Y.Z-P`, including a marker `vX.Y.0-0` | `vX.Y.Z-P.dev.N.g<sha>` |

`N` is the number of commits since that tag. Images also keep a
`nightly-<short-commit>` alias tag. Release images are not affected.

PR images (`Build-PR-Image` in `.github/workflows/pr-images.yml`) keep their
`pr-<num>-<sha7>` and `pr-<num>-<sha7>-run-<id>-<attempt>` tags. The same image
is also tagged with the development version of the PR head commit, computed on
the line of the PR's base branch, for example `v3.0.0-rc.3.dev.39.g3a0ae49`.
The `g<sha>` part is the same commit as `<sha7>`. The squash commit built on
`main` after merge is a different commit, so it gets its own version.

Only tags in the commit's history count. A release tagged on a release branch is
not visible from `main`, so `main` needs a marker once it starts the next
version line.

## Cutting a release branch

When `release/vX.Y` is cut from `main` and `main` moves on to the next minor
version, tag the branch point with the marker `vX.(Y+1).0-0`. For `release/vX`,
the marker is `v(X+1).0.0-0`. Without it, `main` nightlies keep the old line's
versions, such as `v3.0.0-rc.3.dev.N` while `main` is already 3.1 work.

Skip the marker if `main` still produces that line's releases, for example RCs
tagged on `main` while `release/vX.Y` only points at a commit.

1. **Create the marker through CI only.** Never push a `-0` tag by hand.
   - A pushed tag runs `release.yml` *from the tagged commit*. The branch point
     can predate the `'!v*.*.*-0'` exclusion, so a hand-pushed marker would be
     published as a release.
   - Tags pushed by a workflow with `GITHUB_TOKEN` do not trigger other
     workflows.
2. **If `.github/workflows/mark-version-line.yml` does not exist yet, add it.**
   It calls the shared workflow, as documented in
   [mark-version-line](https://github.com/formancehq/ci/blob/main/docs/actions/mark-version-line.md):

   ```yaml
   name: Mark Version Line
   on:
     create:
     workflow_dispatch:
       inputs:
         branch:
           description: Release branch to mark (release/vX.Y or release/vX)
           required: true
         dry-run:
           type: boolean
           default: true

   jobs:
     Mark:
       if: github.event_name == 'workflow_dispatch' || (github.ref_type == 'branch' && startsWith(github.ref_name, 'release/'))
       uses: formancehq/ci/.github/workflows/mark-version-line.yml@v1
       permissions:
         contents: write
       with:
         branch: ${{ inputs.branch || github.ref_name }}
         dry-run: ${{ inputs.dry-run || false }}
   ```

   - **The `create` trigger:** once the workflow is on `main`, it marks every
     new `release/*` branch automatically.
   - **The `workflow_dispatch` trigger:** use it to mark a branch that already
     exists.
3. **Run it.** For a branch that already exists, dispatch the workflow on
   `main` with `branch: release/vX.Y`.
   - Run it once with `dry-run: true` and check the marker and branch point in
     the job log.
   - Then run it again with `dry-run: false`.
4. **Verify.** The next `main` nightly is versioned `vX.(Y+1).0-0.dev.N.g<sha>`.
   Keep the `'!v*.*.*-0'` exclusion in `release.yml`.

## Pending: v3.1 marker

`release/v3.0` was cut at `v3.0.0-rc.3` (`0e044f13`) without a marker, so
`main` nightlies are `v3.0.0-rc.3.dev.N.g<sha>`.

When `main` becomes v3.1 work, create `v3.1.0-0` on `0e044f13` with the steps
above. Dispatch the workflow with `branch: release/v3.0`.

### `-rc` in nightly versions

While the highest tag in `main`'s history is an RC, nightly versions contain
`-rc` (`v3.0.0-rc.N.dev.M.g<sha>`). The operator's RC upgrade check
(`misc/operator/internal/controller/rc_upgrade.go`) treats any image tag
containing `-rc` as an RC and requires the exact `vX.Y.Z-rc.N` form, so it
rejects such a nightly as an upgrade target.

This ends at whichever comes first:

- `v3.0.0` is tagged on a commit in `main`'s history, and nightlies become
  `v3.0.1-dev.N.g<sha>`;
- the `v3.1.0-0` marker is created, and nightlies become `v3.1.0-0.dev.N.g<sha>`.

If `v3.0.0` is tagged only on `release/v3.0`, `main` keeps `-rc` versions
until the marker exists.
