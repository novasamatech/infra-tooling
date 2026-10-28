# infra-tooling: Repository Rules

These rules apply to the whole repository. A project directory may add its own
`AGENTS.md`; read both before changing a project.

## Layout

Every top-level directory that is not hidden is an independent project with
its own tooling, documentation and, optionally, a container image.

## Continuous Integration

The repository has one workflow, [.github/workflows/ci.yml](.github/workflows/ci.yml).
It must stay generic and extensible:

- It never names a project or a path inside a project. What runs is derived by
  [.github/ci/discover.sh](.github/ci/discover.sh) from the changed files and
  the conventions below.
- New CI behavior is added as a convention any project can adopt (a make target
  or a marker file that `discover.sh` detects, plus one generic job or step),
  never as a project-specific job, step or workflow.
- A change touches a project when it changes a file in the project directory.
  A change outside project directories (the workflow, repository files) touches
  every project.

| Convention in `<project>/` | CI behavior |
| --- | --- |
| `ci` target in `Makefile` | `make -C <project> ci` on pull requests and pushes to `main` that touch the project |
| `ci-integration` target in `Makefile` | an additional step: `make -C <project> ci-integration` |
| `Dockerfile` and `.version` | on a push to `main` that changes `.version`: build for linux/amd64 and linux/arm64 and publish `ghcr.io/<repository>/<project>:latest` and `:<version>`, only when no check of that push failed |

Contract for the CI targets:

- Run every task in containers through `CONTAINER_ENGINE`; CI passes `docker`,
  so the host needs only make and the engine.
- Use no secrets and contact nothing but package and image registries. Tests
  that need live services or credentials stay outside these targets.
- `ci` holds the fast checks: lint and offline tests. `ci-integration` holds
  heavier checks with local service containers (databases, object storage) and
  image builds, and removes what it starts.
- Declare both targets literally (`ci:` at the start of a line), so that
  discovery finds them.

Check workflow changes with `actionlint` and dry-run the selection with
`BASE=<commit> HEAD=<commit> PUBLISH=true .github/ci/discover.sh`.

## Public Repository

- The repository is public: keep provider, host, network, capacity and other
  details of real environments out of code, docs, tests, commits and PR texts.
- Commits use the configured Git identity without AI/agent attribution or
  `Co-authored-by` trailers. Do not commit or push without an explicit request.
