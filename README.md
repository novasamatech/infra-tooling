# infra-tooling
Tools for automatization and infrastructure management

## Continuous integration

One generic workflow serves every project directory: a project opts into checks
with `ci` / `ci-integration` make targets and into image publishing with a
`Dockerfile` and `.version`. See [AGENTS.md](AGENTS.md) for the conventions.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
