<!--
Copyright © 2026 Novasama Technologies GmbH
SPDX-License-Identifier: Apache-2.0
-->

# Changelog

All notable changes to `cf-exporter` are documented here.
This project adheres to [Semantic Versioning](https://semver.org/).

## [1.5.0] — 2026-10-26

Metric names, labels and label order of `cf_visits` / `cf_requests` are unchanged.

### Added

- `cf_cache_requests` gauge for every host of every zone, with `zone_name`,
  `host_name`, `method_name`, `path`, `cache_status`, `origin_response_status`.
- Dimensionless `cf_zone_requests` and `cf_zone_visits` day-to-date totals.
- `cf_exporter_groups` and `cf_exporter_group_limit_reached`, labelled by zone
  and dataset (`visits`, `requests`, `cache`), to expose export size and truncation.
- Optional `CF_EXPORTER_GROUP_LIMIT`: unset, empty or zero exports all groups;
  a positive value exports the largest N nonzero label sets per zone/selection.
- Time-filtered regression tests, completed-day live sum checks and a repeatable
  live profiling driver for memory, selection count and exposition cost.

### Fixed

- Default exports no longer drop groups beyond a single API page. Adaptive,
  half-open time slicing retries only full selections, discards parent pages
  and merges all child values. Slice-count hints are reused until UTC midnight.
- Full visits pages ending in zero no longer cause false truncation warnings.
  A configured cap requests one extra group to see whether it cut anything;
  truncation is decided from the pages, never from the separately fetched totals.
- Yesterday's analytics and group metrics disappear immediately at UTC midnight,
  even before the next collection finishes. Health metrics remain available.
- Any failed slice discards the entire zone's analytics for that cycle.
- A long `Retry-After` (Cloudflare asks for up to 5 minutes) no longer stalls a
  cycle for each retried request. Waits above 30 seconds fail the request at
  once, and after an HTTP 429 (or a GraphQL budget error with another status)
  the remaining zones are skipped and reported as failed until the next cycle.
- The traffic between the last cycle of a day and midnight was never exported
  (since 1.4.0, up to one scrape interval). Once a day, the cycle before
  midnight is moved earlier so that it finishes 30 seconds before midnight.
- The image smoke test and live checks no longer fail when a cycle crosses UTC
  midnight, where day-scoped families are omitted by design.

### Changed

- One initial aliased analytics request per zone includes visits, requests, cache
  and totals; sliced retries use additional HTTP requests. Settings discovery
  remains a separate request. Full slices stop at 60s and expose residual cuts.
- Slicing is bounded by a GraphQL budget shared by all zones of a cycle: two
  thirds of the documented quota (300 selections per 5 minutes), scaled to the
  scrape interval, after every zone's baseline selections; at most 32 requests
  per selection and zone. A burst of unique groups is reported as truncation
  instead of exhausting the rate limit.
- All development commands use a cached Python 3.13 Alpine container, replacing
  host virtualenvs. Pins determine the dev image tag, files retain host ownership,
  and token-dependent targets feed credentials through stdin. Lock resolution
  uses a throwaway container; the image smoke driver remains stdlib-only on host.
  `make clean` removes every local dev image tag, `make profile-live` runs the
  live profiler.
- Runtime image base and dependencies are unchanged.
- Repeated label strings are shared within each zone's collection. Two live
  cycles over the completed UTC day 2026-10-25 (46337 requests, 5013 visits and
  15308 cache groups, all equal to the zone totals) peaked at 127.14 MiB RSS,
  with 66733 custom samples and a 19.4 MiB exposition. They took 22.7 s with 42
  analytics selections, then 11.2 s with 37 once slice hints applied.

## [1.4.0] — 2026-10-13

Metric names and labels of `cf_visits` / `cf_requests` are unchanged.

### Fixed
- Zones on the Free plan exported no metrics at all: the query asked for
  `clientRequestQuery` and `clientRequestReferer`, which the Free plan does not
  offer, so Cloudflare rejected the whole query. The exporter now reads each
  zone's `availableFields` and reports dimensions the plan lacks as `unknown`.
- A transient error while listing zones terminated the process (restart loop).
  API failures are now logged and reported via metrics; the loop keeps running.
- `cf_requests` series were never cleared, so stale series (including previous
  days) were exported forever. Each cycle now replaces all series.
- Metrics were cleared before collection, so scrapes during a cycle saw partial
  data. Results are now published atomically once the cycle is complete.
- Groups above Cloudflare's 10000-group limit were cut off arbitrarily and
  silently. Groups are now ordered largest first and a warning is logged.
- Cloudflare error messages were lost (`raise_for_status`); a failed page in
  the zone listing silently returned a partial list.
- Startup configuration is validated (positive interval/timeout, port range);
  surrounding whitespace in the API token is ignored, embedded whitespace and
  non-printable or non-ASCII characters are rejected without echoing the token.
- Missing or malformed zone listings, pagination and analytics results no
  longer report a successful empty collection.
- Zones with no available dimensions use a valid aggregate-only query; empty
  time windows at midnight do not send an invalid analytics query.
- Group limits respect each zone's `maxPageSize`. Unexpected cycle failures
  also publish cycle duration.
- Tokens are redacted from API errors, tracebacks and configuration reprs.
- Container run targets deliver the token over stdin instead of storing it in
  container metadata. The image smoke check reads the full metrics response
  and verifies that metadata contains no token.
- Live process-test logs stay in memory. Failed dependency resolution cleans
  up its temporary lock files and resolver virtualenv.

### Changed
- One analytics request combines both metrics, plus a separate GraphQL
  settings request per zone (GraphQL variables, one time window per cycle).
  Rate limiting (HTTP 429)
  and 5xx errors are retried with backoff.
- Dependencies are pinned (`requirements.txt`, generated by `make lock`).
- Dockerfile: dependency layer is cached separately, port 8000 is exposed,
  `.dockerignore` keeps the build context minimal.

### Added
- Exporter health metrics: `cf_exporter_zone_scrape_success{zone_name}`,
  `cf_exporter_last_cycle_success`, `cf_exporter_last_cycle_timestamp_seconds`,
  `cf_exporter_cycle_duration_seconds`.
- Makefile with separate runtime (`venv/`) and development (`venv-dev/`)
  virtualenvs, unit tests, live tests against the real Cloudflare API (incl. a
  schema check that the used dataset/fields are not deprecated), lint, lock,
  image build and image smoke test.
