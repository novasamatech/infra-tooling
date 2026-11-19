# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0
#
# Developer workflow for the Cloudflare exporter (script.py). Every task for
# running, checking and debugging it is a target here.
#
# Every Python task runs in a local Python 3.13 image built from Dockerfile.dev,
# with the sources mounted at /src; no host virtualenv is needed. The image tag
# hashes the pinned requirements and Dockerfile.dev, so it is rebuilt only when
# they change. Token-dependent targets take the API token from
# CF_EXPORTER_API_TOKEN or, if that is unset, from the file TOKEN_FILE, and pass
# it to the container through stdin.
#
# Settings of one machine (CONTAINER_ENGINE, TOKEN_FILE, PORT, ...) can be kept in
# an untracked local.mk next to this file instead of on every command line.

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT          := $(dir $(MAKEFILE_PATH))
-include $(ROOT)local.mk

CONTAINER_ENGINE ?= podman
IMAGE            ?= ghcr.io/novasamatech/infra-tooling/cf-exporter
VERSION          := $(shell sed -n '/[^[:space:]]/{s/[[:space:]]//g;p;q;}' "$(ROOT).version")
DEV_HASH         := $(shell cat "$(ROOT)requirements.txt" "$(ROOT)requirements-dev.txt" "$(ROOT)Dockerfile.dev" \
                      | { sha256sum 2>/dev/null || shasum -a 256; } | cut -c1-16)
# Local-only name: the dev image is never published.
DEV_REPO         := cf-exporter-dev
DEV_IMAGE        ?= $(DEV_REPO):$(DEV_HASH)
PORT             ?= 8000
SMOKE_TIMEOUT    ?= 300
PROFILE_ARGS     ?= --hours 2
# A file with the API token on its first line, used when CF_EXPORTER_API_TOKEN is unset.
TOKEN_FILE       ?=
# Extra pytest arguments, e.g. PYTEST_ARGS='tests/test_storage.py -k wiped --no-cov'.
PYTEST_ARGS      ?=
# The container of `make run`, addressed by status, scrape, logs and stop.
RUN_NAME         ?= cf-exporter-run
# The container of a live check (test-live, test-all, profile-live), so that a
# check that takes too long can be stopped with `make stop-check`.
CHECK_NAME       ?= cf-exporter-check
# Its database, inside the throwaway container unless CF_EXPORTER_DB_PATH is set.
RUN_DB_PATH      ?= /tmp/cf-exporter/cf-exporter.sqlite3
LOG_LINES        ?= 50

# Keep generated source/coverage files owned by the operator with either engine.
ifneq ($(findstring podman,$(CONTAINER_ENGINE)),)
USER_FLAGS := --userns=keep-id
else
USER_FLAGS := --user $(shell id -u):$(shell id -g)
endif
DEV_RUN = $(CONTAINER_ENGINE) run --rm $(USER_FLAGS) -v "$(ROOT):/src" -w /src
# Settings are passed by name: a variable unset on the host stays unset.
# CF_EXPORTER_CONSUME_TOKEN is a secret and is not passed: `make scrape` and the
# checks read /analytics without consuming.
CONFIG_ENV = --env CF_EXPORTER_REQUEST_TIMEOUT --env CF_EXPORTER_METRICS_PORT \
             --env CF_EXPORTER_LOGLEVEL --env CF_EXPORTER_DB_PATH \
             --env CF_EXPORTER_BACKFILL_DAYS --env CF_EXPORTER_MAX_RETRY_AFTER_SECONDS \
             --env CF_EXPORTER_PAGE_SIZE --env CF_EXPORTER_DISABLED_LABELS \
             --env CF_EXPORTER_HISTORY_INTERVAL_SECONDS --env CF_EXPORTER_RESPONSE_SAMPLES
# Writes the token to stdout for a pipe, never to a command line or the log.
TOKEN_CMD = if [ -n "$$CF_EXPORTER_API_TOKEN" ]; then printf '%s\n' "$$CF_EXPORTER_API_TOKEN"; \
            else head -n 1 "$(TOKEN_FILE)"; fi
# Only stdin carries the token; it never enters container arguments or metadata.
TOKEN_RUN = { $(TOKEN_CMD); } | $(DEV_RUN) -i $(CONFIG_ENV)
TOKEN_ENTRY = /bin/sh -c 'IFS= read -r CF_EXPORTER_API_TOKEN; export CF_EXPORTER_API_TOKEN; exec "$$@"' sh
# A command inside the running `make run` container.
RUN_EXEC = $(CONTAINER_ENGINE) exec "$(RUN_NAME)"
RUN_URL  = http://127.0.0.1:$(PORT)

.DEFAULT_GOAL := help
.PHONY: help dev-image run status scrape logs stop stop-check test test-live test-all coverage \
        lint fmt lock outdated profile-live image image-run test-image clean check-token ci

help:
	@echo "cf-exporter — Python 3.13 container tasks (no host virtualenvs):"
	@echo "  make run         run the exporter on :$(PORT) in container $(RUN_NAME) (needs token)"
	@echo "  make status      readiness and health metrics of the running exporter"
	@echo "  make scrape      its /analytics exposition, without consuming it"
	@echo "  make logs        its last $(LOG_LINES) log lines (LOG_LINES=n)"
	@echo "  make stop        stop it"
	@echo "  make test        offline tests with coverage (PYTEST_ARGS='...' to narrow)"
	@echo "  make test-live   real Cloudflare API and process tests (needs token)"
	@echo "  make stop-check  stop a running live check (test-live, test-all, profile-live)"
	@echo "  make test-all    offline + live tests with coverage (needs token)"
	@echo "  make ci          repository CI checks: lint + offline tests"
	@echo "  make coverage    offline tests + HTML coverage in ./htmlcov"
	@echo "  make lint        ruff lint + format check"
	@echo "  make fmt         ruff auto-format + autofix"
	@echo "  make lock        resolve requirements*.in with Python 3.13"
	@echo "  make outdated    list dependency updates"
	@echo "  make profile-live  compare exported buckets with Cloudflare totals, with measurements (needs token)"
	@echo "  make image       build $(IMAGE):$(VERSION)"
	@echo "  make image-run   run the runtime image on :$(PORT) (needs token)"
	@echo "  make test-image  build + check the runtime image (needs token)"
	@echo "  make clean       remove caches, coverage output and all dev images"
	@echo "The token comes from CF_EXPORTER_API_TOKEN or the file TOKEN_FILE."
	@echo "Machine settings (CONTAINER_ENGINE, TOKEN_FILE, PORT) may live in an untracked local.mk."

dev-image:
	@$(CONTAINER_ENGINE) image inspect "$(DEV_IMAGE)" >/dev/null 2>&1 || \
	  $(CONTAINER_ENGINE) build -f "$(ROOT)Dockerfile.dev" -t "$(DEV_IMAGE)" "$(ROOT)"

check-token:
	@token="$$CF_EXPORTER_API_TOKEN"; \
	if [ -z "$$token" ] && [ -n "$(TOKEN_FILE)" ]; then \
	  token="$$(head -n 1 "$(TOKEN_FILE)" 2>/dev/null)"; \
	fi; \
	case "$$token" in \
	  *[![:space:]]*) ;; \
	  *) echo "ERROR: no API token: set CF_EXPORTER_API_TOKEN or TOKEN_FILE"; exit 1 ;; \
	esac

run: dev-image check-token
	@$(TOKEN_RUN) --name "$(RUN_NAME)" -p "127.0.0.1:$(PORT):$(PORT)" \
	  --env CF_EXPORTER_METRICS_PORT=$(PORT) \
	  --env CF_EXPORTER_DB_PATH="$${CF_EXPORTER_DB_PATH:-$(RUN_DB_PATH)}" \
	  "$(DEV_IMAGE)" $(TOKEN_ENTRY) python script.py

# The three targets below ask the exporter from inside its own container, so
# they do not depend on the host's port forwarding.
status:
	@$(RUN_EXEC) wget -qO- "$(RUN_URL)/readyz" || echo "not ready, see: make logs"
	@$(RUN_EXEC) wget -qO- "$(RUN_URL)/metrics" | grep '^cf_exporter_'

scrape:
	@$(RUN_EXEC) wget -qO- "$(RUN_URL)/analytics"

logs:
	@$(CONTAINER_ENGINE) logs --tail "$(LOG_LINES)" "$(RUN_NAME)"

stop:
	$(CONTAINER_ENGINE) stop -t 10 "$(RUN_NAME)"

stop-check:
	$(CONTAINER_ENGINE) stop -t 10 "$(CHECK_NAME)"

test: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m pytest -m "not live" --cov=script --cov-report=term-missing $(PYTEST_ARGS)

# Repository CI entry point (see the root AGENTS.md); live checks need a token
# and stay out of CI.
ci: lint test

test-live: dev-image check-token
	@$(TOKEN_RUN) --name "$(CHECK_NAME)" "$(DEV_IMAGE)" $(TOKEN_ENTRY) python -m pytest -m live -v $(PYTEST_ARGS)

test-all: dev-image check-token
	@$(TOKEN_RUN) --name "$(CHECK_NAME)" "$(DEV_IMAGE)" $(TOKEN_ENTRY) python -m pytest -v --cov=script --cov-report=term-missing $(PYTEST_ARGS)

coverage: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m pytest -m "not live" --cov=script --cov-report=term-missing --cov-report=html

lint: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m ruff check .
	$(DEV_RUN) "$(DEV_IMAGE)" python -m ruff format --check .

fmt: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m ruff format .
	$(DEV_RUN) "$(DEV_IMAGE)" python -m ruff check --fix .

outdated: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m pip list --outdated

# For example PROFILE_ARGS='--hours 6', or PROFILE_ARGS='--end 2026-11-18T12:00:00Z
# --hours 1' for a fixed hour.
profile-live: dev-image check-token
	@$(TOKEN_RUN) --name "$(CHECK_NAME)" "$(DEV_IMAGE)" $(TOKEN_ENTRY) python tests/profile_live.py $(PROFILE_ARGS)

# Resolve inside an ephemeral container; only completed lock files reach /src.
lock:
	$(DEV_RUN) python:3.13-alpine /bin/sh -ec '\
	  trap '\''rm -f /src/requirements.txt.tmp /src/requirements-dev.txt.tmp'\'' EXIT; \
	  python -m venv /tmp/cf-resolver; \
	  /tmp/cf-resolver/bin/python -m pip install --quiet --upgrade pip; \
	  /tmp/cf-resolver/bin/python -m pip install --quiet -r requirements.in; \
	  { echo "# Generated by \`make lock\` from requirements.in — do not edit by hand."; \
	    /tmp/cf-resolver/bin/python -m pip freeze; } > requirements.txt.tmp; \
	  /tmp/cf-resolver/bin/python -m pip install --quiet -r requirements-dev.in -c requirements.txt.tmp; \
	  { echo "# Generated by \`make lock\` from requirements-dev.in — do not edit by hand."; \
	    echo "-r requirements.txt"; \
	    /tmp/cf-resolver/bin/python -m pip freeze | grep -vxFf requirements.txt.tmp; } > requirements-dev.txt.tmp; \
	  mv requirements.txt.tmp requirements.txt; \
	  mv requirements-dev.txt.tmp requirements-dev.txt'

image:
	$(CONTAINER_ENGINE) build -t "$(IMAGE):$(VERSION)" "$(ROOT)"

image-run: check-token
	@{ $(TOKEN_CMD); } | $(CONTAINER_ENGINE) run --rm -i \
	  $(CONFIG_ENV) --env CF_EXPORTER_METRICS_PORT=8000 -p "127.0.0.1:$(PORT):8000" "$(IMAGE):$(VERSION)" $(TOKEN_ENTRY) python script.py

# Host driver uses only stdlib; the exporter itself runs in Python 3.13. It
# forwards the exporter settings set here, except the port and the storage
# path, to the container.
test-image: image check-token
	@cd "$(ROOT)" && CF_EXPORTER_API_TOKEN="$$($(TOKEN_CMD))" python3 tests/image_smoke.py \
	  "$(CONTAINER_ENGINE)" "$(IMAGE):$(VERSION)" "$(PORT)" "$(SMOKE_TIMEOUT)"

clean:
	rm -rf "$(ROOT).pytest_cache" "$(ROOT).ruff_cache" "$(ROOT)htmlcov" "$(ROOT).coverage"
	find "$(ROOT)" -name __pycache__ -type d -prune -exec rm -rf {} +
	@# Every dev image tag, including those left behind by earlier pins.
	@for image in $$($(CONTAINER_ENGINE) images --format '{{.Repository}}:{{.Tag}}' \
	    | grep -E '(^|/)$(DEV_REPO):'); do \
	  $(CONTAINER_ENGINE) image rm "$$image"; \
	done
