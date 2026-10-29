# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0
#
# Developer workflow for the Cloudflare exporter (script.py).
#
# Every Python task runs in a local Python 3.13 image built from Dockerfile.dev,
# with the sources mounted at /src; no host virtualenv is needed. The image tag
# hashes the pinned requirements and Dockerfile.dev, so it is rebuilt only when
# they change. Token-dependent targets read CF_EXPORTER_API_TOKEN from the
# environment and pass it to the container through stdin.

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT          := $(dir $(MAKEFILE_PATH))
CONTAINER_ENGINE ?= podman
IMAGE            ?= ghcr.io/novasamatech/infra-tooling/cf-exporter
VERSION          := $(shell sed -n '/[^[:space:]]/{s/[[:space:]]//g;p;q;}' "$(ROOT).version")
DEV_HASH         := $(shell cat "$(ROOT)requirements.txt" "$(ROOT)requirements-dev.txt" "$(ROOT)Dockerfile.dev" \
                      | { sha256sum 2>/dev/null || shasum -a 256; } | cut -c1-16)
# Local-only name: the dev image is never published.
DEV_REPO         := cf-exporter-dev
DEV_IMAGE        ?= $(DEV_REPO):$(DEV_HASH)
PORT             ?= 8000
SMOKE_TIMEOUT    ?= 180
PROFILE_ARGS     ?= --cycles 2

# Keep generated source/coverage files owned by the operator with either engine.
ifneq ($(findstring podman,$(CONTAINER_ENGINE)),)
USER_FLAGS := --userns=keep-id
else
USER_FLAGS := --user $(shell id -u):$(shell id -g)
endif
DEV_RUN = $(CONTAINER_ENGINE) run --rm $(USER_FLAGS) -v "$(ROOT):/src" -w /src
# Settings are passed by name: a variable unset on the host stays unset.
CONFIG_ENV = --env CF_EXPORTER_REQUEST_TIMEOUT --env CF_EXPORTER_SCRAPE_INTERVAL \
             --env CF_EXPORTER_METRICS_PORT --env CF_EXPORTER_LOGLEVEL \
             --env CF_EXPORTER_GROUP_LIMIT --env CF_EXPORTER_MAX_SLICE_REQUESTS \
             --env CF_EXPORTER_MAX_EXTRA_SELECTIONS --env CF_EXPORTER_MIN_SLICE_SECONDS \
             --env CF_EXPORTER_MAX_RETRY_AFTER_SECONDS --env CF_EXPORTER_PAGE_SIZE \
             --env CF_EXPORTER_DISABLED_LABELS
# Only stdin carries the token; it never enters container arguments or metadata.
TOKEN_RUN = printf '%s\n' "$$CF_EXPORTER_API_TOKEN" | $(DEV_RUN) -i $(CONFIG_ENV)
TOKEN_ENTRY = /bin/sh -c 'IFS= read -r CF_EXPORTER_API_TOKEN; export CF_EXPORTER_API_TOKEN; exec "$$@"' sh

.DEFAULT_GOAL := help
.PHONY: help dev-image run test test-live test-all coverage lint fmt lock outdated \
        profile-live image image-run test-image clean check-token ci

help:
	@echo "cf-exporter — Python 3.13 container tasks (no host virtualenvs):"
	@echo "  make run         run the exporter on :$(PORT) (needs token)"
	@echo "  make test        offline tests with coverage"
	@echo "  make test-live   real Cloudflare API and process tests (needs token)"
	@echo "  make test-all    offline + live tests with coverage (needs token)"
	@echo "  make ci          repository CI checks: lint + offline tests"
	@echo "  make coverage    offline tests + HTML coverage in ./htmlcov"
	@echo "  make lint        ruff lint + format check"
	@echo "  make fmt         ruff auto-format + autofix"
	@echo "  make lock        resolve requirements*.in with Python 3.13"
	@echo "  make outdated    list dependency updates"
	@echo "  make profile-live  live cycle, memory and exposition measurements (needs token)"
	@echo "  make image       build $(IMAGE):$(VERSION)"
	@echo "  make image-run   run the runtime image on :$(PORT) (needs token)"
	@echo "  make test-image  build + check the runtime image (needs token)"
	@echo "  make clean       remove caches, coverage output and all dev images"
	@echo "Token-dependent targets read CF_EXPORTER_API_TOKEN from the environment."

dev-image:
	@$(CONTAINER_ENGINE) image inspect "$(DEV_IMAGE)" >/dev/null 2>&1 || \
	  $(CONTAINER_ENGINE) build -f "$(ROOT)Dockerfile.dev" -t "$(DEV_IMAGE)" "$(ROOT)"

check-token:
	@case "$$CF_EXPORTER_API_TOKEN" in \
	  *[![:space:]]*) ;; \
	  *) echo "ERROR: CF_EXPORTER_API_TOKEN is not set or is blank"; exit 1 ;; \
	esac

run: dev-image check-token
	@$(TOKEN_RUN) -p "$(PORT):$(PORT)" --env CF_EXPORTER_METRICS_PORT=$(PORT) "$(DEV_IMAGE)" $(TOKEN_ENTRY) python script.py

test: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m pytest -m "not live" --cov=script --cov-report=term-missing

# Repository CI entry point (see the root AGENTS.md); live checks need a token
# and stay out of CI.
ci: lint test

test-live: dev-image check-token
	@$(TOKEN_RUN) "$(DEV_IMAGE)" $(TOKEN_ENTRY) python -m pytest -m live -v

test-all: dev-image check-token
	@$(TOKEN_RUN) "$(DEV_IMAGE)" $(TOKEN_ENTRY) python -m pytest -v --cov=script --cov-report=term-missing

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

# For example PROFILE_ARGS='--day 2026-10-25 --cycles 2' for a completed UTC day,
# or PROFILE_ARGS='--until 2026-10-25T12:00:00Z' for a fixed window of a day.
profile-live: dev-image check-token
	@$(TOKEN_RUN) "$(DEV_IMAGE)" $(TOKEN_ENTRY) python tests/profile_live.py $(PROFILE_ARGS)

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
	@printf '%s\n' "$$CF_EXPORTER_API_TOKEN" | $(CONTAINER_ENGINE) run --rm -i \
	  $(CONFIG_ENV) --env CF_EXPORTER_METRICS_PORT=8000 -p "$(PORT):8000" "$(IMAGE):$(VERSION)" $(TOKEN_ENTRY) python script.py

# Host driver uses only stdlib; the exporter itself runs in Python 3.13. It
# forwards the exporter settings set here, except the port, to the container.
test-image: image check-token
	cd "$(ROOT)" && python3 tests/image_smoke.py \
	  "$(CONTAINER_ENGINE)" "$(IMAGE):$(VERSION)" "$(PORT)" "$(SMOKE_TIMEOUT)"

clean:
	rm -rf "$(ROOT).pytest_cache" "$(ROOT).ruff_cache" "$(ROOT)htmlcov" "$(ROOT).coverage"
	find "$(ROOT)" -name __pycache__ -type d -prune -exec rm -rf {} +
	@# Every dev image tag, including those left behind by earlier pins.
	@for image in $$($(CONTAINER_ENGINE) images --format '{{.Repository}}:{{.Tag}}' \
	    | grep -E '(^|/)$(DEV_REPO):'); do \
	  $(CONTAINER_ENGINE) image rm "$$image"; \
	done
