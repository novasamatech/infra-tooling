# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0
#
# Operator and developer workflow for the SubQuery database migration tool (sqdb).
#
# Everything runs in containers; the host only needs make and Podman (or Docker).
# Operator targets run the runtime image with the migration config mounted
# read-only and WORK mounted for state, logs, reports and local dumps. By default
# that is the image CI publishes; `make image` builds it from these sources
# instead. Developer targets run the dev image with the sources mounted at /src.
# The harness starts PostgreSQL 16 and 17 and an S3 server (versitygw) on a
# private container network.

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT          := $(dir $(MAKEFILE_PATH))
CONTAINER_ENGINE ?= podman
# Builds images; podman builds through buildah, BUILD_ENGINE=buildah works too.
BUILD_ENGINE     ?= $(CONTAINER_ENGINE)
IMAGE            ?= ghcr.io/novasamatech/infra-tooling/subquery-db
VERSION          := $(shell sed -n '/[^[:space:]]/{s/[[:space:]]//g;p;q;}' "$(ROOT).version")
# Runtime image of the operator targets and the rehearsal. CI publishes
# IMAGE:latest and IMAGE:<version> when .version changes on main; `make image`
# builds IMAGE:local from these sources, selected with IMAGE_TAG=local.
IMAGE_TAG        ?= latest
TOOL_IMAGE       := $(IMAGE):$(IMAGE_TAG)
# always: refresh a published tag before running; missing; never (local builds).
PULL             ?= $(if $(filter local,$(IMAGE_TAG)),never,always)
PG_MAJOR         ?= 17
DEV_HASH         := $(shell cat "$(ROOT)requirements.txt" "$(ROOT)requirements-dev.txt" "$(ROOT)Dockerfile" \
                      | { sha256sum 2>/dev/null || shasum -a 256; } | cut -c1-16)-pg$(PG_MAJOR)
# Local-only name: the dev image is never published.
DEV_REPO         := subquery-db-dev
DEV_IMAGE        ?= $(DEV_REPO):$(DEV_HASH)

# Operator inputs. CONFIG holds secrets: keep it under WORK (git-ignored) or
# outside the repository. Relative paths are relative to this directory.
WORK     ?= $(ROOT)work
CONFIG   ?= $(WORK)/config.yaml
# Extra CLI arguments, e.g. ARGS='--group accounts --parallel 2 --yes'.
ARGS     ?=
# Container network for operator runs; host networking reaches private networks
# exactly as the host does.
NETWORK  ?= host

# Harness: container/network name prefix, images and seeding size.
HARNESS        ?= sqdb-harness
HARNESS_PG_OLD ?= docker.io/library/postgres:16
HARNESS_PG_NEW ?= docker.io/library/postgres:17
HARNESS_S3     ?= docker.io/versity/versitygw:v1.10.0
HARNESS_CONFIG ?= tests/harness/config.yaml
HARNESS_ROWS   ?= 300
HARNESS_TIMEOUT ?= 120
# Fixed throwaway credentials of the local harness services.
HARNESS_PG_PASSWORD := harness-superuser
HARNESS_S3_ACCESS   := harness-access-key
HARNESS_S3_SECRET   := harness-secret-key

CONFIG_ABS := $(abspath $(CONFIG))
WORK_ABS   := $(abspath $(WORK))
TTY_FLAG   := $(shell [ -t 0 ] && echo --tty)

# Run as the operator (also over the runtime image's own user), so the config
# stays readable and WORK files stay owned by the operator with either engine.
USER_FLAGS := --user $(shell id -u):$(shell id -g)
ifneq ($(findstring podman,$(CONTAINER_ENGINE)),)
USER_FLAGS += --userns=keep-id
endif

DEV_RUN  = $(CONTAINER_ENGINE) run --rm $(USER_FLAGS) -v "$(ROOT):/src" -w /src
TOOL_RUN = $(CONTAINER_ENGINE) run --rm --interactive $(TTY_FLAG) $(USER_FLAGS) \
           --network "$(NETWORK)" -v "$(CONFIG_ABS):/config/config.yaml:ro" -v "$(WORK_ABS):/work" \
           "$(TOOL_IMAGE)" --config /config/config.yaml --work-dir /work
HARNESS_ENV = --env SQDB_HARNESS="$(HARNESS)" --env HARNESS_PG_PASSWORD="$(HARNESS_PG_PASSWORD)" \
              --env HARNESS_S3_ACCESS="$(HARNESS_S3_ACCESS)" --env HARNESS_S3_SECRET="$(HARNESS_S3_SECRET)" \
              --env HARNESS_ROWS="$(HARNESS_ROWS)" --env HARNESS_TIMEOUT="$(HARNESS_TIMEOUT)"
HARNESS_DEV_RUN = $(DEV_RUN) --network "$(HARNESS)" $(HARNESS_ENV)

OPERATIONS := validate check backup restore migrate verify status

.DEFAULT_GOAL := help
.PHONY: help image image-ready dev-image work-dir check-config $(OPERATIONS) \
        lint fmt test coverage lock outdated clean \
        harness-up harness-seed harness-down harness-psql test-e2e test-all rehearse \
        ci ci-integration

help:
	@echo "sqdb — SubQuery database migration (all tasks run in containers)"
	@echo "Operator (CONFIG=$(CONFIG), WORK=$(WORK), ARGS='...'):"
	@echo "  make validate    offline config validation and plan"
	@echo "  make check       read-only preflight: connectivity, versions, sizes, targets, S3, DDL probe"
	@echo "  make backup      dump source databases to storage"
	@echo "  make restore     four-phase restore into target databases"
	@echo "  make migrate     target checks + probe + backup + restore + verify per database"
	@echo "  make verify      compare source and target databases"
	@echo "  make status      show the state recorded in WORK"
	@echo "Operator targets run $(TOOL_IMAGE) (PULL=$(PULL)); CI publishes :latest and"
	@echo ":<version> (sources: $(VERSION)) when .version changes on main."
	@echo "  make image       build $(IMAGE):local from these sources with $(BUILD_ENGINE)"
	@echo "                   (PostgreSQL client $(PG_MAJOR)); run targets with IMAGE_TAG=local"
	@echo "  make image-ready pull $(TOOL_IMAGE) (or check that the local build exists)"
	@echo "Harness (PostgreSQL 16 -> 17 + versitygw on network $(HARNESS)):"
	@echo "  make harness-up      start the harness containers"
	@echo "  make harness-seed    (re)create roles, databases and fixtures from HARNESS_CONFIG"
	@echo "  make harness-psql    psql shell, SERVER=old|new (default old)"
	@echo "  make harness-down    remove the harness containers, volume and network"
	@echo "  make test-e2e        end-to-end tests against the harness"
	@echo "  make rehearse        replay CONFIG's groups/users/databases in the harness"
	@echo "                       (CREATE_DATABASES=1: targets missing, check/migrate --create-databases)"
	@echo "Development:"
	@echo "  make lint | fmt | test | coverage | test-all | lock | outdated | clean"
	@echo "  make ci | ci-integration   repository CI checks (see the root AGENTS.md)"
	@echo "Podman without a systemd user bus: CONTAINER_ENGINE='podman --cgroup-manager=cgroupfs'."

# The same runtime stage CI builds, for testing local changes before a release.
image:
	$(BUILD_ENGINE) build --build-arg PG_MAJOR=$(PG_MAJOR) --target runtime -t "$(IMAGE):local" "$(ROOT)"

image-ready:
	@case "$(PULL)" in \
	  never) $(CONTAINER_ENGINE) image inspect "$(TOOL_IMAGE)" >/dev/null 2>&1 || { \
	    echo "ERROR: $(TOOL_IMAGE) is not available locally; build it with make image"; exit 1; } ;; \
	  missing) $(CONTAINER_ENGINE) image inspect "$(TOOL_IMAGE)" >/dev/null 2>&1 || \
	    $(CONTAINER_ENGINE) pull "$(TOOL_IMAGE)" >/dev/null ;; \
	  always) $(CONTAINER_ENGINE) pull "$(TOOL_IMAGE)" >/dev/null ;; \
	  *) echo "ERROR: PULL must be always, missing or never"; exit 1 ;; \
	esac

dev-image:
	@$(CONTAINER_ENGINE) image inspect "$(DEV_IMAGE)" >/dev/null 2>&1 || \
	  $(BUILD_ENGINE) build --build-arg PG_MAJOR=$(PG_MAJOR) --target dev -t "$(DEV_IMAGE)" "$(ROOT)"

work-dir:
	@mkdir -p "$(WORK_ABS)"

check-config:
	@test -f "$(CONFIG_ABS)" || { echo "ERROR: config not found: $(CONFIG_ABS) (see config.example.yaml)"; exit 1; }

$(OPERATIONS): image-ready work-dir check-config
	$(TOOL_RUN) $@ $(ARGS)

# ---------------------------------------------------------------- development

lint: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m ruff check .
	$(DEV_RUN) "$(DEV_IMAGE)" python -m ruff format --check .

fmt: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m ruff format .
	$(DEV_RUN) "$(DEV_IMAGE)" python -m ruff check --fix .

test: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m pytest -m "not e2e" --cov=sqdb --cov-report=term-missing

coverage: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m pytest -m "not e2e" --cov=sqdb --cov-report=term-missing --cov-report=html

outdated: dev-image
	$(DEV_RUN) "$(DEV_IMAGE)" python -m pip list --outdated

# Resolve inside an ephemeral container; only completed lock files reach /src.
lock:
	$(DEV_RUN) --env HOME=/tmp docker.io/library/python:3.13-slim-trixie /bin/sh -ec '\
	  trap '\''rm -f /src/requirements.txt.tmp /src/requirements-dev.txt.tmp'\'' EXIT; \
	  python -m venv /tmp/resolver; \
	  /tmp/resolver/bin/python -m pip install --quiet --upgrade pip; \
	  /tmp/resolver/bin/python -m pip install --quiet -r requirements.in; \
	  { echo "# Generated by \`make lock\` from requirements.in — do not edit by hand."; \
	    /tmp/resolver/bin/python -m pip freeze; } > requirements.txt.tmp; \
	  /tmp/resolver/bin/python -m pip install --quiet -r requirements-dev.in -c requirements.txt.tmp; \
	  { echo "# Generated by \`make lock\` from requirements-dev.in — do not edit by hand."; \
	    echo "-r requirements.txt"; \
	    /tmp/resolver/bin/python -m pip freeze | grep -vxFf requirements.txt.tmp; } > requirements-dev.txt.tmp; \
	  mv requirements.txt.tmp requirements.txt; \
	  mv requirements-dev.txt.tmp requirements-dev.txt'

clean:
	rm -rf "$(ROOT).pytest_cache" "$(ROOT).ruff_cache" "$(ROOT)htmlcov" "$(ROOT).coverage"
	find "$(ROOT)" -name __pycache__ -type d -prune -exec rm -rf {} +
	@# Every dev image tag, including those left behind by earlier pins.
	@for image in $$($(CONTAINER_ENGINE) images --format '{{.Repository}}:{{.Tag}}' \
	    | grep -E '(^|/)$(DEV_REPO):'); do \
	  $(CONTAINER_ENGINE) image rm "$$image"; \
	done

# -------------------------------------------------------------------- harness

harness-up:
	@$(CONTAINER_ENGINE) network exists "$(HARNESS)" 2>/dev/null || \
	  $(CONTAINER_ENGINE) network inspect "$(HARNESS)" >/dev/null 2>&1 || \
	  $(CONTAINER_ENGINE) network create "$(HARNESS)" >/dev/null
	@for spec in "old|$(HARNESS_PG_OLD)" "new|$(HARNESS_PG_NEW)"; do \
	  name="$(HARNESS)-$${spec%%|*}"; image="$${spec#*|}"; \
	  if ! $(CONTAINER_ENGINE) container inspect "$$name" >/dev/null 2>&1; then \
	    echo "starting $$name ($$image)"; \
	    $(CONTAINER_ENGINE) run -d --name "$$name" --network "$(HARNESS)" \
	      --env POSTGRES_PASSWORD="$(HARNESS_PG_PASSWORD)" "$$image" \
	      -c fsync=off -c synchronous_commit=off -c full_page_writes=off >/dev/null; \
	  else \
	    $(CONTAINER_ENGINE) start "$$name" >/dev/null; \
	  fi; \
	done
	@if ! $(CONTAINER_ENGINE) container inspect "$(HARNESS)-s3" >/dev/null 2>&1; then \
	  echo "starting $(HARNESS)-s3 ($(HARNESS_S3))"; \
	  $(CONTAINER_ENGINE) run -d --name "$(HARNESS)-s3" --network "$(HARNESS)" \
	    -v "$(HARNESS)-s3:/data" \
	    --env ROOT_ACCESS_KEY="$(HARNESS_S3_ACCESS)" --env ROOT_SECRET_KEY="$(HARNESS_S3_SECRET)" \
	    "$(HARNESS_S3)" --quiet --port :7070 posix /data >/dev/null; \
	else \
	  $(CONTAINER_ENGINE) start "$(HARNESS)-s3" >/dev/null; \
	fi
	@for server in old new; do \
	  deadline=$$(( $$(date +%s) + $(HARNESS_TIMEOUT) )); \
	  until $(CONTAINER_ENGINE) exec "$(HARNESS)-$$server" pg_isready -q -h 127.0.0.1 -U postgres; do \
	    [ $$(date +%s) -lt $$deadline ] || { echo "ERROR: $(HARNESS)-$$server is not ready"; exit 1; }; \
	    sleep 1; \
	  done; \
	done
	@echo "harness is up: $(HARNESS)-old, $(HARNESS)-new, $(HARNESS)-s3 on network $(HARNESS)"

harness-seed: dev-image harness-up
	$(HARNESS_DEV_RUN) "$(DEV_IMAGE)" python -m tests.harness.seed --config "$(HARNESS_CONFIG)"

SERVER ?= old
harness-psql:
	$(CONTAINER_ENGINE) exec -it "$(HARNESS)-$(SERVER)" psql -U postgres

harness-down:
	-@$(CONTAINER_ENGINE) rm -f "$(HARNESS)-old" "$(HARNESS)-new" "$(HARNESS)-s3" >/dev/null 2>&1
	-@$(CONTAINER_ENGINE) volume rm -f "$(HARNESS)-s3" >/dev/null 2>&1
	-@$(CONTAINER_ENGINE) network rm "$(HARNESS)" >/dev/null 2>&1
	@echo "harness removed"

# The e2e session reseeds the harness itself; containers stay up for inspection.
test-e2e: dev-image harness-up
	$(HARNESS_DEV_RUN) "$(DEV_IMAGE)" python -m pytest -m e2e -v $(PYTEST_ARGS)

test-all: lint dev-image harness-up
	$(HARNESS_DEV_RUN) "$(DEV_IMAGE)" python -m pytest --cov=sqdb --cov-report=term-missing \
	  --cov-fail-under=85 $(PYTEST_ARGS)

# Replays CONFIG's topology (groups, users, passwords, database names, admin) in
# the harness with fixture data, then runs check, migrate and verify through the
# runtime image the operator targets use (IMAGE_TAG=local for a local build). Nothing outside the harness is contacted. Target databases are
# pre-created like the database service does; CREATE_DATABASES=1 leaves them
# missing and runs check and migrate with --create-databases instead.
REHEARSAL_DIR := $(WORK_ABS)/rehearsal
CREATE_DATABASES ?=
rehearse: image-ready dev-image harness-up work-dir check-config
	@# The harness is reseeded, so earlier rehearsal state no longer applies.
	@rm -rf "$(REHEARSAL_DIR)" && mkdir -p "$(REHEARSAL_DIR)"
	$(HARNESS_DEV_RUN) -v "$(CONFIG_ABS):/config/config.yaml:ro" -v "$(REHEARSAL_DIR):/rehearsal" \
	  "$(DEV_IMAGE)" python -m tests.harness.seed --config /config/config.yaml \
	  --write-config /rehearsal/config.yaml $(if $(CREATE_DATABASES),--no-precreate)
	@for step in validate check migrate verify; do \
	  echo "== rehearsal: $$step"; \
	  case $$step in check|migrate) extra="$(if $(CREATE_DATABASES),--create-databases)";; *) extra="";; esac; \
	  $(CONTAINER_ENGINE) run --rm $(USER_FLAGS) --network "$(HARNESS)" \
	    -v "$(REHEARSAL_DIR):/work" "$(TOOL_IMAGE)" \
	    --config /work/config.yaml --work-dir /work $$step --yes $$extra $(ARGS) || exit 1; \
	done
	@echo "rehearsal passed; state, logs and reports are in $(REHEARSAL_DIR)"

# ------------------------------------------------------------------------- CI

# Repository CI entry points (see the root AGENTS.md). ci-integration uses the
# local harness only: PostgreSQL 16 -> 17, versitygw and the runtime image. It
# removes the harness whether the steps pass or fail; KEEP_HARNESS=1 leaves it
# up for debugging.
KEEP_HARNESS ?=
ci: lint test

ci-integration:
	@rc=0; \
	$(MAKE) test-all && $(MAKE) image && \
	  $(MAKE) rehearse CONFIG=config.example.yaml IMAGE_TAG=local || rc=$$?; \
	$(if $(KEEP_HARNESS),echo "KEEP_HARNESS set: harness left running";,$(MAKE) harness-down;) \
	exit $$rc
