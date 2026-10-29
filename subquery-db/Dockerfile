# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0
#
# Stages: base (Python + PostgreSQL client tools) → dev (tests, lint, an older
# client for legacy-dump tests) → runtime. The runtime stage is last, so a plain
# `build` produces it.

ARG PYTHON_IMAGE=docker.io/library/python:3.13-slim-trixie

FROM ${PYTHON_IMAGE} AS base
# Client major version for pg_dump/pg_restore/psql. It must be at least the
# newest server major involved in a migration.
ARG PG_MAJOR=17
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    PG_BINDIR=/usr/lib/postgresql/${PG_MAJOR}/bin
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates curl \
 && install -d /usr/share/postgresql-common/pgdg \
 && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
      https://www.postgresql.org/media/keys/ACCC4CF8.asc \
 && . /etc/os-release \
 && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "postgresql-client-${PG_MAJOR}" \
 && apt-get purge -y curl && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

FROM base AS dev
# pg_dump 16 produces archives in the format of the dumps made before 1.0.0.
ARG LEGACY_PG_MAJOR=16
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "postgresql-client-${LEGACY_PG_MAJOR}" \
 && rm -rf /var/lib/apt/lists/*
ENV LEGACY_PG_BINDIR=/usr/lib/postgresql/${LEGACY_PG_MAJOR}/bin
COPY --chmod=644 requirements.txt requirements-dev.txt /tmp/requirements/
RUN pip install --no-cache-dir --root-user-action=ignore -r /tmp/requirements/requirements-dev.txt
WORKDIR /src

FROM base AS runtime
COPY --chmod=644 requirements.txt /tmp/requirements/
RUN pip install --no-cache-dir --root-user-action=ignore -r /tmp/requirements/requirements.txt \
 && useradd --uid 1000 --user-group --no-create-home --shell /usr/sbin/nologin sqdb
COPY --chmod=755 sqdb /app/sqdb
ENV PYTHONPATH=/app
WORKDIR /work
USER sqdb
ENTRYPOINT ["python", "-m", "sqdb"]
CMD ["--help"]
