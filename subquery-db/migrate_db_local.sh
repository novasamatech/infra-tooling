#!/usr/bin/env bash

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

# ---------------------------------------------------------------------
# SubQuery database migration helper
#
# REQUIREMENTS
#   * Podman installed and in PATH. For debian run `apt install podman runc slirp4netns uidmap`
#
# FLOW
#   Phase 0 : pg_dump   – source → dump directory
#   Phase 1 : schema    – PRE-DATA
#   Phase 2 : indexes   – on empty tables
#   Phase 3 : data      – DATA section
#   Phase 4 : post-data – constraints, triggers, ACL …
# ---------------------------------------------------------------------

set -euo pipefail

# ---------- SOURCE ---------------------------------------------------
SRC_PG_IMG="docker.io/library/postgres:16"
SRC_PGHOST=""
SRC_PGPORT=""
SRC_PGUSER=""
SRC_PGPASSWORD=""
SRC_DBNAME=""
DUMP_JOBS=2

# ---------- DESTINATION ----------------------------------------------
DST_PG_IMG="docker.io/library/postgres:16"
DST_PGHOST=""
DST_PGPORT=""
DST_PGUSER=""
DST_PGPASSWORD=""
DST_DBNAME=""
RESTORE_JOBS=2

HOST_DUMP_BASE="./pgdump"
HOST_DUMP_DIR="${HOST_DUMP_BASE}/${SRC_DBNAME}"

export PGOPTIONS='-c statement_timeout=0 -c maintenance_work_mem=1GB -c max_parallel_maintenance_workers=1'

log(){ echo "[ $(date +'%F %T') ] $*"; }

command -v podman >/dev/null || { echo "❌  Podman not found"; exit 1; }

# ---------- PHASE 0 : pg_dump ----------------------------------------
if [[ ! -d "$HOST_DUMP_DIR" || ! -f "$HOST_DUMP_DIR/toc.dat" ]]; then
  log "Phase 0/5 : creating dump"
  export PGPASSWORD="${SRC_PGPASSWORD}"
  podman run --rm -v "${HOST_DUMP_BASE}:/dump" \
    -e PGPASSWORD \
    "$SRC_PG_IMG" \
    pg_dump -h "$SRC_PGHOST" -p "$SRC_PGPORT" -U "$SRC_PGUSER" \
            -F d -j "$DUMP_JOBS" -Z0 \
            -f "/dump/$SRC_DBNAME" "$SRC_DBNAME"
else
  log "Phase 0/5 : dump already exists – skipping"
fi

[[ -f "$HOST_DUMP_DIR/toc.dat" ]] || { echo "❌ invalid dump"; exit 2; }

DUMP_IN_CT="/dump/$(basename "$HOST_DUMP_DIR")"

unset PGPASSWORD
export PGPASSWORD="${DST_PGPASSWORD}"

# ---------- PHASE 1 : schema (PRE-DATA) -------------------------------
log "Phase 1/5 : loading schema"
podman run --rm -v "${HOST_DUMP_BASE}:/dump:ro" \
  -e PGPASSWORD -e PGOPTIONS \
  "$DST_PG_IMG" \
  pg_restore --section=pre-data --no-owner --no-privileges -j "$RESTORE_JOBS" \
             -h "$DST_PGHOST" -p "$DST_PGPORT" -U "$DST_PGUSER" -d "$DST_DBNAME" \
             "$DUMP_IN_CT"

# ---------- PHASE 2 : indexes ----------------------------------------
log "Phase 2/5 : creating indexes"
podman run --rm -v "${HOST_DUMP_BASE}:/dump:ro" \
  -e PGPASSWORD -e PGOPTIONS \
  "$DST_PG_IMG" \
  bash -eu -o pipefail -c '
    IDX=$(mktemp)
    pg_restore -l "'"$DUMP_IN_CT"'" | grep " INDEX " > "$IDX"
    pg_restore --use-list="$IDX" --section=post-data \
               --no-owner --no-privileges -j '"$RESTORE_JOBS"' \
               -h "'"$DST_PGHOST"'" -p "'"$DST_PGPORT"'" -U "'"$DST_PGUSER"'" -d "'"$DST_DBNAME"'" \
               "'"$DUMP_IN_CT"'"
  '

# ---------- PHASE 3 : data -------------------------------------------
log "Phase 3/5 : loading data"
podman run --rm -v "${HOST_DUMP_BASE}:/dump:ro" \
  -e PGPASSWORD -e PGOPTIONS \
  "$DST_PG_IMG" \
  pg_restore --section=data --no-owner --no-privileges -j "$RESTORE_JOBS" \
             -h "$DST_PGHOST" -p "$DST_PGPORT" -U "$DST_PGUSER" -d "$DST_DBNAME" \
             "$DUMP_IN_CT"

# ---------- PHASE 4 : post-data --------------------------------------
log "Phase 4/5 : loading constraints & triggers"
podman run --rm -v "${HOST_DUMP_BASE}:/dump:ro" \
  -e PGPASSWORD -e PGOPTIONS \
  "$DST_PG_IMG" \
  bash -eu -o pipefail -c '
    PD=$(mktemp)
    pg_restore -l "'"$DUMP_IN_CT"'" | grep -v " INDEX " > "$PD"
    pg_restore --use-list="$PD" --section=post-data \
               --no-owner --no-privileges -j '"$RESTORE_JOBS"' \
               -h "'"$DST_PGHOST"'" -p "'"$DST_PGPORT"'" -U "'"$DST_PGUSER"'" -d "'"$DST_DBNAME"'" \
               "'"$DUMP_IN_CT"'"
  '

log "✅  Migration completed successfully"