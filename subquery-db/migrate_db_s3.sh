#!/usr/bin/env bash

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

# ---------------------------------------------------------------------
# SubQuery DB migration via S3 (fully containerized with Podman)
#
# - Builds a temp image: postgres:16 + awscli
# - Streams dump to S3 (single-file, -Fc)
# - Restores in 4 phases while streaming from S3 (no local dump)
# - Supports S3-compatible endpoints via AWS_* envs and endpoint URL
#
# USAGE:
#   ./migrate_db_s3.sh MODE [OPTIONS]
#
# REQUIRED MODE (one of):
#   --all         Run complete migration (backup + restore)
#   --backup      Create and upload backup to S3 only
#   --restore     Restore from existing S3 backup only
#
# OPTIONS:
#   --help        Show this help message
#   --skip-checks Skip database connectivity checks
#   --force       Skip confirmation prompts
#   --quiet       Reduce output verbosity (disable progress indicators)
#   --debug       Enable debug output
#
# ENVIRONMENT VARIABLES:
#   Source database:
#     SRC_PGHOST          Source PostgreSQL host
#     SRC_PGPORT          Source PostgreSQL port (default: 5432)
#     SRC_PGUSER          Source PostgreSQL user
#     SRC_PGPASSWORD      Source PostgreSQL password
#     SRC_DBNAME          Source database name
#
#   Destination database:
#     DST_PGHOST          Destination PostgreSQL host
#     DST_PGPORT          Destination PostgreSQL port (default: 5432)
#     DST_PGUSER          Destination PostgreSQL user
#     DST_PGPASSWORD      Destination PostgreSQL password
#     DST_DBNAME          Destination database name
#
#   S3 configuration:
#     S3_URI              S3 URI for backup (s3://bucket/path/file.dump)
#     AWS_ACCESS_KEY_ID   AWS access key
#     AWS_SECRET_ACCESS_KEY AWS secret key
#     AWS_SESSION_TOKEN   AWS session token (optional)
#     AWS_DEFAULT_REGION  AWS region (default: us-east-1)
#     S3_ENDPOINT_URL     Custom S3 endpoint for S3-compatible services
#     AWS_S3_FORCE_PATH_STYLE Use path-style addressing (for MinIO/Ceph)
#     AWS_NO_VERIFY_SSL   Disable SSL verification (not recommended)
#
#   Tuning:
#     RESTORE_JOBS        Parallel restore jobs (default: 1)
#     DUMP_COMPRESS       Compression level 0-9 (default: 0)
#     S3_MAX_RETRIES      Max retries for S3 ops (default: 3)
#     SHOW_PROGRESS       Show progress indicators (default: true)
#
# EXAMPLES:
#   # Full migration
#   ./migrate_db_s3.sh --all
#
#   # Backup only
#   ./migrate_db_s3.sh --backup
#
#   # Restore only
#   ./migrate_db_s3.sh --restore
#
#   # Restore without connectivity checks
#   ./migrate_db_s3.sh --restore --skip-checks
#
# NOTES:
# - Parallel dump (-j) is not supported for -Fc (single-file).
# - Parallel restore from stdin can force spooling; to avoid local disk usage,
#   we set RESTORE_JOBS=1 by default. You can raise it, but it may cause
#   temporary spooling inside the container filesystem.
# ---------------------------------------------------------------------

set -euo pipefail
# Disable xtrace to prevent secrets from being exposed
set +x

# ---------- CLI ARGUMENTS --------------------------------------------
MODE=""
SKIP_CHECKS=false
FORCE=false
QUIET=false
DEBUG=false

show_help() {
  sed -n '/^# USAGE:/,/^# ---/p' "$0" | sed 's/^# //' | head -n -1
  exit 0
}

parse_args() {
  if [[ $# -eq 0 ]]; then
    echo "❌ Error: No mode specified. Use --all, --backup, or --restore"
    echo "Use --help for more information"
    exit 1
  fi

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --all)
        [[ -n "$MODE" ]] && { echo "❌ Error: Multiple modes specified"; exit 1; }
        MODE="all"
        shift
        ;;
      --backup)
        [[ -n "$MODE" ]] && { echo "❌ Error: Multiple modes specified"; exit 1; }
        MODE="backup"
        shift
        ;;
      --restore)
        [[ -n "$MODE" ]] && { echo "❌ Error: Multiple modes specified"; exit 1; }
        MODE="restore"
        shift
        ;;
      --help|-h)
        show_help
        ;;
      --skip-checks)
        SKIP_CHECKS=true
        shift
        ;;
      --force)
        FORCE=true
        shift
        ;;
      --quiet)
        QUIET=true
        SHOW_PROGRESS=false
        export SHOW_PROGRESS
        shift
        ;;
      --debug)
        DEBUG=true
        # Don't use set -x to avoid printing secrets
        shift
        ;;
      *)
        echo "❌ Error: Unknown option: $1"
        echo "Use --help for more information"
        exit 1
        ;;
    esac
  done

  if [[ -z "$MODE" ]]; then
    echo "❌ Error: No mode specified. Use --all, --backup, or --restore"
    echo "Use --help for more information"
    exit 1
  fi
}

# ---------- CONFIG: SOURCE -------------------------------------------
SRC_PGHOST="${SRC_PGHOST:-}"
SRC_PGPORT="${SRC_PGPORT:-}"
SRC_PGUSER="${SRC_PGUSER:-}"
SRC_PGPASSWORD="${SRC_PGPASSWORD:-}"
SRC_DBNAME="${SRC_DBNAME:-}"

# ---------- CONFIG: DESTINATION --------------------------------------
DST_PGHOST="${DST_PGHOST:-}"
DST_PGPORT="${DST_PGPORT:-}"
DST_PGUSER="${DST_PGUSER:-}"
DST_PGPASSWORD="${DST_PGPASSWORD:-}"
DST_DBNAME="${DST_DBNAME:-}"

# ---------- CONFIG: BACKUP --------------------------------------
BACKUP_NAME="${BACKUP_NAME:-}"

# ---------- CONFIG: S3 -----------------------------------------------
S3_URI="s3://subquery-dumps/${SRC_DBNAME}/${BACKUP_NAME}.dump"

# S3-compatible providers:
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-}"
S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-}"
AWS_S3_FORCE_PATH_STYLE="${AWS_S3_FORCE_PATH_STYLE:-}"
AWS_NO_VERIFY_SSL="${AWS_NO_VERIFY_SSL:-}"

# ---------- TUNING ---------------------------------------------------
RESTORE_JOBS="${RESTORE_JOBS:-1}"
DUMP_COMPRESS="${DUMP_COMPRESS:-0}"
S3_MAX_RETRIES="${S3_MAX_RETRIES:-3}"
SHOW_PROGRESS="${SHOW_PROGRESS:-true}"

# ---------- IMAGE BUILD ----------------------------------------------
PG_BASE_IMG="${PG_BASE_IMG:-docker.io/library/postgres:16}"
PG_AWS_IMG="${PG_AWS_IMG:-local/postgres16-awscli:latest}"

export PGOPTIONS='-c statement_timeout=0 -c maintenance_work_mem=1GB -c max_parallel_maintenance_workers=1'

log(){ if [[ "$QUIET" == "false" ]]; then echo "[ $(date +'%F %T') ] $*"; fi; }
error(){ echo "❌ $*" >&2; }
warn(){ echo "⚠️  $*" >&2; }
success(){ if [[ "$QUIET" == "false" ]]; then echo "✅ $*" >&2; fi; }
debug(){
  if [[ "$DEBUG" == "true" ]]; then
    # Mask sensitive data in debug output
    local msg="${*}"
    # Mask all potential secrets
    [[ -n "$AWS_SECRET_ACCESS_KEY" ]] && msg="${msg//$AWS_SECRET_ACCESS_KEY/***}"
    [[ -n "$AWS_SESSION_TOKEN" ]] && msg="${msg//$AWS_SESSION_TOKEN/***}"
    [[ -n "$SRC_PGPASSWORD" ]] && msg="${msg//$SRC_PGPASSWORD/***}"
    [[ -n "$DST_PGPASSWORD" ]] && msg="${msg//$DST_PGPASSWORD/***}"
    [[ -n "$AWS_ACCESS_KEY_ID" ]] && msg="${msg//$AWS_ACCESS_KEY_ID/***KEY***}"
    echo "🔍 DEBUG: $msg" >&2
  fi
}

require_cmd(){ command -v "$1" >/dev/null || { error "$1 not found"; exit 1; }; }

# Validate required variables based on mode
validate_config() {
  local errors=()

  # common checks
  [[ -z "$BACKUP_NAME" ]] && errors+=("BACKUP_NAME is not set")

  # Check S3 config (always required)
  [[ "$S3_URI" == s3://* ]] || errors+=("S3_URI must start with s3://")
  [[ -z "$S3_URI" ]] && errors+=("S3_URI is not set")
  [[ -z "$AWS_ACCESS_KEY_ID" ]] && errors+=("AWS_ACCESS_KEY_ID is not set")
  [[ -z "$AWS_SECRET_ACCESS_KEY" ]] && errors+=("AWS_SECRET_ACCESS_KEY is not set")

  # Check source config (for backup and all modes)
  if [[ "$MODE" == "backup" ]] || [[ "$MODE" == "all" ]]; then
    [[ -z "$SRC_PGHOST" ]] && errors+=("SRC_PGHOST is not set")
    [[ -z "$SRC_PGUSER" ]] && errors+=("SRC_PGUSER is not set")
    [[ -z "$SRC_DBNAME" ]] && errors+=("SRC_DBNAME is not set")
  fi

  # Check destination config (for restore and all modes)
  if [[ "$MODE" == "restore" ]] || [[ "$MODE" == "all" ]]; then
    [[ -z "$DST_PGHOST" ]] && errors+=("DST_PGHOST is not set")
    [[ -z "$DST_PGUSER" ]] && errors+=("DST_PGUSER is not set")
    [[ -z "$DST_DBNAME" ]] && errors+=("DST_DBNAME is not set")
  fi

  if [[ ${#errors[@]} -gt 0 ]]; then
    error "Configuration errors:"
    for err in "${errors[@]}"; do
      error "  - $err"
    done
    exit 1
  fi
}

confirm_action() {
  if [[ "$FORCE" == "true" ]]; then
    return 0
  fi

  local msg=""
  case "$MODE" in
    all)
      msg="This will backup $SRC_DBNAME and restore to $DST_DBNAME via $S3_URI"
      ;;
    backup)
      msg="This will backup $SRC_DBNAME to $S3_URI"
      ;;
    restore)
      msg="This will restore from $S3_URI to $DST_DBNAME"
      ;;
  esac

  echo ""
  warn "$msg"
  read -p "Are you sure? (yes/no): " -r
  if [[ ! "$REPLY" =~ ^[Yy]es$ ]]; then
    echo "Cancelled by user"
    exit 0
  fi
  echo ""
  debug "Confirmation completed, continuing..."
}

build_image_if_needed() {
  debug "Entering build_image_if_needed function"
  if podman image exists "$PG_AWS_IMG" 2>/dev/null; then
    debug "Image $PG_AWS_IMG already exists"
    return
  fi
  log "Building helper image: $PG_AWS_IMG (base: $PG_BASE_IMG)"
  if ! podman build -t "$PG_AWS_IMG" -q - <<EOF
FROM $PG_BASE_IMG
RUN apt-get update \\
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \\
      awscli ca-certificates less pv \\
 && rm -rf /var/lib/apt/lists/*
EOF
  then
    error "Failed to build image"
    exit 1
  fi
}

# Execute command with retry logic
execute_with_retry() {
  local attempt=1
  while [[ $attempt -le $S3_MAX_RETRIES ]]; do
    if "$@"; then
      return 0
    fi
    warn "Operation failed (attempt $attempt/$S3_MAX_RETRIES)"
    [[ $attempt -lt $S3_MAX_RETRIES ]] && sleep $((attempt * 2))
    ((attempt++))
  done
  return 1
}

check_db_connection() {
  local type="$1"
  local host port user pass dbname

  if [[ "$type" == "source" ]]; then
    host="$SRC_PGHOST" port="$SRC_PGPORT"
    user="$SRC_PGUSER" pass="$SRC_PGPASSWORD"
    dbname="$SRC_DBNAME"
  else
    host="$DST_PGHOST" port="$DST_PGPORT"
    user="$DST_PGUSER" pass="$DST_PGPASSWORD"
    dbname="$DST_DBNAME"
  fi

  log "Checking $type database connection..."
  debug "Testing connection to $host:$port/$dbname as user $user"
  if PGPASSWORD="$pass" podman run --rm \
    -e PGPASSWORD \
    "$PG_AWS_IMG" \
    psql -h "$host" -p "$port" -U "$user" -d "$dbname" -c "SELECT 1" >/dev/null 2>&1; then
    success "$type database is accessible"
  else
    error "Cannot connect to $type database at $host:$port"
    debug "Connection failed - check credentials and network access"
    exit 1
  fi
}

# Initialize temp file variables globally
TOC=""
IDX_LIST=""
PD_LIST=""

cleanup() {
  [[ -n "$TOC" ]] && rm -f "$TOC" 2>/dev/null || true
  [[ -n "$IDX_LIST" ]] && rm -f "$IDX_LIST" 2>/dev/null || true
  [[ -n "$PD_LIST" ]] && rm -f "$PD_LIST" 2>/dev/null || true
  # Don't remove the image as it's reusable
}

# ---------- BACKUP FUNCTION ------------------------------------------
do_backup() {
  debug "Entering do_backup function"
  log "Starting backup process for database: $SRC_DBNAME"

  # Build AWS CLI options as a string
  local aws_opts=""
  [[ -n "$S3_ENDPOINT_URL" ]] && aws_opts="--endpoint-url $S3_ENDPOINT_URL"
  [[ "$AWS_NO_VERIFY_SSL" == "true" ]] && aws_opts="$aws_opts --no-verify-ssl"

  # Check if backup already exists (no retry needed for existence check)
  if podman run --rm \
    -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
    -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
    -e "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" \
    -e "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION" \
    -e "AWS_S3_FORCE_PATH_STYLE=$AWS_S3_FORCE_PATH_STYLE" \
    "$PG_AWS_IMG" \
    bash -c "aws s3 ls '$S3_URI' $aws_opts >/dev/null 2>&1"; then

    warn "Backup already exists at $S3_URI"
    if [[ "$FORCE" != "true" ]]; then
      read -p "Overwrite existing backup? (yes/no): " -r
      if [[ ! "$REPLY" =~ ^[Yy]es$ ]]; then
        echo "Backup cancelled"
        return 0
      fi
    fi
  else
    log "Backup does not exist, will create"
  fi

  log "Starting database dump of $SRC_DBNAME"
  log "Creating dump and uploading to S3: $S3_URI"

  # Create a temp script to run inside container
  local script=$(cat <<'SCRIPT'
set -euo pipefail
aws_opts=""
[[ -n "$S3_ENDPOINT_URL" ]] && aws_opts="--endpoint-url $S3_ENDPOINT_URL"
[[ "$AWS_NO_VERIFY_SSL" == "true" ]] && aws_opts="$aws_opts --no-verify-ssl"

verbose_flag=""
[[ "$SHOW_PROGRESS" == "true" ]] && verbose_flag="--verbose"

pg_dump -h "$SRC_PGHOST" -p "$SRC_PGPORT" -U "$SRC_PGUSER" \
        -Fc -Z "$DUMP_COMPRESS" $verbose_flag \
        "$SRC_DBNAME" \
| aws s3 cp - "$S3_URI" $aws_opts
SCRIPT
)

  if ! execute_with_retry podman run --rm \
    -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
    -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
    -e "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" \
    -e "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION" \
    -e "AWS_S3_FORCE_PATH_STYLE=$AWS_S3_FORCE_PATH_STYLE" \
    -e "S3_ENDPOINT_URL=$S3_ENDPOINT_URL" \
    -e "AWS_NO_VERIFY_SSL=$AWS_NO_VERIFY_SSL" \
    -e "PGPASSWORD=$SRC_PGPASSWORD" \
    -e "SRC_PGHOST=$SRC_PGHOST" \
    -e "SRC_PGPORT=$SRC_PGPORT" \
    -e "SRC_PGUSER=$SRC_PGUSER" \
    -e "SRC_DBNAME=$SRC_DBNAME" \
    -e "DUMP_COMPRESS=$DUMP_COMPRESS" \
    -e "S3_URI=$S3_URI" \
    -e "SHOW_PROGRESS=$SHOW_PROGRESS" \
    -e "PGOPTIONS=$PGOPTIONS" \
    "$PG_AWS_IMG" \
    bash -c "$script"; then
    error "Failed to create and upload dump"
    exit 1
  fi

  success "Backup completed successfully: $S3_URI"
}

# ---------- RESTORE FUNCTION -----------------------------------------
do_restore() {
  debug "Entering do_restore function"
  log "Starting restore process"

  # Build AWS CLI options as a string
  local aws_opts=""
  [[ -n "$S3_ENDPOINT_URL" ]] && aws_opts="--endpoint-url $S3_ENDPOINT_URL"
  [[ "$AWS_NO_VERIFY_SSL" == "true" ]] && aws_opts="$aws_opts --no-verify-ssl"

  # Check if backup exists (with retry since it must exist for restore)
  if ! execute_with_retry podman run --rm \
    -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
    -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
    -e "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" \
    -e "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION" \
    -e "AWS_S3_FORCE_PATH_STYLE=$AWS_S3_FORCE_PATH_STYLE" \
    "$PG_AWS_IMG" \
    bash -c "aws s3 ls '$S3_URI' $aws_opts >/dev/null 2>&1"; then
    error "Backup not found at $S3_URI"
    exit 1
  fi

  # Check if destination database exists
  log "Checking if destination database exists: $DST_DBNAME"
  if ! PGPASSWORD="$DST_PGPASSWORD" podman run --rm \
    -e PGPASSWORD \
    "$PG_AWS_IMG" \
    psql -h "$DST_PGHOST" -p "$DST_PGPORT" -U "$DST_PGUSER" -d "$DST_DBNAME" -c "SELECT 1" >/dev/null 2>&1; then

    warn "Database $DST_DBNAME does not exist on $DST_PGHOST:$DST_PGPORT"

    # Try to create the database
    log "Attempting to create database $DST_DBNAME"
    if ! PGPASSWORD="$DST_PGPASSWORD" podman run --rm \
      -e PGPASSWORD \
      "$PG_AWS_IMG" \
      psql -h "$DST_PGHOST" -p "$DST_PGPORT" -U "$DST_PGUSER" -d postgres \
           -c "CREATE DATABASE \"$DST_DBNAME\" WITH OWNER = \"$DST_PGUSER\"" 2>&1; then
      error "Failed to create database $DST_DBNAME"
      error "Please create the database manually or check your permissions"
      exit 1
    fi
    success "Database $DST_DBNAME created successfully"
  else
    debug "Database $DST_DBNAME exists, checking if it's empty"

    # Check if 'app' schema exists
    log "Checking for existing 'app' schema in destination database"
    if PGPASSWORD="$DST_PGPASSWORD" podman run --rm \
      -e PGPASSWORD \
      "$PG_AWS_IMG" \
      psql -h "$DST_PGHOST" -p "$DST_PGPORT" -U "$DST_PGUSER" -d "$DST_DBNAME" \
           -c "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'app'" 2>/dev/null | grep -q "1 row"; then
      error "Database $DST_DBNAME contains 'app' schema"
      error "Destination database must be empty for restore to work correctly"
      error "Please drop the 'app' schema or use a different database"
      error "You can drop it with: DROP SCHEMA IF EXISTS app CASCADE;"
      exit 1
    fi

    success "Database $DST_DBNAME exists and is empty (no 'app' schema found)"
  fi

  # Setup temp files
  TOC="$(mktemp)"; IDX_LIST="$(mktemp)"; PD_LIST="$(mktemp)"

  log "Preparing TOC and object lists"

  # Get TOC
  if ! podman run --rm \
    -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
    -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
    -e "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" \
    -e "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION" \
    -e "AWS_S3_FORCE_PATH_STYLE=$AWS_S3_FORCE_PATH_STYLE" \
    -e "S3_ENDPOINT_URL=$S3_ENDPOINT_URL" \
    -e "AWS_NO_VERIFY_SSL=$AWS_NO_VERIFY_SSL" \
    -e "S3_URI=$S3_URI" \
    "$PG_AWS_IMG" \
    bash -c "
      set +o pipefail  # Ignore SIGPIPE from pg_restore closing early
      aws_opts=\"\"
      [[ -n \"\$S3_ENDPOINT_URL\" ]] && aws_opts=\"--endpoint-url \$S3_ENDPOINT_URL\"
      [[ \"\$AWS_NO_VERIFY_SSL\" == \"true\" ]] && aws_opts=\"\$aws_opts --no-verify-ssl\"
      aws s3 cp \"\$S3_URI\" - \$aws_opts 2>/dev/null | pg_restore -l
    " > "$TOC" 2>/dev/null; then
    error "Failed to retrieve TOC from dump"
    exit 1
  fi

  # Check if TOC is valid
  if [[ ! -s "$TOC" ]]; then
    error "Retrieved TOC is empty"
    exit 1
  fi

  # Build lists
  if grep " INDEX " "$TOC" > "$IDX_LIST" 2>/dev/null; then
    log "Found $(wc -l < "$IDX_LIST") indexes to create"
  else
    warn "No indexes found in dump"
    echo "# No indexes" > "$IDX_LIST"
  fi

  grep -v " INDEX " "$TOC" > "$PD_LIST" || true

  # Phase 1: schema
  log "Phase 1/4: Loading schema (pre-data)"
  if ! podman run --rm \
    -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
    -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
    -e "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" \
    -e "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION" \
    -e "AWS_S3_FORCE_PATH_STYLE=$AWS_S3_FORCE_PATH_STYLE" \
    -e "S3_ENDPOINT_URL=$S3_ENDPOINT_URL" \
    -e "AWS_NO_VERIFY_SSL=$AWS_NO_VERIFY_SSL" \
    -e "PGPASSWORD=$DST_PGPASSWORD" \
    -e "DST_PGHOST=$DST_PGHOST" \
    -e "DST_PGPORT=$DST_PGPORT" \
    -e "DST_PGUSER=$DST_PGUSER" \
    -e "DST_DBNAME=$DST_DBNAME" \
    -e "S3_URI=$S3_URI" \
    -e "RESTORE_JOBS=$RESTORE_JOBS" \
    -e "SHOW_PROGRESS=$SHOW_PROGRESS" \
    -e "PGOPTIONS=$PGOPTIONS" \
    "$PG_AWS_IMG" \
    bash -c "
      set -eu
      aws_opts=\"\"
      [[ -n \"\$S3_ENDPOINT_URL\" ]] && aws_opts=\"--endpoint-url \$S3_ENDPOINT_URL\"
      [[ \"\$AWS_NO_VERIFY_SSL\" == \"true\" ]] && aws_opts=\"\$aws_opts --no-verify-ssl\"
      verbose_flag=\"\"
      [[ \"\$SHOW_PROGRESS\" == \"true\" ]] && verbose_flag=\"--verbose\"
      # Stream from S3 to pg_restore, ignoring SIGPIPE from aws when pg_restore closes early
      set +o pipefail  # Disable pipefail to handle SIGPIPE gracefully
      aws s3 cp \"\$S3_URI\" - \$aws_opts 2>/dev/null | \
      pg_restore --section=pre-data --no-owner --no-privileges -j \"\$RESTORE_JOBS\" \
                   \$verbose_flag \
                   -h \"\$DST_PGHOST\" -p \"\$DST_PGPORT\" -U \"\$DST_PGUSER\" -d \"\$DST_DBNAME\"
      # Return pg_restore exit code directly
      exit \$?
    "; then
    error "Phase 1 failed"
    exit 1
  fi

  # Phase 2: indexes
  if [[ $(grep -c -v '^#' "$IDX_LIST" || echo 0) -gt 0 ]]; then
    log "Phase 2/4: Creating indexes"
    if ! podman run --rm \
      -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
      -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
      -e "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" \
      -e "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION" \
      -e "AWS_S3_FORCE_PATH_STYLE=$AWS_S3_FORCE_PATH_STYLE" \
      -e "S3_ENDPOINT_URL=$S3_ENDPOINT_URL" \
      -e "AWS_NO_VERIFY_SSL=$AWS_NO_VERIFY_SSL" \
      -e "PGPASSWORD=$DST_PGPASSWORD" \
      -e "DST_PGHOST=$DST_PGHOST" \
      -e "DST_PGPORT=$DST_PGPORT" \
      -e "DST_PGUSER=$DST_PGUSER" \
      -e "DST_DBNAME=$DST_DBNAME" \
      -e "S3_URI=$S3_URI" \
      -e "RESTORE_JOBS=$RESTORE_JOBS" \
      -e "SHOW_PROGRESS=$SHOW_PROGRESS" \
      -e "PGOPTIONS=$PGOPTIONS" \
      -v "$IDX_LIST:/tmp/idx.list:ro" \
      "$PG_AWS_IMG" \
      bash -c "
        set -eu
        aws_opts=\"\"
        [[ -n \"\$S3_ENDPOINT_URL\" ]] && aws_opts=\"--endpoint-url \$S3_ENDPOINT_URL\"
        [[ \"\$AWS_NO_VERIFY_SSL\" == \"true\" ]] && aws_opts=\"\$aws_opts --no-verify-ssl\"
        verbose_flag=\"\"
        [[ \"\$SHOW_PROGRESS\" == \"true\" ]] && verbose_flag=\"--verbose\"
        # Stream from S3 to pg_restore, ignoring SIGPIPE from aws when pg_restore closes early
        set +o pipefail  # Disable pipefail to handle SIGPIPE gracefully
        aws s3 cp \"\$S3_URI\" - \$aws_opts 2>/dev/null | \
        pg_restore --use-list=/tmp/idx.list --section=post-data \
                     --no-owner --no-privileges -j \"\$RESTORE_JOBS\" \
                      \$verbose_flag \
                      -h \"\$DST_PGHOST\" -p \"\$DST_PGPORT\" -U \"\$DST_PGUSER\" -d \"\$DST_DBNAME\"
        # Return pg_restore exit code directly
        exit \$?
      "; then
      error "Phase 2 failed"
      exit 1
    fi
  else
    log "Phase 2/4: Skipping (no indexes)"
  fi

  # Phase 3: data
  log "Phase 3/4: Loading data"
  if ! podman run --rm \
    -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
    -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
    -e "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" \
    -e "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION" \
    -e "AWS_S3_FORCE_PATH_STYLE=$AWS_S3_FORCE_PATH_STYLE" \
    -e "S3_ENDPOINT_URL=$S3_ENDPOINT_URL" \
    -e "AWS_NO_VERIFY_SSL=$AWS_NO_VERIFY_SSL" \
    -e "PGPASSWORD=$DST_PGPASSWORD" \
    -e "DST_PGHOST=$DST_PGHOST" \
    -e "DST_PGPORT=$DST_PGPORT" \
    -e "DST_PGUSER=$DST_PGUSER" \
    -e "DST_DBNAME=$DST_DBNAME" \
    -e "S3_URI=$S3_URI" \
    -e "RESTORE_JOBS=$RESTORE_JOBS" \
    -e "SHOW_PROGRESS=$SHOW_PROGRESS" \
    -e "PGOPTIONS=$PGOPTIONS" \
    "$PG_AWS_IMG" \
    bash -c "
      set -eu
      aws_opts=\"\"
      [[ -n \"\$S3_ENDPOINT_URL\" ]] && aws_opts=\"--endpoint-url \$S3_ENDPOINT_URL\"
      [[ \"\$AWS_NO_VERIFY_SSL\" == \"true\" ]] && aws_opts=\"\$aws_opts --no-verify-ssl\"
      verbose_flag=\"\"
      [[ \"\$SHOW_PROGRESS\" == \"true\" ]] && verbose_flag=\"--verbose\"
      # Stream from S3 to pg_restore, ignoring SIGPIPE from aws when pg_restore closes early
      set +o pipefail  # Disable pipefail to handle SIGPIPE gracefully
      aws s3 cp \"\$S3_URI\" - \$aws_opts 2>/dev/null | \
      pg_restore --section=data --no-owner --no-privileges -j \"\$RESTORE_JOBS\" \
                   \$verbose_flag \
                   -h \"\$DST_PGHOST\" -p \"\$DST_PGPORT\" -U \"\$DST_PGUSER\" -d \"\$DST_DBNAME\"
      # Return pg_restore exit code directly
      exit \$?
    "; then
    error "Phase 3 failed"
    exit 1
  fi

  # Phase 4: post-data
  log "Phase 4/4: Loading constraints & triggers"
  if ! podman run --rm \
    -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
    -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
    -e "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" \
    -e "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION" \
    -e "AWS_S3_FORCE_PATH_STYLE=$AWS_S3_FORCE_PATH_STYLE" \
    -e "S3_ENDPOINT_URL=$S3_ENDPOINT_URL" \
    -e "AWS_NO_VERIFY_SSL=$AWS_NO_VERIFY_SSL" \
    -e "PGPASSWORD=$DST_PGPASSWORD" \
    -e "DST_PGHOST=$DST_PGHOST" \
    -e "DST_PGPORT=$DST_PGPORT" \
    -e "DST_PGUSER=$DST_PGUSER" \
    -e "DST_DBNAME=$DST_DBNAME" \
    -e "S3_URI=$S3_URI" \
    -e "RESTORE_JOBS=$RESTORE_JOBS" \
    -e "SHOW_PROGRESS=$SHOW_PROGRESS" \
    -e "PGOPTIONS=$PGOPTIONS" \
    -v "$PD_LIST:/tmp/pd.list:ro" \
    "$PG_AWS_IMG" \
    bash -c "
      set -eu
      aws_opts=\"\"
      [[ -n \"\$S3_ENDPOINT_URL\" ]] && aws_opts=\"--endpoint-url \$S3_ENDPOINT_URL\"
      [[ \"\$AWS_NO_VERIFY_SSL\" == \"true\" ]] && aws_opts=\"\$aws_opts --no-verify-ssl\"
      verbose_flag=\"\"
      [[ \"\$SHOW_PROGRESS\" == \"true\" ]] && verbose_flag=\"--verbose\"
      # Stream from S3 to pg_restore, ignoring SIGPIPE from aws when pg_restore closes early
      set +o pipefail  # Disable pipefail to handle SIGPIPE gracefully
      aws s3 cp \"\$S3_URI\" - \$aws_opts 2>/dev/null | \
      pg_restore --use-list=/tmp/pd.list --section=post-data \
                 --no-owner --no-privileges -j \"\$RESTORE_JOBS\" \
                 \$verbose_flag \
                 -h \"\$DST_PGHOST\" -p \"\$DST_PGPORT\" -U \"\$DST_PGUSER\" -d \"\$DST_DBNAME\"
      # Return pg_restore exit code directly
      exit \$?
    "; then
    error "Phase 4 failed"
    exit 1
  fi

  success "Restore completed successfully"
}

# ---------- MAIN FLOW ------------------------------------------------

# Parse CLI arguments
parse_args "$@"

# Check requirements
require_cmd podman

# Validate configuration
validate_config

# Confirm action
confirm_action

debug "After confirm_action, MODE=$MODE"

# Build image
build_image_if_needed

debug "After build_image_if_needed"

# Setup cleanup
trap cleanup EXIT INT TERM

debug "Trap set, SKIP_CHECKS=$SKIP_CHECKS"

# Check database connections if not skipped
if [[ "$SKIP_CHECKS" == "false" ]]; then
  if [[ "$MODE" == "backup" ]] || [[ "$MODE" == "all" ]]; then
    check_db_connection "source"
  fi
  if [[ "$MODE" == "restore" ]] || [[ "$MODE" == "all" ]]; then
    check_db_connection "destination"
  fi
fi

# Execute based on mode
debug "About to execute mode: $MODE"
case "$MODE" in
  all)
    debug "Executing 'all' mode"
    log "Running full migration (backup + restore)"
    do_backup
    do_restore
    success "Full migration completed successfully"
    ;;
  backup)
    debug "Executing 'backup' mode"
    do_backup
    ;;
  restore)
    debug "Executing 'restore' mode"
    do_restore
    ;;
  *)
    error "Unknown mode: $MODE"
    exit 1
    ;;
esac

debug "Script completed normally"
