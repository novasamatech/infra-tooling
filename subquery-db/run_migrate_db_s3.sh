#!/usr/bin/env bash

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

export SRC_DBNAME=
export SRC_PGUSER=
export SRC_PGPASSWORD=''

export DST_DBNAME=
export DST_PGUSER=
export DST_PGPASSWORD="${SRC_PGPASSWORD}"

export AWS_ACCESS_KEY_ID=''
export AWS_SECRET_ACCESS_KEY=''

export BACKUP_NAME=20250926-1

# to backup
./migrate_db_s3.sh --backup
# to restore
#./migrate_db_s3.sh --restore
# to backup and restore
#./migrate_db_s3.sh --all