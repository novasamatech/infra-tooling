-- Copyright © 2026 Novasama Technologies GmbH
-- SPDX-License-Identifier: Apache-2.0
--
-- SubQuery-like project schema with small deterministic data. It runs as the
-- group user (not a superuser), like a SubQuery node does, so only trusted
-- extensions are used. __SCHEMA__ is replaced by the quoted schema name and
-- __ROWS__ by the row count of the largest table.
--
-- Besides the usual SubQuery objects (historical entities with _id/_block_range,
-- GiST indexes, _metadata, _poi, hashed enum names, notification triggers,
-- @foreignKey smart comments) it covers the TOC entry kinds the restore phases
-- must place correctly: partitioned indexes (INDEX ATTACH), exclusion and unique
-- constraints, deferrable foreign keys, comments on indexes, identity and
-- generated columns, materialized views with indexes and sequence values.

CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

CREATE SCHEMA __SCHEMA__;
COMMENT ON SCHEMA __SCHEMA__ IS 'SubQuery project schema';
SET search_path = __SCHEMA__, public;

-- SubQuery names enum types by a hash of the project and enum name.
CREATE TYPE "6f1a2b3c4d" AS ENUM ('Pending', 'Success', 'Failed');
CREATE DOMAIN positive_amount AS numeric CHECK (VALUE >= 0);
CREATE TYPE call_args AS (pallet text, method text);

CREATE TABLE _metadata (
    key character varying(255) NOT NULL PRIMARY KEY,
    value jsonb,
    "createdAt" timestamp with time zone NOT NULL,
    "updatedAt" timestamp with time zone NOT NULL
);

CREATE TABLE _poi (
    id bigint NOT NULL PRIMARY KEY,
    chain_block_hash bytea UNIQUE,
    hash bytea UNIQUE,
    parent_hash bytea,
    operation_hash_root bytea,
    project_id integer NOT NULL DEFAULT 1
);

-- Historical entities: versions of the same id with disjoint block ranges.
CREATE TABLE accounts (
    id text NOT NULL,
    "publicKey" text,
    "firstTransferBlock" integer,
    "lastTransferBlock" integer,
    _id uuid NOT NULL DEFAULT gen_random_uuid(),
    _block_range int8range NOT NULL,
    CONSTRAINT accounts_pkey PRIMARY KEY (_id)
);
CREATE INDEX "0x2a1e3c4b5d6f7a8b" ON accounts USING gist (id, _block_range);
CREATE INDEX "0x9c8b7a6f5e4d3c2b" ON accounts USING btree ("publicKey");
COMMENT ON TABLE accounts IS E'@foreignKey ("id") REFERENCES transfers ("fromId")';

CREATE TABLE transfers (
    id text NOT NULL,
    amount positive_amount NOT NULL,
    "fromId" text NOT NULL,
    "toId" text NOT NULL,
    "blockNumber" integer NOT NULL,
    "timestamp" timestamp without time zone,
    status "6f1a2b3c4d" NOT NULL,
    call call_args,
    "extrinsicHash" text,
    tags text[],
    payload jsonb,
    _id uuid NOT NULL,
    _block_range int8range NOT NULL,
    CONSTRAINT transfers_pkey PRIMARY KEY (_id),
    CONSTRAINT transfers_no_overlap EXCLUDE USING gist (id WITH =, _block_range WITH &&)
);
CREATE INDEX "0x1f2e3d4c5b6a7980" ON transfers USING gist (id, _block_range);
CREATE INDEX "0x5a4b3c2d1e0f9a8b" ON transfers USING btree ("fromId");
CREATE INDEX "0x6b5a4c3d2e1f0a9b" ON transfers USING btree ("toId");
CREATE INDEX transfers_block_include ON transfers USING btree ("blockNumber") INCLUDE (amount);
CREATE INDEX transfers_open_status ON transfers (status) WHERE status <> 'Success';
CREATE INDEX transfers_hash_lower ON transfers (lower("extrinsicHash"));
CREATE INDEX transfers_hash_eq ON transfers USING hash ("extrinsicHash");
CREATE INDEX transfers_time_brin ON transfers USING brin ("timestamp");
CREATE INDEX transfers_tags ON transfers USING gin (tags);
CREATE INDEX transfers_payload ON transfers USING gin (payload jsonb_path_ops);
CREATE INDEX transfers_hash_trgm ON transfers USING gin ("extrinsicHash" gin_trgm_ops);
COMMENT ON TABLE transfers IS E'@foreignKey ("fromId") REFERENCES accounts (id)';
COMMENT ON COLUMN transfers.amount IS 'smallest currency units';
COMMENT ON INDEX "0x5a4b3c2d1e0f9a8b" IS 'lookup by sender';

-- Non-historical entities with real foreign keys.
CREATE TABLE blocks (
    id text PRIMARY KEY,
    number bigint NOT NULL UNIQUE,
    hash bytea NOT NULL,
    "parentHash" bytea,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    body text
);

CREATE TABLE extrinsics (
    id text PRIMARY KEY,
    "blockId" text NOT NULL REFERENCES blocks (id) ON UPDATE CASCADE ON DELETE CASCADE,
    idx integer NOT NULL,
    "isSigned" boolean NOT NULL,
    fee numeric(38, 0),
    seq bigint GENERATED ALWAYS AS IDENTITY,
    size_kb numeric GENERATED ALWAYS AS (idx::numeric / 1024) STORED,
    CONSTRAINT extrinsics_block_idx UNIQUE ("blockId", idx)
);

CREATE TABLE events (
    id bigserial PRIMARY KEY,
    "extrinsicId" text REFERENCES extrinsics (id) DEFERRABLE INITIALLY DEFERRED,
    section text CHECK (section <> ''),
    method text,
    data jsonb
);
CREATE INDEX events_section_method ON events (section, method);

-- Partitioned table: the partitioned index restores as INDEX + INDEX ATTACH.
CREATE TABLE rewards (
    id text NOT NULL,
    era integer NOT NULL,
    amount numeric NOT NULL,
    PRIMARY KEY (id, era)
) PARTITION BY RANGE (era);
CREATE TABLE rewards_early PARTITION OF rewards FOR VALUES FROM (0) TO (50);
CREATE TABLE rewards_late PARTITION OF rewards FOR VALUES FROM (50) TO (MAXVALUE);
CREATE INDEX rewards_amount ON rewards (amount);

CREATE SEQUENCE batch_seq START 100 INCREMENT 5;

-- Subscription notifications, as SubQuery installs them.
CREATE FUNCTION send_notification() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    payload jsonb;
BEGIN
    payload := jsonb_build_object('table', TG_TABLE_NAME, 'op', TG_OP,
        'id', CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END);
    PERFORM pg_notify(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, payload::text);
    RETURN NULL;
END;
$$;

CREATE FUNCTION touch_updated() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW."updatedAt" := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER "0x7e6d5c4b_notify_trigger"
    AFTER INSERT OR UPDATE OR DELETE ON transfers
    FOR EACH ROW EXECUTE FUNCTION send_notification();
CREATE TRIGGER metadata_touch BEFORE UPDATE ON _metadata
    FOR EACH ROW EXECUTE FUNCTION touch_updated();

CREATE VIEW transfer_totals AS
    SELECT "fromId", count(*) AS transfers, sum(amount) AS total
    FROM transfers WHERE upper_inf(_block_range) GROUP BY "fromId";

CREATE MATERIALIZED VIEW daily_volume AS
    SELECT date_trunc('day', "timestamp") AS day, count(*) AS transfers, sum(amount) AS volume
    FROM transfers WHERE "timestamp" IS NOT NULL GROUP BY 1;
CREATE UNIQUE INDEX daily_volume_day ON daily_volume (day);

-- Data ------------------------------------------------------------------------

INSERT INTO _metadata (key, value, "createdAt", "updatedAt") VALUES
    ('lastProcessedHeight', '123456', '2024-01-01', '2024-01-02'),
    ('chain', '"0x00112233445566778899aabbccddeeff"', '2024-01-01', '2024-01-01'),
    ('indexerNodeVersion', '"5.2.0"', '2024-01-01', '2024-01-01'),
    ('historicalStateEnabled', 'true', '2024-01-01', '2024-01-01');

INSERT INTO _poi (id, chain_block_hash, hash, parent_hash, operation_hash_root)
SELECT i, decode(md5('c' || i), 'hex'), decode(md5('h' || i), 'hex'),
       decode(md5('h' || (i - 1)), 'hex'), CASE WHEN i % 4 = 0 THEN NULL ELSE '\x00ff'::bytea END
FROM generate_series(1, greatest(__ROWS__ / 10, 1)) AS i;

INSERT INTO accounts (id, "publicKey", "firstTransferBlock", "lastTransferBlock", _id, _block_range)
SELECT 'acc-' || (i / 2), CASE WHEN i % 9 = 0 THEN NULL ELSE '0x' || md5(i::text) END, i, i * 2,
       md5('account' || i)::uuid, int8range(i, CASE WHEN i % 2 = 1 THEN NULL ELSE i + 1 END)
FROM generate_series(1, greatest(__ROWS__ / 5, 2)) AS i;

INSERT INTO transfers (id, amount, "fromId", "toId", "blockNumber", "timestamp", status, call,
                       "extrinsicHash", tags, payload, _id, _block_range)
SELECT 'tr-' || (i / 2), (i * 1000)::numeric, 'acc-' || (i % 37), 'acc-' || (i % 41), i,
       CASE WHEN i % 17 = 0 THEN NULL ELSE '2024-01-01'::timestamp + i * interval '7 minutes' END,
       (ARRAY['Pending', 'Success', 'Failed'])[1 + i % 3]::"6f1a2b3c4d",
       ROW('balances', CASE WHEN i % 2 = 0 THEN 'transfer' ELSE 'transfer_keep_alive' END)::call_args,
       '0x' || md5('ext' || i), ARRAY['tag-' || (i % 7), 'ünïcødé ✓'],
       jsonb_build_object('i', i, 'note', E'quote '' and backslash \\ and tab\t', 'even', i % 2 = 0),
       md5('transfer' || i)::uuid, int8range(i, CASE WHEN i % 2 = 1 THEN NULL ELSE i + 1 END)
FROM generate_series(1, __ROWS__) AS i;

INSERT INTO blocks (id, number, hash, "parentHash", "createdAt", body)
SELECT 'blk-' || i, i, decode(md5('b' || i), 'hex'), decode(md5('b' || (i - 1)), 'hex'),
       '2024-01-01'::timestamptz + i * interval '6 seconds',
       -- About 2.6 kB of poorly compressible text, so the value is TOASTed.
       (SELECT string_agg(md5(i::text || '-' || j), '') FROM generate_series(1, 80) AS j)
FROM generate_series(1, greatest(__ROWS__ / 2, 1)) AS i;

INSERT INTO extrinsics (id, "blockId", idx, "isSigned", fee)
SELECT 'ext-' || i, 'blk-' || (1 + i % greatest(__ROWS__ / 2, 1)), i, i % 3 = 0,
       CASE WHEN i % 5 = 0 THEN NULL ELSE i::numeric * 123456789 END
FROM generate_series(1, greatest(__ROWS__ / 2, 1)) AS i;

INSERT INTO events ("extrinsicId", section, method, data)
SELECT CASE WHEN i % 6 = 0 THEN NULL ELSE 'ext-' || (1 + i % greatest(__ROWS__ / 2, 1)) END,
       'balances', CASE WHEN i % 2 = 0 THEN 'Transfer' ELSE 'Deposit' END,
       jsonb_build_object('amount', i * 10)
FROM generate_series(1, __ROWS__) AS i;

INSERT INTO rewards (id, era, amount)
SELECT 'acc-' || (i % 13), i, (i * 7)::numeric
FROM generate_series(0, greatest(__ROWS__ / 5, 60)) AS i;

REFRESH MATERIALIZED VIEW daily_volume;
SELECT setval('batch_seq', 100 + 5 * __ROWS__);
