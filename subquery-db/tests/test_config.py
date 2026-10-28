# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import pytest

from sqdb import __version__
from sqdb.config import ConfigError, format_size, load_config, parse_config, parse_size

from .conftest import BASE


def test_version_matches_release_file():
    from pathlib import Path

    assert (Path(__file__).parents[1] / ".version").read_text().strip() == __version__


def test_groups_resolve_credentials_targets_and_option_inheritance(config):
    by_name = {db.name: db for db in config.databases}
    assert list(by_name) == ["chain-a-accounts", "chain-b-accounts", "chain-a-transfers"]
    assert by_name["chain-b-accounts"].target == "chain-b"
    assert by_name["chain-a-accounts"].target == "chain-a-accounts"
    accounts = by_name["chain-a-accounts"]
    assert accounts.source_credentials == accounts.target_credentials
    assert accounts.source_credentials.user == "accounts"
    transfers = by_name["chain-a-transfers"]
    assert transfers.options.verify_rows == "none"  # group override
    assert transfers.options.probe_ddl is False  # database override
    assert accounts.options.probe_ddl is True  # built-in default
    assert config.storage_type == "s3" and config.s3.chunk_size is None
    assert config.destination.admin.user == "admin"
    assert config.destination.maintenance_db == "postgres"
    assert config.parallel == 1


def test_destination_credentials_override(raw_config):
    raw_config["groups"]["accounts"]["destination"] = {"user": "acc2", "password": "other"}
    db = parse_config(raw_config).databases[0]
    assert db.source_credentials.user == "accounts"
    assert (db.target_credentials.user, db.target_credentials.password) == ("acc2", "other")


def test_secrets_and_reprs_hide_values(config):
    assert config.secrets() == {"acc-pw", "tr-pw", "admin-pw", "s3-secret", "AKIA"}
    text = repr(config)
    for secret in config.secrets():
        assert secret not in text


def test_select_filters_and_rejects_unknown_names(config):
    assert [db.name for db in config.select(groups=["transfers"])] == ["chain-a-transfers"]
    assert [db.name for db in config.select(names=["chain-b-accounts"])] == ["chain-b-accounts"]
    assert config.select(groups=["accounts"], names=["chain-a-transfers"]) == []
    with pytest.raises(ConfigError, match="unknown group"):
        config.select(groups=["nope"])
    with pytest.raises(ConfigError, match="unknown source database"):
        config.select(names=["nope"])


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda c: c.update(version=2), "config.version"),
        (lambda c: c.update(version=True), "config.version"),
        (lambda c: c.update(version=1.0), "config.version"),
        (lambda c: c.update(extra=1), "unknown key"),
        (lambda c: c["groups"]["accounts"].update(password=12345), "quote the value"),
        (lambda c: c["groups"]["accounts"].update(password=True), "quote the value"),
        (lambda c: c["groups"]["accounts"].update(databases=[]), "non-empty list"),
        (lambda c: c["groups"]["accounts"].update(databases=[2024]), "quote names"),
        (lambda c: c["groups"]["accounts"].update(databases=["a/b"]), "invalid database name"),
        (lambda c: c["groups"]["accounts"].update(databases=["x" * 64]), "exceeds 63 bytes"),
        (lambda c: c["groups"].update({"bad group": c["groups"]["accounts"]}), "group name"),
        (
            lambda c: c["groups"]["transfers"].update(databases=["chain-a-accounts"]),
            "listed twice",
        ),
        (
            lambda c: c["groups"]["transfers"].update(
                databases=[{"name": "x", "target": "chain-b"}]
            ),
            "used twice",
        ),
        (lambda c: c["groups"]["transfers"].update(user="accounts"), "different passwords"),
        (lambda c: c["groups"]["accounts"].update(options={"restore_jobs": 2}), "needs local"),
        (lambda c: c["groups"]["accounts"].update(options={"dump_jobs": 2}), "format: directory"),
        (lambda c: c["groups"]["accounts"].update(options={"jobs": 2}), "unknown key"),
        (lambda c: c["groups"]["accounts"].update(options={"verify_rows": "some"}), "one of"),
        (
            lambda c: c["groups"]["accounts"].update(options={"dump_compress": "bogus"}),
            "dump_compress",
        ),
        (lambda c: c["storage"].update(local={"typo": True}), "unknown key"),
        (lambda c: c["destination"].update(host="old.example"), "overlap"),
        (lambda c: c["source"].update(port=70000), "between 1 and 65535"),
        (lambda c: c["source"].update(sslmode="maybe"), "one of"),
        (lambda c: c["storage"]["s3"].update(chunk_size="1MiB"), "between 5 MiB and 5 GiB"),
        (lambda c: c["storage"]["s3"].update(chunk_size="lots"), "invalid size"),
        (lambda c: c["storage"]["s3"].update(prefix="/dumps"), "must not start"),
        (lambda c: c["storage"].pop("s3"), "required for type s3"),
        (lambda c: c.update(backup_name="a b"), "backup_name"),
        (lambda c: c.update(backup_name="name\n"), "backup_name"),
        (lambda c: c.update(groups={}), "at least one group"),
        (lambda c: c["destination"].update(admin={"user": "admin"}), "password: required"),
    ],
)
def test_invalid_configs_are_rejected(raw_config, change, message):
    change(raw_config)
    with pytest.raises(ConfigError, match=message):
        parse_config(raw_config)


def test_local_storage_allows_parallel_jobs(raw_config):
    raw_config["storage"] = {"type": "local", "local": {"format": "directory", "path": "/dumps"}}
    raw_config["options"] = {"dump_jobs": 4, "restore_jobs": 2, "dump_compress": 3}
    config = parse_config(raw_config)
    assert config.s3 is None and config.local.path == "/dumps"
    assert {db.options.dump_jobs for db in config.databases} == {4}
    assert config.databases[0].options.dump_compress == "3"


def test_s3_prefix_and_chunk_size_are_normalised(raw_config):
    raw_config["storage"]["s3"].update(prefix="pg17", chunk_size="256MB")
    s3 = parse_config(raw_config).s3
    assert s3.prefix == "pg17/"
    assert s3.chunk_size == 256 * 1024**2


def test_yaml_duplicate_keys_and_bad_files(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("version: 1\nbackup_name: a\nbackup_name: b\n")
    with pytest.raises(ConfigError, match="duplicate key 'backup_name' at line 3"):
        load_config(path)
    path.write_text("version: [1\n")
    with pytest.raises(ConfigError, match="invalid YAML"):
        load_config(path)
    with pytest.raises(ConfigError, match="cannot read"):
        load_config(tmp_path / "missing.yaml")


def test_load_example_config():
    from pathlib import Path

    config = load_config(Path(__file__).parents[1] / "config.example.yaml")
    assert len(config.groups) >= 2
    assert BASE["version"] == 1


def test_yaml_errors_do_not_echo_credentials(tmp_path):
    path = tmp_path / "secret.yaml"
    path.write_text("password: [extremely-sensitive-password\n")
    with pytest.raises(ConfigError, match="invalid YAML at line") as caught:
        load_config(path)
    assert "extremely-sensitive" not in str(caught.value)
    path.write_text("? [complex, key]\n: value\n")
    with pytest.raises(ConfigError, match="keys must be strings"):
        load_config(path)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("64MiB", 64 * 1024**2),
        ("512MB", 512 * 1024**2),
        ("1G", 1024**3),
        (4096, 4096),
        ("5 mib", 5 * 1024**2),
    ],
)
def test_parse_size(value, expected):
    assert parse_size(value, "x") == expected


def test_format_size():
    assert format_size(512) == "512 B"
    assert format_size(1536) == "1.5 KiB"
    assert format_size(3 * 1024**4) == "3.0 TiB"
