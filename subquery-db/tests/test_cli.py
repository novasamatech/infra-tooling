# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import logging

import pytest
import yaml

from sqdb import cli
from sqdb.config import parse_config
from sqdb.log import MASK, MASKER, setup_logging
from sqdb.pg import conninfo


@pytest.fixture
def config_file(tmp_path, raw_config):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    return path


def test_validate_prints_the_plan_without_secrets(config_file, capsys):
    assert cli.main(["--config", str(config_file), "validate"]) == 0
    out = capsys.readouterr().out
    assert "chain-b-accounts → chain-b" in out and "3 database(s)" in out
    assert "acc-pw" not in out and "s3-secret" not in out


def test_config_errors_exit_with_code_2(tmp_path, config_file, capsys):
    assert cli.main(["--config", str(tmp_path / "missing.yaml"), "validate"]) == cli.EXIT_CONFIG
    assert cli.main(["--config", str(config_file), "validate", "--group", "nope"]) == 2


def test_status_reads_the_state(tmp_path, config_file, capsys):
    from sqdb.state import State

    state = State(tmp_path / "work" / "state" / "20261027-pg17.json")
    state.update("chain-b-accounts", "backup", status="done", size=2048)
    state.update("chain-b-accounts", "restore", status="failed", phases_done=[1, 2])
    args = ["--config", str(config_file), "--work-dir", str(tmp_path / "work"), "status"]
    assert cli.main(args) == 0
    out = capsys.readouterr().out
    assert "done 2.0 KiB" in out and "failed phases [1, 2]" in out


def test_mutating_commands_need_confirmation(config, monkeypatch, caplog):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    assert not cli.confirm("migrate", config, list(config.databases), assume_yes=False)
    assert cli.confirm("migrate", config, list(config.databases), assume_yes=True)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "yes")
    assert cli.confirm("migrate", config, list(config.databases), assume_yes=False)
    monkeypatch.setattr("builtins.input", lambda _: "y")
    assert not cli.confirm("migrate", config, list(config.databases), assume_yes=False)

    def eof(_):
        raise EOFError

    monkeypatch.setattr("builtins.input", eof)
    assert not cli.confirm("migrate", config, list(config.databases), assume_yes=False)


def test_parser_rejects_conflicting_dump_flags():
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["backup", "--reuse-dumps", "--overwrite-dumps"])
    args = parser.parse_args(["restore", "--from-phase", "3", "--clean-target", "--db", "a"])
    assert (args.from_phase, args.to_phase, args.clean_target, args.db) == (3, 4, True, ["a"])


def test_log_masking(tmp_path, capsys):
    MASKER.update({"hunter2"})
    log_file = tmp_path / "run.log"
    logger = setup_logging(verbose=True, log_file=log_file)
    logger.info("password is hunter2")
    logging.getLogger("sqdb").handlers[1].flush()
    assert f"password is {MASK}" in capsys.readouterr().err
    assert "hunter2" not in log_file.read_text()


def test_conninfo_quotes_values_and_omits_the_password(raw_config):
    raw_config["source"]["host"] = "db host"
    config = parse_config(raw_config)
    text = conninfo(config.source, "it's", "us\\er")
    assert "host='db host'" in text and "dbname='it\\'s'" in text and "user='us\\\\er'" in text
    assert "keepalives='1'" in text and "password" not in text


@pytest.mark.parametrize(
    "args",
    [
        ["backup", "--parallel", "0"],
        ["backup", "--parallel", "-1"],
        ["backup", "--parallel", "65"],
        ["backup", "--progress-interval", "nan"],
        ["backup", "--progress-interval", "inf"],
        ["backup", "--progress-interval", "-1"],
        ["restore", "--from-phase", "3", "--clean-target"],
        ["restore", "--from-phase", "4", "--to-phase", "2"],
    ],
)
def test_invalid_cli_options_fail_before_connecting(config_file, args):
    assert cli.main(["--config", str(config_file), *args]) == cli.EXIT_CONFIG


def test_invalid_state_does_not_raise_a_traceback(config_file, tmp_path, capsys):
    work = tmp_path / "work"
    (work / "state").mkdir(parents=True)
    (work / "state" / "20261027-pg17.json").write_text("{broken")
    assert cli.main(["--config", str(config_file), "--work-dir", str(work), "status"]) == 1
    output = capsys.readouterr().err
    assert "cannot read state" in output and "Traceback" not in output
