# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import pytest

from sqdb.toc import parse_toc, schema_names

from .conftest import DATA

# A pg_restore -l listing of the harness fixture (pg_dump 17 from PostgreSQL 16).
SAMPLE = (DATA / "toc_sample.list").read_text()


def entry(toc, kind, tag):
    return next(e for e in toc.entries if e.type == kind and e.tag == tag)


def test_header_and_counts():
    toc = parse_toc(SAMPLE)
    assert toc.header["Format"] == "CUSTOM"
    assert toc.header["Dumped from database version"].startswith("16.")
    counts = toc.counts()
    assert counts["INDEX"] == 18
    assert counts["INDEX ATTACH"] == 4
    assert counts["FK CONSTRAINT"] == 2
    assert toc.schemas == {"app"}


def test_plain_indexes_and_their_attachments_go_to_phase_2():
    toc = parse_toc(SAMPLE)
    phase2 = {(e.type, e.tag) for e in toc.index_entries}
    assert ("INDEX", "rewards_amount") in phase2
    assert ("INDEX ATTACH", "rewards_early_amount_idx") in phase2
    assert ("COMMENT", 'INDEX "0x5a4b3c2d1e0f9a8b"') in phase2
    assert all(e.type in {"INDEX", "INDEX ATTACH", "COMMENT"} for e in toc.index_entries)


def test_constraint_index_attachments_stay_with_their_constraints():
    toc = parse_toc(SAMPLE)
    attach = entry(toc, "INDEX ATTACH", "rewards_early_pkey")
    assert not toc.is_index(attach)
    phase4 = toc.phase_list(4)
    assert attach.line in phase4
    lines = phase4.splitlines()
    pkey = entry(toc, "CONSTRAINT", "rewards_early rewards_early_pkey")
    assert lines.index(pkey.line) < lines.index(attach.line)


def test_phase_lists_partition_the_toc():
    toc = parse_toc(SAMPLE)
    phase2, phase4 = toc.phase_list(2).splitlines(), toc.phase_list(4).splitlines()
    assert not set(phase2) & set(phase4)
    assert sorted(phase2 + phase4) == sorted(e.line for e in toc.entries)
    with pytest.raises(ValueError):
        toc.phase_list(3)


def test_entries_with_blank_owner_and_multiword_types():
    toc = parse_toc(SAMPLE)
    ext = entry(toc, "EXTENSION", "btree_gist")
    assert ext.namespace == "-"
    assert entry(toc, "TABLE DATA", "transfers").namespace == "app"
    assert any(e.type == "SEQUENCE SET" for e in toc.entries)
    assert any(e.type == "MATERIALIZED VIEW DATA" for e in toc.entries)


def test_object_names_containing_index_do_not_move_entries():
    toc = parse_toc(
        "10; 1259 1 TABLE app my INDEX table owner\n"
        '11; 0 0 COMMENT app TABLE "my INDEX table" owner\n'
        "12; 1259 2 INDEX app real_idx owner\n"
        "13; 0 0 COMMENT app INDEX real_idx owner\n"
        '14; 0 0 COMMENT app INDEX "Quoted ""idx""" owner\n'
        '15; 1259 3 INDEX app Quoted "idx" owner\n'
    )
    assert [e.dump_id for e in toc.index_entries] == [12, 13, 14, 15]


def test_invalid_listings():
    with pytest.raises(ValueError, match="no entries"):
        parse_toc("; just a header\n")
    with pytest.raises(ValueError, match="unexpected TOC line"):
        parse_toc("not a toc line\n")


def test_verbose_dependencies_handle_spaces_in_schemas_indexes_and_owners():
    toc = parse_toc(
        "1; 1259 1 INDEX a schema leaf index an owner\n;\tdepends on: 10\n"
        "2; 0 0 INDEX ATTACH a schema leaf index an owner\n;\tdepends on: 1 10\n"
        '3; 0 0 COMMENT a schema INDEX "leaf index" an owner\n;\tdepends on: 1\n'
        "4; 2606 2 CONSTRAINT a schema table pk an owner\n;\tdepends on: 10\n"
        "5; 0 0 INDEX ATTACH a schema table pk an owner\n;\tdepends on: 4 10\n"
    )
    assert [e.dump_id for e in toc.index_entries] == [1, 2, 3]
    assert schema_names('CREATE SCHEMA "a schema";\nCREATE SCHEMA "a""b";\n') == {"a schema", 'a"b'}
