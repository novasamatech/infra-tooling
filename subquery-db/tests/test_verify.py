# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

from sqdb.verify import compare_maps, split_foreign


def test_target_objects_the_restore_did_not_create_are_reported_not_compared():
    source = {
        "app.t": ("table", "app"),
        "public.f()": ("function", "public"),
        "extension btree_gist": ("comment", None),
    }
    target = {
        "app.t": ("table", "app"),
        "app.extra": ("table", "app"),  # in a restored schema: a real difference
        "public.f()": ("changed", "public"),  # changed: a real difference
        "public.service_table": ("table", "public"),
        "service.audit": ("table", "service"),  # schema the source does not have
        "extension btree_gist": ("comment", None),
        "extension service_ext": ("comment", None),
    }
    src, dst, foreign = split_foreign(source, target, {"app", "public"})
    assert foreign == ["extension service_ext", "public.service_table", "service.audit"]
    diff = compare_maps(src, dst)
    assert diff.extra == ["app.extra"]
    assert [change["name"] for change in diff.changed] == ["public.f()"]
    assert not diff.missing
    _, _, foreign = split_foreign({}, {"app.t": (1, "app")}, set())
    assert foreign == ["app.t"]
