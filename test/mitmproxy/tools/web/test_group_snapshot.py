import hashlib
import sqlite3
import threading
import tracemalloc

import pytest

from mitmproxy.tools.web import group_snapshot as gs


def build(tmp_path, rows, **kwargs):
    return gs.GroupSnapshot.build(
        lambda cancel: iter(rows), directory=tmp_path, **kwargs
    )


def test_label_override_preserves_identity_order_and_published_versions(tmp_path):
    with gs.GroupStore.build(
        lambda _: [gs.Metadata("a", "z", label="first"), gs.Metadata("b", "z")],
        directory=tmp_path,
    ) as store:
        old = store.publish()
        token = gs.group_identity("z").key
        assert old.group(token)["label"] == "first"
        store.apply([gs.Metadata("b", "z", label="changed"), gs.Metadata("c", "a")])
        with store.publish() as new:
            assert [g["key"] for g in new.groups()] == [
                gs.group_identity("a").key,
                token,
            ]
            assert new.group(token)["label"] == "changed"
            assert new.group(token)["count"] == 2
            assert new.locate("a")["group"]["key"] == token
            assert old.group(token)["label"] == "first"
            assert old.group(token)["count"] == 2


def test_label_override_keeps_oversized_identities_distinct(tmp_path):
    values = ["x" * (1024 * 1024 + 1) + suffix for suffix in ("a", "b")]
    with build(
        tmp_path,
        [
            gs.Metadata(str(i), value, label="value too large to group")
            for i, value in enumerate(values)
        ],
        max_value_bytes=None,
    ) as snap:
        groups = snap.groups()
        assert [g["label"] for g in groups] == ["value too large to group"] * 2
        assert [g["key"] for g in groups] == [
            gs.group_identity(v, max_value_bytes=None).key for v in values
        ]
        assert [snap.members(g["key"])[0]["flow_id"] for g in groups] == ["0", "1"]


@pytest.mark.parametrize("label", ["x" * 257, 123, "\ud800"])
def test_invalid_label_override_rolls_back(tmp_path, label):
    with gs.GroupStore(directory=tmp_path) as store:
        store.apply([gs.Metadata("a", "x")])
        with pytest.raises((ValueError, UnicodeEncodeError)):
            store.apply([gs.Metadata("b", "y"), gs.Metadata("a", "x", label=label)])
        with store.publish() as snap:
            assert snap.member_count == 1
            assert snap.groups()[0]["label"] == "x"
            assert snap.locate("b") is None


def test_label_override_accepts_empty_and_256_characters(tmp_path):
    with build(
        tmp_path,
        [gs.Metadata("a", "a", label=""), gs.Metadata("b", "b", label="x" * 256)],
    ) as snap:
        assert [g["label"] for g in snap.groups()] == ["", "x" * 256]


def test_only_member_update_retains_explicit_label(tmp_path):
    with gs.GroupStore(directory=tmp_path) as store:
        store.apply([gs.Metadata("a", "x", label="override")])
        old = store.publish()
        store.apply([gs.Metadata("a", "x", {"method": "GET"})])
        with store.publish() as current:
            assert current.groups()[0]["label"] == "override"
            assert old.groups()[0]["label"] == "override"


def test_invalid_order_and_store_options(tmp_path):
    for summary in ({"timestamp_created": True}, {"sequence": "1"}):
        with pytest.raises(ValueError, match="member"):
            build(tmp_path, [gs.Metadata("a", "x", summary)])
    for options in ({"max_versions": 0}, {"grace_seconds": -1}, {"max_wal_bytes": 1}):
        with pytest.raises(ValueError):
            gs.GroupStore(directory=tmp_path, **options)
    with pytest.raises(ValueError, match="flow ID"):
        gs._summary(gs.Metadata("", "x"))


def test_spool_transaction_and_closed_store_guards(tmp_path):
    store = gs.GroupStore(directory=tmp_path)
    store.db.execute("CREATE TABLE spool (id INTEGER)")
    store.db.execute("INSERT INTO spool VALUES (1)")
    with pytest.raises(gs.SnapshotError, match="commit"):
        store.apply([])
    with pytest.raises(gs.SnapshotError, match="uncommitted"):
        store.publish()
    store.db.rollback()
    with pytest.raises(ValueError, match="does not match"):
        store.apply({"other": gs.Metadata("a", "x")})
    store.close()
    assert store.wal_bytes == 0
    with pytest.raises(gs.SnapshotError, match="closed"):
        store.publish()


def test_initialization_and_publish_database_errors_cleanup(tmp_path, monkeypatch):
    connect = sqlite3.connect

    def fail_connect(path):
        raise sqlite3.OperationalError("injected connect failure")

    monkeypatch.setattr(gs.sqlite3, "connect", fail_connect)
    with pytest.raises(gs.SnapshotError, match="initialization"):
        gs.GroupStore(directory=tmp_path)
    assert list(tmp_path.iterdir()) == []
    monkeypatch.setattr(gs.sqlite3, "connect", connect)
    with gs.GroupStore(directory=tmp_path) as store:
        monkeypatch.setattr(gs.sqlite3, "connect", fail_connect)
        with pytest.raises(gs.SnapshotError, match="reader failed"):
            store.publish()
        assert not store._versions
        cancel = threading.Event()

        def cancel_connect(path):
            cancel.set()
            raise sqlite3.OperationalError("interrupted")

        monkeypatch.setattr(gs.sqlite3, "connect", cancel_connect)
        with pytest.raises(gs.SnapshotCancelled):
            store.publish(cancel=cancel)
        assert not store._versions

        def unexpected_connect(path):
            raise RuntimeError("unexpected reader failure")

        monkeypatch.setattr(gs.sqlite3, "connect", unexpected_connect)
        with pytest.raises(RuntimeError, match="unexpected reader failure"):
            store.publish()
        assert not store._versions


def test_facade_cleans_up_when_publish_fails(tmp_path, monkeypatch):
    def fail_publish(self, *, cancel=None):
        raise RuntimeError("publish failed")

    monkeypatch.setattr(gs.GroupStore, "publish", fail_publish)
    with pytest.raises(RuntimeError, match="publish failed"):
        build(tmp_path, [gs.Metadata("a", "x")])
    assert list(tmp_path.iterdir()) == []


def test_sqlite_query_failure_is_wrapped(tmp_path):
    with build(tmp_path, [gs.Metadata("a", "x")]) as snap:
        snap._db.close()
        with pytest.raises(gs.SnapshotError, match="query failed"):
            snap.locate("a")


@pytest.mark.parametrize("mode", ["cancel", "wal", "integrity"])
def test_update_sqlite_errors_rollback_and_reuse(tmp_path, monkeypatch, mode):
    event = threading.Event()
    with gs.GroupStore(directory=tmp_path) as store:
        store.apply([gs.Metadata("a", "x")])
        old = store.publish()
        write = store._write

        def fail(sql, args):
            if sql.startswith("INSERT INTO members"):
                if mode == "cancel":
                    event.set()
                    raise sqlite3.OperationalError("interrupted")
                if mode == "wal":
                    store._wal_full = True
                    raise sqlite3.OperationalError("interrupted")
                raise sqlite3.IntegrityError("injected constraint")
            return write(sql, args)

        monkeypatch.setattr(store, "_write", fail)
        with pytest.raises(
            gs.SnapshotCancelled if mode == "cancel" else gs.SnapshotError
        ):
            store.apply([gs.Metadata("b", "y")], cancel=event)
        monkeypatch.setattr(store, "_write", write)
        event.clear()
        with store.publish() as current:
            assert current.groups() == old.groups()
            assert current.locate("b") is None
        store.apply([gs.Metadata("b", "y")])
        assert old.locate("b") is None


def test_large_value_wal_admission_on_build_and_changes(tmp_path, monkeypatch):
    monkeypatch.setattr(gs, "GROUP_CACHE_BYTES", 1024)
    value = "x" * (2 * 1024 * 1024 + 100)
    with gs.GroupStore.build(
        lambda _: [gs.Metadata("a", value), gs.Metadata("b", value)],
        directory=tmp_path,
        max_value_bytes=None,
        max_bytes=128 * 1024**2,
    ) as store:
        store.apply(["a"])
        store.apply(["b"])
        with store.publish() as snap:
            assert snap.member_count == 0


def test_incremental_versions_own_summaries_and_order(tmp_path):
    store = gs.GroupStore.build(
        lambda cancel: iter(
            [
                gs.Metadata("b", "z", {"timestamp_created": 2, "sequence": 2}),
                gs.Metadata("a", "z", {"timestamp_created": 1, "sequence": 1}),
            ]
        ),
        directory=tmp_path,
    )
    old = store.publish()
    key = gs.group_identity("z").key
    assert [m["flow_id"] for m in old.members(key)] == ["a", "b"]
    store.apply(
        {
            "a": gs.Metadata(
                "a",
                "a",
                {
                    "timestamp_created": 3,
                    "sequence": 3,
                    "path": "/" + "x" * 4095,
                    "live": True,
                },
            ),
            "b": None,
            "c": gs.Metadata("c", None, {"timestamp_created": 0, "sequence": 4}),
        }
    )
    new = store.publish()
    assert [(g["label"], g["count"], g["members_before"]) for g in new.groups()] == [
        ("a", 1, 0),
        ("(none)", 1, 1),
    ]
    assert new.locate("b") is None
    assert new.locate("a")["member_ordinal"] == 0
    assert new.members(gs.group_identity("a").key)[0]["summary"]["live"] is True
    assert [m["flow_id"] for m in old.members(key)] == ["a", "b"]
    assert old.group_count == 1 and old.member_count == 2
    assert new.group_count == 2 and new.member_count == 2
    old.close()
    new.close()
    store.close()
    assert list(tmp_path.iterdir()) == []


def test_incremental_rollback_and_independent_cancellation(tmp_path):
    store = gs.GroupStore.build(
        lambda cancel: [gs.Metadata("a", "x")], directory=tmp_path
    )
    cancelled = threading.Event()
    old = store.publish(cancel=cancelled)
    with pytest.raises(ValueError):
        store.apply([gs.Metadata("b", "y"), gs.Metadata("c", "z", {"body": "no"})])
    current = store.publish()
    assert current.locate("b") is None
    cancelled.set()
    with pytest.raises(gs.SnapshotCancelled):
        old.groups()
    assert current.groups()[0]["count"] == 1
    store.apply({"a": None})
    with store.publish() as empty:
        assert empty.member_count == empty.group_count == 0
    assert current.locate("a") is not None
    store.close()
    assert list(tmp_path.iterdir()) == []


def test_block_churn_preserves_group_and_member_prefixes(tmp_path):
    with gs.GroupStore(directory=tmp_path) as store:
        store.apply(gs.Metadata(str(i), i) for i in reversed(range(1600)))
        store.apply(str(i) for i in range(0, 1500))
        store.apply(
            gs.Metadata(str(i), -1, {"timestamp_created": -i, "sequence": i})
            for i in range(0, 1500)
        )
        with store.publish() as snap:
            groups = snap.groups(limit=500)
            assert [g["label"] for g in groups] == ["-1"] + [
                str(i) for i in range(1500, 1600)
            ]
            assert [g["members_before"] for g in groups] == [0] + list(
                range(1500, 1600)
            )
            key = groups[0]["key"]
            for offset in (0, 127, 255, 256, 511, 512, 1000, 1499):
                assert snap.members(key, offset=offset, limit=1)[0]["flow_id"] == str(
                    1499 - offset
                )
                assert snap.locate(str(1499 - offset))["member_ordinal"] == offset
        store.apply(str(i) for i in range(0, 1400))
        # Every non-singleton namespace remains densely blocked after churn.
        assert (
            store.db.execute("SELECT max(count) FROM blocks").fetchone()[0]
            <= gs.BLOCK_SIZE
        )
        assert store.db.execute("SELECT count(*) FROM blocks").fetchone()[0] == 102
        assert (
            store.db.execute(
                "SELECT count(*) FROM items i LEFT JOIN blocks b ON b.id=i.block WHERE b.id IS NULL"
            ).fetchone()[0]
            == 0
        )
        with store.publish() as snap:
            assert snap.member_count == 200
            assert snap.groups()[1]["members_before"] == 100
            assert snap.locate("1400")["member_ordinal"] == 99


def test_versions_retention_grace_and_progress_batches(tmp_path, monkeypatch):
    now = [0.0]
    monkeypatch.setattr(gs.time, "monotonic", lambda: now[0])
    with gs.GroupStore(directory=tmp_path, max_versions=2, grace_seconds=10) as store:
        empty = store.publish()
        store.apply([gs.Metadata("a", "x")])
        first = store.publish()
        store.apply([gs.Metadata("b", "a")])
        with pytest.raises(gs.SnapshotError, match="retention"):
            store.publish()
        assert empty.groups() == []
        assert first.locate("b") is None
        assert first.groups()[0]["count"] == 1
        now[0] = 10
        with store.publish() as latest:
            assert latest.member_count == latest.group_count == 2
        with pytest.raises(gs.SnapshotError, match="closed"):
            first.groups()
        assert empty._closed
    assert list(tmp_path.iterdir()) == []


def test_default_retention_supports_one_second_revisions(tmp_path, monkeypatch):
    now = [0.0]
    monkeypatch.setattr(gs.time, "monotonic", lambda: now[0])
    with gs.GroupStore(directory=tmp_path) as store:
        assert store.max_wal_bytes <= 256 * 1024**2
        versions = []
        for second in range(40):
            now[0] = second
            store.apply([gs.Metadata("a", second)])
            versions.append(store.publish())
            for age in range(min(second, 14) + 1):
                assert versions[second - age].groups()[0]["label"] == str(second - age)
        assert versions[0]._closed


def test_apply_batch_bound_rolls_back(tmp_path):
    with gs.GroupStore(directory=tmp_path) as store:
        old = store.publish()
        with pytest.raises(gs.SnapshotError, match="8192"):
            store.apply(gs.Metadata(str(i), "x") for i in range(8193))
        with store.publish() as current:
            assert old.member_count == current.member_count == 0
        store.apply([gs.Metadata("ok", "x")])


def test_initial_build_checkpoints_without_per_statement_stats(tmp_path, monkeypatch):
    calls = []
    original = gs.GroupStore.wal_bytes.fget

    def measured(store):
        calls.append(True)
        return original(store)

    monkeypatch.setattr(gs.GroupStore, "wal_bytes", property(measured))
    with gs.GroupStore.build(
        lambda _: (gs.Metadata(str(i), i % 5) for i in range(17000)),
        directory=tmp_path,
    ) as store:
        assert len(calls) < 500
        assert store.wal_bytes < 1024**2
        with store.publish() as snapshot:
            assert snapshot.member_count == 17000
            assert snapshot.locate("16999")["member_ordinal"] == 3399


def test_initial_build_failure_after_committed_chunks_cleans_scratch(tmp_path):
    closed = []

    def source(cancel):
        try:
            for i in range(17000):
                yield gs.Metadata(str(i), i % 5)
            raise RuntimeError("late source failure")
        finally:
            closed.append(True)

    with pytest.raises(RuntimeError, match="late source failure"):
        gs.GroupStore.build(source, directory=tmp_path)
    assert closed == [True]
    assert list(tmp_path.iterdir()) == []


def test_large_block_merge_admitted_before_rewriting_rows(tmp_path):
    with gs.GroupStore.build(
        lambda _: (gs.Metadata(str(i), f"{i:04}" + "x" * 10000) for i in range(600)),
        directory=tmp_path,
    ) as store:
        old = store.publish()
        store.max_wal_bytes = 32 * 1024**2
        with pytest.raises(gs.SnapshotError, match="WAL bound"):
            store.apply(["599"])
        assert store.wal_bytes < store.max_wal_bytes
        with store.publish() as current:
            assert current.member_count == old.member_count == 600
            assert current.locate("599") == old.locate("599")


def test_real_scratch_full_rolls_back_update(tmp_path):
    with gs.GroupStore(directory=tmp_path, max_bytes=65536) as store:
        store.apply([gs.Metadata("a", "x")])
        old = store.publish()
        with pytest.raises(gs.SnapshotError, match="scratch") as caught:
            store.apply(gs.Metadata(str(i), "y" * 2000 + str(i)) for i in range(1000))
        assert "full" in str(caught.value.__cause__)
        assert not store.db.in_transaction
        with store.publish() as current:
            assert current.member_count == 1
            assert current.locate("0") is None
            assert current.groups() == old.groups()
        store.apply([gs.Metadata("b", "x")])
        assert old.members(old.groups()[0]["key"]) == [
            {"flow_id": "a", "ordinal": 0, "summary": {}}
        ]


def test_wal_backpressure_does_not_invalidate_retained_reader(tmp_path):
    with gs.GroupStore(
        directory=tmp_path, max_bytes=65536, max_wal_bytes=1024**2, grace_seconds=3600
    ) as store:
        store.apply([gs.Metadata("a", "x")])
        old = store.publish()
        for i in range(100):
            try:
                store.apply([gs.Metadata("a", "x", {"path": str(i) + "x" * 4000})])
            except gs.SnapshotError as exc:
                assert "WAL bound" in str(exc)
                break
        else:
            pytest.fail("Pinned scratch WAL grew without backpressure")
        assert store.wal_bytes <= store.max_wal_bytes
        assert old.members(old.groups()[0]["key"])[0]["summary"] == {}
        old.close()
        store.apply([gs.Metadata("b", None)])
        with store.publish() as current:
            assert current.member_count == 2


def test_unbounded_exceptional_values_keep_full_identity(tmp_path):
    prefix = "X" * (1024 * 1024 + 1)
    with build(
        tmp_path,
        [gs.Metadata("a", prefix + "A"), gs.Metadata("b", prefix + "B")],
        max_value_bytes=None,
    ) as snap:
        groups = snap.groups()
        assert len(groups) == 2
        assert groups[0]["label"] == groups[1]["label"]
        assert groups[0]["key"] != groups[1]["key"]
        assert (
            snap.locate("a")["group"]["key"]
            == gs.group_identity(prefix + "A", max_value_bytes=None).key
        )


def test_same_timestamp_sequence_uses_exact_id_and_signed_time(tmp_path):
    rows = [
        gs.Metadata("z", None, {"timestamp_created": 0.0, "sequence": 1}),
        gs.Metadata("a", None, {"timestamp_created": -0.0, "sequence": 1}),
        gs.Metadata("first", None, {"timestamp_created": -100.5, "sequence": 3}),
        gs.Metadata("later", None, {"timestamp_created": 1.5, "sequence": -10}),
    ]
    with build(tmp_path, rows) as snap:
        assert [m["flow_id"] for m in snap.members(snap.groups()[0]["key"])] == [
            "first",
            "a",
            "z",
            "later",
        ]


def test_cancelled_apply_rolls_back_and_writer_can_be_reused(tmp_path):
    cancel = threading.Event()
    with gs.GroupStore(directory=tmp_path) as store:
        store.apply([gs.Metadata("a", "x")])
        old = store.publish()

        def changes():
            yield gs.Metadata("a", "new")
            cancel.set()
            yield gs.Metadata("b", "new")

        with pytest.raises(gs.SnapshotCancelled):
            store.apply(changes(), cancel=cancel)
        assert not store.db.in_transaction
        with store.publish() as current:
            assert current.groups() == old.groups()
            assert current.locate("b") is None
        store.apply({"a": None})
        assert old.locate("a") is not None


def test_source_close_failure_cleans_scratch(tmp_path):
    class Rows:
        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self):
            raise RuntimeError("close failed")

    with pytest.raises(RuntimeError, match="close failed"):
        gs.GroupStore.build(lambda cancel: Rows(), directory=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_typed_full_tokens_and_host_normalization():
    values = [None, "", b"", 0, "0", b"0"]
    assert len({gs.group_identity(v).key for v in values}) == 6
    assert gs.group_identity("Straße", host=True) == gs.group_identity(
        "STRASSE", host=True
    )
    assert gs.group_identity("GET").key != gs.group_identity("get").key
    assert (
        gs.group_identity("abc").key == "g1:" + hashlib.sha256(b"t\x00abc").hexdigest()
    )
    prefix = "x" * 1000
    assert gs.group_identity(prefix + "a").key != gs.group_identity(prefix + "b").key
    assert len(gs.group_identity(prefix).label) <= 160
    assert gs.group_identity(b"\xff\x00").label == "b'\\xff\\x00'"


def test_groups_members_and_lookup(tmp_path):
    rows = [
        gs.Metadata("a", "z", {"method": "GET"}),
        gs.Metadata("b", None),
        gs.Metadata("c", "z"),
        gs.Metadata("d", 10),
        gs.Metadata("e", 2),
        gs.Metadata("f", b"z"),
    ]
    with build(tmp_path, rows) as snap:
        groups = snap.groups(limit=10)
        assert [
            (g["label"], g["ordinal"], g["count"], g["members_before"]) for g in groups
        ] == [
            ("2", 0, 1, 0),
            ("10", 1, 1, 1),
            ("z", 2, 2, 2),
            ("b'z'", 3, 1, 4),
            ("(none)", 4, 1, 5),
        ]
        assert snap.group_count == 5
        assert snap.member_count == 6
        group = groups[2]
        assert set(group) == {"key", "label", "ordinal", "count", "members_before"}
        assert snap.group(group["key"]) == group
        assert snap.groups(offset=2, limit=1) == [group]
        assert snap.members(group["key"], offset=1, limit=1) == [
            {"flow_id": "c", "ordinal": 1, "summary": {}}
        ]
        assert snap.members(group["key"], limit=1)[0]["summary"] == {"method": "GET"}
        assert snap.locate("c") == {"group": group, "member_ordinal": 1}
        assert snap.locate("missing") is None
        assert snap.group("g1:" + "0" * 64) is None
        assert snap.members("g1:" + "0" * 64) == []
        assert snap.groups(offset=100) == []
    assert list(tmp_path.iterdir()) == []
    snap.close()
    with pytest.raises(gs.SnapshotError, match="closed"):
        snap.groups()


@pytest.mark.parametrize("null_position", range(12))
def test_null_last_with_typed_canonical_order(tmp_path, null_position):
    values = [b"\xff", "é", 10, b"", "a", -2, "", b"\x00", 2, "10", "Z"]
    values.insert(null_position, None)
    values.append(None)
    with build(
        tmp_path, [gs.Metadata(str(i), value) for i, value in enumerate(values)]
    ) as snap:
        groups = snap.groups()
        assert [
            (g["label"], g["ordinal"], g["count"], g["members_before"]) for g in groups
        ] == [
            ("-2", 0, 1, 0),
            ("2", 1, 1, 1),
            ("10", 2, 1, 2),
            ("", 3, 1, 3),
            ("10", 4, 1, 4),
            ("Z", 5, 1, 5),
            ("a", 6, 1, 6),
            ("é", 7, 1, 7),
            ("b''", 8, 1, 8),
            ("b'\\x00'", 9, 1, 9),
            ("b'\\xff'", 10, 1, 10),
            ("(none)", 11, 2, 11),
        ]
        null_group = groups[-1]
        assert null_group["key"] == "g1:" + hashlib.sha256(b"n\x00").hexdigest()
        assert snap.groups(offset=11, limit=1) == [null_group]
        assert snap.group(null_group["key"]) == null_group
        assert snap.members(null_group["key"]) == [
            {"flow_id": str(null_position), "ordinal": 0, "summary": {}},
            {"flow_id": "12", "ordinal": 1, "summary": {}},
        ]
        assert snap.locate("12") == {"group": null_group, "member_ordinal": 1}
        assert snap.member_count == 13
        assert snap.group_count == 12


def test_all_null_members(tmp_path):
    with build(tmp_path, [gs.Metadata(str(i), None) for i in range(3)]) as snap:
        group = snap.groups()[0]
        assert (group["ordinal"], group["count"], group["members_before"]) == (0, 3, 0)
        assert snap.member_count == 3
        assert snap.group_count == 1
        for i in range(3):
            assert snap.locate(str(i)) == {"group": group, "member_ordinal": i}
            assert snap.members(group["key"], offset=i, limit=1) == [
                {"flow_id": str(i), "ordinal": i, "summary": {}}
            ]


def test_empty_and_host_groups(tmp_path):
    with build(tmp_path, []) as snap:
        assert snap.groups() == []
        assert snap.member_count == snap.group_count == 0
    with build(
        tmp_path, [gs.Metadata("1", "Straße"), gs.Metadata("2", "STRASSE")], host=True
    ) as snap:
        assert [(g["label"], g["count"]) for g in snap.groups()] == [("strasse", 2)]


def test_duplicate_ids_are_not_silently_double_counted(tmp_path):
    with pytest.raises(gs.SnapshotError, match="Duplicate"):
        build(tmp_path, [gs.Metadata("a", "x"), gs.Metadata("a", "y")])
    assert list(tmp_path.iterdir()) == []


def test_collision_is_detected(tmp_path, monkeypatch):
    class Digest:
        def hexdigest(self):
            return "0" * 64

    monkeypatch.setattr(gs.hashlib, "sha256", lambda value: Digest())
    with pytest.raises(gs.SnapshotError, match="collision"):
        build(tmp_path, [gs.Metadata("a", "x"), gs.Metadata("b", "y")])
    assert list(tmp_path.iterdir()) == []


def test_cancel_closes_source_and_cleans_scratch(tmp_path):
    cancel = threading.Event()
    closed = []

    def source(event):
        try:
            yield gs.Metadata("a", "x")
            event.set()
            yield gs.Metadata("b", "y")
        finally:
            closed.append(True)

    with pytest.raises(gs.SnapshotCancelled):
        gs.GroupSnapshot.build(source, directory=tmp_path, cancel=cancel)
    assert closed == [True]
    assert list(tmp_path.iterdir()) == []


def test_query_cancellation(tmp_path):
    cancel = threading.Event()
    with build(tmp_path, [gs.Metadata("a", "x")], cancel=cancel) as snap:
        cancel.set()
        with pytest.raises(gs.SnapshotCancelled):
            snap.groups()


def test_disk_budget(tmp_path):
    with pytest.raises(gs.SnapshotError, match="scratch"):
        build(
            tmp_path,
            (gs.Metadata(str(i), "x" * 2000 + str(i)) for i in range(1000)),
            max_bytes=65536,
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "row",
    [
        gs.Metadata("", "x"),
        gs.Metadata("a", True),
        gs.Metadata("a", 1.5),
        gs.Metadata("a", 2**63),
        gs.Metadata("a", "x", {"body": "secret"}),
        gs.Metadata("a", "x", {"method": "x" * 2048}),
        gs.Metadata("a", "x", {"status": []}),
    ],
)
def test_invalid_metadata(tmp_path, row):
    with pytest.raises(ValueError):
        build(tmp_path, [row])
    assert list(tmp_path.iterdir()) == []


def test_bounds(tmp_path):
    with pytest.raises(ValueError):
        build(tmp_path, [], max_bytes=1)
    with pytest.raises(ValueError):
        build(tmp_path, [gs.Metadata("a", "x" * 100)], max_value_bytes=10)
    with build(tmp_path, []) as snap:
        for options in ({"limit": 0}, {"limit": 501}, {"offset": -1}, {"offset": True}):
            with pytest.raises(ValueError):
                snap.groups(**options)
            with pytest.raises(ValueError):
                snap.members("bad", **options)
        with pytest.raises(ValueError):
            snap.group("bad")
        with pytest.raises(ValueError):
            snap.locate("")


@pytest.mark.parametrize("value", [b"1234", "éé", "ßß"])
def test_value_limit_counts_encoded_normalized_bytes(value):
    with pytest.raises(ValueError, match="byte bound"):
        gs.group_identity(value, host=True, max_value_bytes=3)


def test_additional_record_bounds(tmp_path):
    with pytest.raises(ValueError):
        gs.group_identity("x", max_value_bytes=0)
    with pytest.raises(ValueError):
        gs.group_identity("xxxx", max_value_bytes=3)
    with pytest.raises(ValueError):
        build(tmp_path, [], max_value_bytes=0)
    for summary in (
        {str(i): None for i in range(100)},
        {"status": 2**63},
        {key: "é" * 1024 for key in gs.SUMMARY_FIELDS},
        {"timestamp_start": float("nan")},
    ):
        with pytest.raises(ValueError):
            build(tmp_path, [gs.Metadata("a", "x", summary)])
    assert list(tmp_path.iterdir()) == []


def test_source_failure_and_precancel_cleanup(tmp_path):
    def failed(cancel):
        yield gs.Metadata("a", "x")
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError, match="source failed"):
        gs.GroupSnapshot.build(failed, directory=tmp_path)
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(gs.SnapshotCancelled):
        gs.GroupSnapshot.build(failed, directory=tmp_path, cancel=cancel)
    assert list(tmp_path.iterdir()) == []


def test_sqlite_interrupt_during_build(tmp_path, monkeypatch):
    cancel = threading.Event()
    connect = sqlite3.connect

    class InterruptConnection(sqlite3.Connection):
        def set_progress_handler(self, callback, steps):
            def interrupt():
                cancel.set()
                return callback()

            super().set_progress_handler(interrupt, 1)

    monkeypatch.setattr(
        gs.sqlite3, "connect", lambda path: connect(path, factory=InterruptConnection)
    )
    with pytest.raises(gs.SnapshotCancelled):
        build(tmp_path, [], cancel=cancel)
    assert list(tmp_path.iterdir()) == []


def test_sqlite_interrupt_and_query_error(tmp_path):
    cancel = threading.Event()
    with build(tmp_path, [gs.Metadata("a", "x")], cancel=cancel) as snap:

        def interrupt():
            cancel.set()
            return 1

        snap._db.set_progress_handler(interrupt, 1)
        with pytest.raises(gs.SnapshotCancelled):
            snap.groups()
        cancel.clear()
        snap._db.close()
        with pytest.raises(gs.SnapshotError, match="query failed"):
            snap.groups()


def test_reused_snapshot_query_scope_interrupts_sqlite(tmp_path):
    original = threading.Event()
    later = threading.Event()
    with build(
        tmp_path, (gs.Metadata(str(i), "x") for i in range(1000)), cancel=original
    ) as snap:
        key = snap.groups()[0]["key"]
        with snap.query_scope(later) as scoped:
            assert scoped is snap
            # Set the later request's event after _check, inside SQLite execution.
            snap._db.set_trace_callback(
                lambda sql: (
                    later.set() if sql.startswith("SELECT payload,summary") else None
                )
            )
            try:
                with pytest.raises(gs.SnapshotCancelled) as caught:
                    scoped.members(key, limit=500)
                assert isinstance(caught.value.__cause__, sqlite3.OperationalError)
                assert "interrupted" in str(caught.value.__cause__)
            finally:
                snap._db.set_trace_callback(None)
        assert not original.is_set()
        assert snap.locate("999")["member_ordinal"] == 999
        with snap.query_scope(threading.Event()):
            assert snap.members(key, offset=999)[0]["flow_id"] == "999"


def test_query_scope_replaces_old_cancel_and_restores_on_error(tmp_path):
    original = threading.Event()
    later = threading.Event()
    with build(tmp_path, [gs.Metadata("a", "x")], cancel=original) as snap:
        original.set()
        with pytest.raises(RuntimeError, match="request error"):
            with snap.query_scope(later):
                assert snap.groups()[0]["count"] == 1
                assert snap.locate("a")["member_ordinal"] == 0
                raise RuntimeError("request error")
        with pytest.raises(gs.SnapshotCancelled):
            snap.groups()
        original.clear()
        assert snap.groups()[0]["count"] == 1


def test_query_scope_precancel_nested_and_closed(tmp_path):
    original = threading.Event()
    outer = threading.Event()
    cancelled = threading.Event()
    cancelled.set()
    snap = build(tmp_path, [gs.Metadata("a", None)], cancel=original)
    with snap.query_scope(outer):
        with pytest.raises(gs.SnapshotCancelled):
            with snap.query_scope(cancelled):
                pytest.fail("Precancelled scope entered")
        assert snap.groups()[0]["count"] == 1
        outer.set()
        with pytest.raises(gs.SnapshotCancelled):
            snap.groups()
    assert snap.groups()[0]["count"] == 1
    snap.close()
    with pytest.raises(gs.SnapshotError, match="closed"):
        with snap.query_scope(threading.Event()):
            pytest.fail("Closed snapshot scope entered")
    assert list(tmp_path.iterdir()) == []


def test_scratch_file_budget_and_source_release(tmp_path):
    closed = []

    def source(cancel):
        try:
            yield gs.Metadata("a", "x")
        finally:
            closed.append(True)

    with gs.GroupSnapshot.build(source, directory=tmp_path, max_bytes=65536) as snap:
        assert closed == [True]
        files = list(tmp_path.rglob("*"))
        assert snap._store.size_bytes == sum(
            p.stat().st_size for p in files if p.is_file()
        )
        assert snap._store.path.stat().st_size <= 65536
        assert snap._store.wal_bytes <= snap._store.max_wal_bytes
        assert {p.name for p in files if p.is_file()} == {
            "snapshot.sqlite",
            "snapshot.sqlite-wal",
            "snapshot.sqlite-shm",
        }
        assert snap.locate("a")["member_ordinal"] == 0


def test_large_paging_uses_indexes(tmp_path):
    with build(tmp_path, (gs.Metadata(str(i), i % 100) for i in range(10000))) as snap:
        last = snap.groups(offset=99, limit=1)[0]
        assert last["members_before"] == 9900
        assert snap.members(last["key"], offset=99, limit=1)[0]["flow_id"] == "9999"
        assert snap.locate("9999")["member_ordinal"] == 99
        queries = []
        snap._db.set_trace_callback(queries.append)
        snap.groups(offset=99, limit=1)
        snap.members(last["key"], offset=99, limit=1)
        snap.locate("9999")
        snap._db.set_trace_callback(None)
        for sql in queries:
            plan = str(snap._db.execute("EXPLAIN QUERY PLAN " + sql).fetchall())
            assert "SEARCH" in plan
            assert "TEMP B-TREE" not in plan


def test_compact_uuid_members(tmp_path):
    with build(
        tmp_path,
        (
            gs.Metadata(
                f"{i:08x}-0000-0000-0000-000000000000", f"host-{i % 10000}.example"
            )
            for i in range(100000)
        ),
        # Incremental membership/order indexes trade space for bounded updates.
        max_bytes=64 * 1024 * 1024,
    ) as snap:
        assert snap.member_count == 100000
        assert snap.group_count == 10000
        location = snap.locate("0001869f-0000-0000-0000-000000000000")
        assert location["member_ordinal"] == 9
        assert (
            snap.members(location["group"]["key"], offset=9)[0]["flow_id"]
            == "0001869f-0000-0000-0000-000000000000"
        )


def test_incremental_update_queries_are_indexed_and_bounded(tmp_path):
    statements = []
    with gs.GroupStore.build(
        lambda cancel: (gs.Metadata(str(i), i % 10) for i in range(10000)),
        directory=tmp_path,
    ) as store:
        store.db.set_trace_callback(statements.append)
        store.apply([gs.Metadata("9999", -1, {"timestamp_created": -1, "sequence": 0})])
        store.db.set_trace_callback(None)
        assert sum(sql.startswith("UPDATE groups") for sql in statements) <= 2
        assert sum(sql.startswith("UPDATE items SET block") for sql in statements) <= 2
        for sql in statements:
            if sql.startswith(("SELECT", "UPDATE", "DELETE")) and "state" not in sql:
                plan = str(store.db.execute("EXPLAIN QUERY PLAN " + sql).fetchall())
                assert "SEARCH" in plan, sql
                assert "TEMP B-TREE" not in plan, sql
        with store.publish() as snap:
            assert snap.groups()[0]["members_before"] == 0
            assert snap.locate("9999")["member_ordinal"] == 0


def test_cache_eviction_preserves_counts_and_order(tmp_path, monkeypatch):
    monkeypatch.setattr(gs, "GROUP_CACHE_BYTES", 1024)
    rows = [gs.Metadata(str(i), i % 7) for i in range(100)]
    with build(tmp_path, rows) as snap:
        group = snap.group(gs.group_identity(0).key)
        assert group["count"] == 15
        assert [m["flow_id"] for m in snap.members(group["key"])] == [
            str(i) for i in range(0, 100, 7)
        ]
        assert snap.locate("98")["member_ordinal"] == 14


def test_uuid_packing_preserves_exact_flow_ids(tmp_path):
    ids = [
        "abcdef01-2345-6789-abcd-0123456789ab",
        "ABCDEF01-2345-6789-abcd-0123456789ab",
        "abcdef0123456789abcd0123456789ab",
        "not-a-uuid",
        "é",
    ]
    with build(tmp_path, [gs.Metadata(id, None) for id in ids]) as snap:
        key = snap.groups()[0]["key"]
        assert [m["flow_id"] for m in snap.members(key)] == ids
        assert [snap.locate(id)["member_ordinal"] for id in ids] == list(range(5))


def test_cached_values_are_not_rehashed(tmp_path, monkeypatch):
    calls = []
    sha256 = hashlib.sha256

    def digest(value):
        calls.append(value)
        return sha256(value)

    monkeypatch.setattr(gs.hashlib, "sha256", digest)
    with build(
        tmp_path,
        (gs.Metadata(str(i), "Straße" if i % 2 else "STRASSE") for i in range(1000)),
        host=True,
    ) as snap:
        assert snap.groups()[0]["count"] == 1000
    assert calls == [b"t\x00strasse"]


def test_oversized_host_rejected_before_normalization_allocation(tmp_path):
    rows = [gs.Metadata("a", "A" * (4 * 1024 * 1024))]
    tracemalloc.start()
    try:
        with pytest.raises(ValueError):
            build(tmp_path, rows, host=True, max_value_bytes=1024)
        assert tracemalloc.get_traced_memory()[1] < 2 * 1024 * 1024
    finally:
        tracemalloc.stop()
    assert list(tmp_path.iterdir()) == []
