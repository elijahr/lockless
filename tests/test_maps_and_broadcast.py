"""Tests for BroadcastRing, ConcurrentDict (Ctrie), SkipListMap, and SkipListSet (HIGH-03)."""

from __future__ import annotations

import gc
import sys
import threading

import pytest
from lockless import (
    BroadcastRing,
    ConcurrentDict,
    Empty,
    LaggedError,
    SkipListMap,
    SkipListSet,
    drain_deferred_decrefs,
)


class _RefPayload:
    def __init__(self, tag: str, num: int) -> None:
        self.tag = tag
        self.num = num

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _RefPayload) and self.tag == other.tag and self.num == other.num


def test_broadcast_ring_fanout_and_lag_recovery() -> None:
    with pytest.raises(ValueError, match="power of 2"):
        BroadcastRing(capacity=8)

    ring: BroadcastRing[str] = BroadcastRing(capacity=16, overflow="drop_oldest", max_readers=4)
    sub1 = ring.subscribe()
    sub2 = ring.subscribe()
    assert ring.subscriber_count == 2

    ring.publish("m0")
    ring.publish("m1")
    assert sub1.poll() == "m0"
    assert sub1.poll() == "m1"
    assert sub2.poll() == "m0"

    # Publish 17 more items (m2..m18) into capacity=16 ring so slot 1 is overwritten by m17
    for i in range(2, 19):
        ring.publish(f"m{i}")

    with pytest.raises(LaggedError) as exc_info:
        sub2.poll(raise_on_lag=True)
    assert exc_info.value.skipped_count == 16
    assert sub2.total_skipped == 16

    # After lag fast-forward to slot 1's new occupant (m17), sub2 reads m17 and m18
    assert [sub2.poll(), sub2.poll()] == ["m17", "m18"]
    with pytest.raises(Empty):
        sub2.poll()

    sub1.unsubscribe()
    sub2.unsubscribe()
    assert ring.subscriber_count == 0
    ring.destroy()


def test_broadcast_ring_backpressure_and_refcount_cleanup() -> None:
    ring: BroadcastRing[_RefPayload] = BroadcastRing(
        capacity=16, overflow="backpressure", max_readers=2
    )
    sub = ring.subscribe()

    objs = [_RefPayload("bp", i) for i in range(16)]
    baselines = [sys.getrefcount(o) for o in objs]

    for idx in range(len(objs)):
        assert ring.try_publish(objs[idx]) is True

    # Negative control: 17th publish in backpressure mode with unread subscriber returns False
    extra = _RefPayload("extra", 99)
    extra_base = sys.getrefcount(extra)
    assert ring.try_publish(extra) is False
    assert sys.getrefcount(extra) == extra_base

    read_back = [sub.poll() for _ in range(16)]
    assert read_back == objs
    del read_back

    sub.unsubscribe()
    ring.destroy()
    drain_deferred_decrefs()
    gc.collect()
    assert [sys.getrefcount(o) for o in objs] == baselines


def test_broadcast_ring_drop_oldest_eviction_refcounts() -> None:
    ring: BroadcastRing[_RefPayload] = BroadcastRing(
        capacity=16, overflow="drop_oldest", max_readers=2
    )
    evicted = [_RefPayload("evict", i) for i in range(64)]
    baselines = [sys.getrefcount(o) for o in evicted]

    for idx in range(len(evicted)):
        ring.publish(evicted[idx])

    ring.destroy()
    drain_deferred_decrefs()
    gc.collect()
    assert [sys.getrefcount(o) for o in evicted] == baselines


def test_concurrent_dict_crud_and_snapshot_isolation() -> None:
    d: ConcurrentDict[object, object] = ConcurrentDict({"a": 1, "b": 2})
    assert len(d) == 2
    assert d["a"] == 1
    assert d.get("missing", "fallback") == "fallback"

    # Put returns previous value
    assert d.put("a", 10) == 1
    assert d["a"] == 10
    assert d.put_if_absent("a", 99) == 10
    assert d.put_if_absent("c", 3) == 3

    # Diverse key types: int, float, bytes, tuple
    d[42] = "int-key"
    d[3.5] = "float-key"
    d[b"raw"] = "bytes-key"
    d[(1, "two")] = "tuple-key"
    assert d[42] == "int-key"
    assert d[3.5] == "float-key"
    assert d[b"raw"] == "bytes-key"
    assert d[(1, "two")] == "tuple-key"

    # O(1) Snapshot isolation
    snap = d.snapshot()
    assert len(snap) == 7
    assert snap["a"] == 10

    # Mutate live dict after snapshot
    d["a"] = 999
    del d["b"]
    d["new_key"] = "after_snap"

    # Snapshot must still reflect exact point-in-time state
    assert snap["a"] == 10
    assert snap["b"] == 2
    assert "new_key" not in snap
    snap.close()

    # Negative controls
    with pytest.raises(KeyError):
        _ = d["nonexistent"]
    with pytest.raises(KeyError):
        del d["nonexistent"]
    with pytest.raises(TypeError):
        _ = d[[1, 2, 3]]  # type: ignore[index]


def test_concurrent_dict_overwrite_and_remove_refcounts() -> None:
    d: ConcurrentDict[str, _RefPayload] = ConcurrentDict()
    values = [_RefPayload("v", i) for i in range(100)]
    baselines = [sys.getrefcount(v) for v in values]

    # Repeatedly overwrite the same key 100 times (HIGH-03)
    for idx in range(len(values)):
        d["hot_key"] = values[idx]

    assert d.pop("hot_key") == values[-1]
    d.destroy()
    drain_deferred_decrefs()
    gc.collect()

    assert [sys.getrefcount(v) for v in values] == baselines


def test_skiplist_map_and_set_ordering_and_refcounts() -> None:
    m: SkipListMap[int, _RefPayload] = SkipListMap()
    keys = [40, 10, 90, 20, 70, 30, 80, 50, 60]
    objs = {k: _RefPayload("sl", k) for k in keys}
    baselines = {k: sys.getrefcount(objs[k]) for k in keys}

    for k in keys:
        assert m.put(k, objs[k]) is True

    # Keys and items are yielded in strictly ascending sorted order
    assert m.keys() == [10, 20, 30, 40, 50, 60, 70, 80, 90]
    assert [v.num for v in m.values()] == [10, 20, 30, 40, 50, 60, 70, 80, 90]

    # Overwrite existing key returns False (not newly inserted)
    replacement = _RefPayload("sl-new", 400)
    rep_base = sys.getrefcount(replacement)
    assert m.put(40, replacement) is False
    assert m[40] == replacement

    removed = m.remove(40)
    assert removed == replacement
    del removed

    m.clear()
    m.destroy()
    drain_deferred_decrefs()
    gc.collect()

    assert sys.getrefcount(replacement) == rep_base
    assert {k: sys.getrefcount(objs[k]) for k in keys} == baselines

    # SkipListSet ordering & negative controls
    s: SkipListSet[str] = SkipListSet(["pear", "apple", "orange", "banana"])
    assert s.add("apple") is False
    assert s.add("grape") is True
    assert s.to_list() == ["apple", "banana", "grape", "orange", "pear"]
    assert "banana" in s
    s.remove("banana")
    assert "banana" not in s
    with pytest.raises(KeyError):
        s.remove("banana")
    assert s.discard("banana") is False
    s.destroy()


def test_concurrent_dict_multithreaded_contention() -> None:
    d: ConcurrentDict[int, int] = ConcurrentDict()
    num_threads = 4
    keys_per_thread = 100

    def worker(tid: int) -> None:
        base = tid * keys_per_thread
        for i in range(keys_per_thread):
            d[base + i] = (base + i) * 10
            d[base + i] = (base + i) * 20

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(d) == num_threads * keys_per_thread
    for k in range(num_threads * keys_per_thread):
        assert d[k] == k * 20
    d.destroy()
