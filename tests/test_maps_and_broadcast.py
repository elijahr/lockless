"""Tests for BroadcastRing, ConcurrentDict (Ctrie), SkipListMap, and SkipListSet (HIGH-03)."""

from __future__ import annotations

import gc
import math
import sys
import threading

import pytest
from lockless import (
    BroadcastRing,
    Closed,
    ConcurrentDict,
    Empty,
    LaggedError,
    SkipListMap,
    SkipListSet,
    drain_deferred_decrefs,
)
from lockless._cabi import enter_borrow, exit_borrow, lib


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


def test_zero_pickle_and_extreme_float_bigint_tuple_keys() -> None:
    import lockless._cabi as cabi_mod

    assert not hasattr(cabi_mod, "pickle")

    # 1. Extreme float keys (inf, -inf, nan, 1e300) and int/float equality without RangeDefect
    d: ConcurrentDict[object, str] = ConcurrentDict()
    d[1] = "one"
    d[float("inf")] = "pos_inf"
    d[float("-inf")] = "neg_inf"
    d[1e300] = "huge_float"
    nan_key = float("nan")
    d[nan_key] = "nan_val"

    assert d[1.0] == "one"
    assert d[1] == "one"
    assert d[float("inf")] == "pos_inf"
    assert d[float("-inf")] == "neg_inf"
    assert d[1e300] == "huge_float"
    assert math.isnan(float("nan"))

    # Overwrite 1 via 1.0
    assert d.put(1.0, "one_float") == "one"
    assert d[1] == "one_float"

    # Big integers (> 64-bit and < -2^63) in ConcurrentDict
    big_pos = 1 << 100
    big_neg = -(1 << 100)
    d[big_pos] = "big_pos"
    d[big_neg] = "big_neg"
    assert d[big_pos] == "big_pos"
    assert d[big_neg] == "big_neg"
    assert (1 << 101) not in d
    d.destroy()

    # 2. Order-preserving big integers in SkipListMap & SkipListSet
    sm_int: SkipListMap[int, int] = SkipListMap()
    big_keys = [
        1 << 128,
        -(1 << 128),
        1 << 80,
        -(1 << 80),
        (1 << 80) + 1,
        -(1 << 80) - 1,
        0,
        -1,
        1,
        (1 << 63) - 1,
        -(1 << 63),
    ]
    for k in big_keys:
        sm_int[k] = k * 2
    assert sm_int.keys() == sorted(big_keys)
    assert sm_int.values() == [k * 2 for k in sorted(big_keys)]
    sm_int.destroy()

    # 3. Order-preserving scalar tuples in SkipListSet
    s_tup: SkipListSet[tuple[int, str]] = SkipListSet()
    tuples = [
        (10, "b"),
        (2, "z"),
        (-100, "a"),
        (-2, "a"),
        (2, "a"),
        (1 << 80, "big"),
        (-(1 << 80), "neg_big"),
    ]
    for t in tuples:
        assert s_tup.add(t) is True
    assert s_tup.to_list() == sorted(tuples)

    # Negative control: non-comparable tuple element (None) in SkipListSet raises TypeError
    with pytest.raises(TypeError):
        s_tup.add((1, None))  # type: ignore[arg-type]
    s_tup.destroy()


def test_borrow_guard_blocks_cross_thread_deferred_decref() -> None:
    d: ConcurrentDict[str, _RefPayload] = ConcurrentDict()
    p1 = _RefPayload("p1", 1)
    p2 = _RefPayload("p2", 2)
    base1 = sys.getrefcount(p1)
    d["k"] = p1

    # Enter borrow on main thread, then displace "k" -> p2 so p1 is retired into deferred queue
    enter_borrow()
    try:
        d["k"] = p2
        assert int(lib.ll_deferred_decref_count()) >= 1

        # Another thread calling drain_deferred_decrefs() while borrower is active must drain 0
        drained_in_worker: list[int] = []

        def worker() -> None:
            drained_in_worker.append(drain_deferred_decrefs())

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        assert drained_in_worker == [0]
        # p1 must still be kept alive in the deferred queue while borrow is active
        assert sys.getrefcount(p1) == base1 + 1
    finally:
        exit_borrow()

    drain_deferred_decrefs()
    assert sys.getrefcount(p1) == base1
    d.destroy()


def test_broadcast_ring_destroy_invalidates_active_subscribers() -> None:
    ring: BroadcastRing[int] = BroadcastRing(capacity=16)
    sub = ring.subscribe()
    for i in range(20):
        ring.publish(i)
    assert sub.poll(raise_on_lag=False) == 16
    assert sub.lag == 16

    # Destroying ring first must cleanly invalidate active subscribers
    ring.destroy()
    assert sub.is_closed is True
    assert sub.lag == 0
    with pytest.raises(Closed):
        sub.poll()
    # Calling unsubscribe() after ring.destroy() is a safe no-op
    sub.unsubscribe()


def test_broadcast_ring_concurrent_publish_and_poll_no_retired_box_leak() -> None:
    ring: BroadcastRing[_RefPayload] = BroadcastRing(capacity=16, overflow="drop_oldest")
    sub1 = ring.subscribe(from_earliest=True)
    sub2 = ring.subscribe(from_earliest=True)

    payloads = [_RefPayload("bcast", i) for i in range(160)]
    baselines = [sys.getrefcount(p) for p in payloads]

    def publisher() -> None:
        for p in payloads:
            ring.publish(p)

    polled1: list[int] = []
    polled2: list[int] = []

    def reader(sub: object, out: list[int]) -> None:
        from lockless.maps import Subscriber

        assert isinstance(sub, Subscriber)
        while True:
            val = sub.try_poll()
            if val is not None:
                out.append(val.num)
                del val
            elif ring.is_closed:
                break

    tp = threading.Thread(target=publisher)
    tr1 = threading.Thread(target=reader, args=(sub1, polled1))
    tr2 = threading.Thread(target=reader, args=(sub2, polled2))
    tp.start()
    tr1.start()
    tr2.start()
    tp.join()
    ring.close()
    tr1.join()
    tr2.join()

    sub1.unsubscribe()
    sub2.unsubscribe()
    ring.destroy()
    drain_deferred_decrefs()
    gc.collect()

    assert [sys.getrefcount(p) for p in payloads] == baselines


def test_debra_concurrent_readers_writers_and_snapshots_no_uaf() -> None:
    d: ConcurrentDict[int, _RefPayload] = ConcurrentDict()
    sm: SkipListMap[int, _RefPayload] = SkipListMap()
    key_count = 32
    initial_objs = [_RefPayload("init", k) for k in range(key_count)]
    for k in range(key_count):
        d[k] = initial_objs[k]
        sm[k] = initial_objs[k]

    stop_ev = threading.Event()
    errors: list[BaseException] = []

    def writer_worker(wid: int) -> None:
        try:
            for step in range(80):
                k = (wid * 17 + step) % key_count
                obj = _RefPayload(f"w{wid}", step)
                d[k] = obj
                sm.put(k, obj)
                if step % 10 == 0:
                    snap = d.snapshot()
                    _ = len(snap)
                    snap.close()
        except BaseException as exc:
            errors.append(exc)

    def reader_worker() -> None:
        try:
            while not stop_ev.is_set():
                for k in range(key_count):
                    v1 = d.get(k)
                    if v1 is not None:
                        assert isinstance(v1.num, int)
                    v2 = sm.get(k)
                    if v2 is not None:
                        assert isinstance(v2.num, int)
        except BaseException as exc:
            errors.append(exc)

    readers = [threading.Thread(target=reader_worker) for _ in range(3)]
    writers = [threading.Thread(target=writer_worker, args=(w,)) for w in range(2)]
    for r in readers:
        r.start()
    for w in writers:
        w.start()
    for w in writers:
        w.join()
    stop_ev.set()
    for r in readers:
        r.join()

    assert errors == []
    d.destroy()
    sm.destroy()
    drain_deferred_decrefs()
