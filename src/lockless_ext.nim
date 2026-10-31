## ==============================================================================
## lockless_ext.nim — C-ABI Extension Layer for the `lockless` Python Package
## ==============================================================================
##
## Compiles to a native shared library (`liblockless_ext.dylib` / `.so` / `.dll`)
## with `--app:lib --noMain --mm:orc -d:useMalloc -d:noSignalHandler -d:release`.
##
## Provides:
## 1. All `lockfree/cabi` (`lf_*`) C-ABI symbols re-exported directly:
##    - `BQueue128` (`lf_bqueue128_*`)
##    - `MpmcQueue` (`lf_bounded_mpmc_create`, `lf_unbounded_mpmc_create`,
##      `lf_producer_acquire`, `lf_consumer_acquire`, `lf_push`, `lf_push_ex`,
##      `lf_pop`, `lf_pop_batch`, `lf_queue_close`, `lf_queue_destroy`)
##    - `StreamBuffer` (`lf_streambuffer_*`, `lf_stream_reserve_write`,
##      `lf_stream_commit_write`, `lf_stream_peek_read`, `lf_stream_consume_read`)
##    - `RendezvousChannel` (`lf_rendezvous_*`)
##    - `TaskPool` (`lf_taskpool_*`)
##    - `TokenBucket` & `LeakyBucket` (`lf_token_bucket_*`, `lf_leaky_bucket_*`)
## 2. Wait-Free Deferred `Py_DecRef` Queue (`ll_retire_pyobj`,
##    `ll_pop_deferred_decref`, `ll_deferred_decref_count`):
##    Allows SMR / container callbacks on any thread to enqueue `PyObject*`
##    pointers without acquiring the CPython GIL or risking lock-order inversion.
## 3. Dedicated `SpscQueue` C-ABI (`ll_spsc_*`) wrapping `BQueue[pointer, ccSingle, ccSingle]`.
## 4. `HIGH-03` Reference-Safe `BroadcastRing` (`ll_broadcast_*`):
##    Wraps `BroadcastRing[PyBroadcastBox]` (`sizeof(PyBroadcastBox) == 16 > 8`)
##    so `BroadcastRing` uses its `VBox` + hazard-pointer (`readingBox`) path
##    and automatically retires overwritten/evicted `PyObject*` references on
##    `omDropOldest` and remaining slot references on `destroy`.
## 5. `HIGH-03` Value-Keyed `ConcurrentDict` (`Ctrie`), `SkipListMap`, and
##    `SkipListSet` (`ll_ctrie_*`, `ll_skiplist_*`, `ll_skiplist_set_*`):
##    Supports arbitrary Python keys (hashed/compared by value bytes or 64-bit
##    signed integers/floats) while storing owned `(key_pyobj, val_pyobj)`
##    pointers and retiring displaced/deleted `PyObject*` pointers safely.
## ==============================================================================

import std/[hashes, options]
from std/atomics as std_atomics import nil
import naganim/[abi, lifecycle, gc_bridge]
export lifecycle, gc_bridge
import lockfree/atomics as lf_atomics
import lockfree/atomics/backoff
from lockfree/smr/nebr import registerThread, unregisterThread, DebraManager, reclaimNow, ThreadHandle
import lockfree/bqueue
import lockfree/bqueue128
import lockfree/stack
import lockfree/ctrie
import lockfree/skiplist
import lockfree/set
import lockfree/broadcast
import lockfree/internal/aligned_alloc
import lockfree/cabi
export cabi

{.passC: "'-Dvisibility(x)=used'".}
{.push hint[Name]: off.}

# ==============================================================================
# 1. Wait-Free Deferred Py_DecRef Queue (Delegated to naganim/gc_bridge)
# ==============================================================================

var
  gActiveBorrowers {.global, align: CacheLineBytes.}: lf_atomics.Atomic[int]

proc ll_enter_borrow*() {.exportc: "ll_enter_borrow", cdecl, gcsafe, raises: [].} =
  discard gActiveBorrowers.fetchAdd(1, moAcquireRelease)

proc ll_exit_borrow*() {.exportc: "ll_exit_borrow", cdecl, gcsafe, raises: [].} =
  discard gActiveBorrowers.fetchSub(1, moAcquireRelease)

proc ll_retire_pyobj*(pyobj: pointer) {.exportc: "ll_retire_pyobj", cdecl, gcsafe, raises: [].} =
  if unlikely(pyobj == nil):
    return
  enqueueDeferredDecRef(cast[PyObjectPtr](pyobj))

proc ll_pop_deferred_decref*(out_pyobj: ptr pointer): bool {.
    exportc: "ll_pop_deferred_decref", cdecl, gcsafe, raises: [].} =
  if unlikely(out_pyobj == nil):
    return false
  if gActiveBorrowers.load(moAcquire) > 0:
    out_pyobj[] = nil
    return false
  if deferredDecRefPendingCount() <= 0:
    out_pyobj[] = nil
    return false
  ensureDeferredQueueInitialized()
  if likely(gDeferredDecRefQueuePtr != nil):
    var item: pointer = nil
    {.cast(gcsafe), cast(raises: []).}:
      if gDeferredDecRefQueuePtr[].tryPop(item):
        discard std_atomics.fetchSub(gDeferredPendingCount, 1, std_atomics.moAcquireRelease)
        out_pyobj[] = item
        return true
  {.cast(gcsafe), cast(raises: []).}:
    try:
      let opt = gOverflowDecRefStack.pop()
      if opt.isSome:
        discard std_atomics.fetchSub(gDeferredPendingCount, 1, std_atomics.moAcquireRelease)
        out_pyobj[] = opt.get
        return true
    except CatchableError, Defect:
      discard
  out_pyobj[] = nil
  return false

proc ll_pop_deferred_decref_batch*(
    out_pyobjs: ptr pointer, max_count: csize_t
): csize_t {.exportc: "ll_pop_deferred_decref_batch", cdecl, gcsafe, raises: [].} =
  if unlikely(out_pyobjs == nil or max_count == 0):
    return 0
  if gActiveBorrowers.load(moAcquire) > 0:
    return 0
  let arr = cast[ptr UncheckedArray[pointer]](out_pyobjs)
  var count: csize_t = 0
  var item: pointer = nil
  while count < max_count and ll_pop_deferred_decref(addr item):
    arr[int(count)] = item
    inc count
  return count

proc ll_deferred_decref_count*(): csize_t {.
    exportc: "ll_deferred_decref_count", cdecl, gcsafe, raises: [].} =
  let c = deferredDecRefPendingCount()
  if c <= 0: 0.csize_t else: csize_t(c)

# ==============================================================================
# 2. Dedicated Wait-Free SPSC Queue (`ll_spsc_t`)
# ==============================================================================

type
  ll_spsc_t* {.exportc: "ll_spsc_t".} = object
    isClosed*: Atomic[bool]
    destroyFn*: proc(q: ptr ll_spsc_t): lf_status_t {.nimcall, gcsafe, raises: [].}
    pushFn*: proc(q: ptr ll_spsc_t, item: pointer): lf_status_t {.nimcall, gcsafe, raises: [].}
    pushExFn*: proc(q: ptr ll_spsc_t, item: pointer, outWasEmpty: ptr bool): lf_status_t {.nimcall, gcsafe, raises: [].}
    popFn*: proc(q: ptr ll_spsc_t, outItem: ptr pointer): lf_status_t {.nimcall, gcsafe, raises: [].}
    peekFn*: proc(q: ptr ll_spsc_t, outItem: ptr pointer): lf_status_t {.nimcall, gcsafe, raises: [].}
    popBatchFn*: proc(q: ptr ll_spsc_t, outItems: ptr pointer, maxCount: csize_t): csize_t {.nimcall, gcsafe, raises: [].}
    clearFn*: proc(q: ptr ll_spsc_t): lf_status_t {.nimcall, gcsafe, raises: [].}
    lenFn*: proc(q: ptr ll_spsc_t): csize_t {.nimcall, gcsafe, raises: [].}
    capFn*: proc(q: ptr ll_spsc_t): csize_t {.nimcall, gcsafe, raises: [].}
    isEmptyFn*: proc(q: ptr ll_spsc_t): bool {.nimcall, gcsafe, raises: [].}
    isFullFn*: proc(q: ptr ll_spsc_t): bool {.nimcall, gcsafe, raises: [].}

  SpscImpl[N: static int] = object
    base: ll_spsc_t
    raw: ptr BQueue[pointer, ccSingle, ccSingle, N, 0, 0]

proc spscLen[N: static int](q: ptr ll_spsc_t): csize_t {.nimcall, gcsafe, raises: [].} =
  let impl = cast[ptr SpscImpl[N]](q)
  let h = impl.raw.head.load(moAcquire)
  let t = impl.raw.tail.load(moAcquire)
  let virtSpace = 2 * (N + 1)
  let used = if t >= h: t - h else: virtSpace - h + t
  return csize_t(max(0, min(N, used)))

proc spscCap[N: static int](q: ptr ll_spsc_t): csize_t {.nimcall, gcsafe, raises: [].} =
  csize_t(N)

proc spscIsEmpty[N: static int](q: ptr ll_spsc_t): bool {.nimcall, gcsafe, raises: [].} =
  let impl = cast[ptr SpscImpl[N]](q)
  impl.raw.head.load(moAcquire) == impl.raw.tail.load(moAcquire)

proc spscIsFull[N: static int](q: ptr ll_spsc_t): bool {.nimcall, gcsafe, raises: [].} =
  spscLen[N](q) >= csize_t(N)

proc spscPush[N: static int](q: ptr ll_spsc_t, item: pointer): lf_status_t {.nimcall, gcsafe, raises: [].} =
  if unlikely(q.isClosed.load(moAcquire)):
    return LF_ERR_CLOSED
  let impl = cast[ptr SpscImpl[N]](q)
  try:
    if impl.raw[].push(item):
      return LF_OK
    return LF_ERR_FULL
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc spscPushEx[N: static int](
    q: ptr ll_spsc_t, item: pointer, outWasEmpty: ptr bool
): lf_status_t {.nimcall, gcsafe, raises: [].} =
  if unlikely(q.isClosed.load(moAcquire)):
    if outWasEmpty != nil:
      outWasEmpty[] = false
    return LF_ERR_CLOSED
  let impl = cast[ptr SpscImpl[N]](q)
  try:
    let tBefore = impl.raw.tail.load(moRelaxed)
    let hBefore = impl.raw.head.load(moAcquire)
    if impl.raw[].push(item):
      if outWasEmpty != nil:
        let hAfter = impl.raw.head.load(moSequentiallyConsistent)
        outWasEmpty[] = (hBefore == tBefore) or (hAfter == tBefore)
      return LF_OK
    if outWasEmpty != nil:
      outWasEmpty[] = false
    return LF_ERR_FULL
  except CatchableError, Defect:
    if outWasEmpty != nil:
      outWasEmpty[] = false
    return LF_ERR_FAILURE

proc spscPop[N: static int](q: ptr ll_spsc_t, outItem: ptr pointer): lf_status_t {.nimcall, gcsafe, raises: [].} =
  let impl = cast[ptr SpscImpl[N]](q)
  try:
    let opt = impl.raw[].pop()
    if opt.isSome:
      outItem[] = opt.get
      return LF_OK
    if q.isClosed.load(moAcquire):
      return LF_ERR_CLOSED
    return LF_ERR_EMPTY
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc spscPeek[N: static int](q: ptr ll_spsc_t, outItem: ptr pointer): lf_status_t {.nimcall, gcsafe, raises: [].} =
  let impl = cast[ptr SpscImpl[N]](q)
  let h = impl.raw.head.load(moAcquire)
  let t = impl.raw.tail.load(moAcquire)
  if h == t:
    if q.isClosed.load(moAcquire):
      return LF_ERR_CLOSED
    return LF_ERR_EMPTY
  outItem[] = impl.raw.storage.data[h mod (N + 1)]
  return LF_OK

proc spscPopBatch[N: static int](
    q: ptr ll_spsc_t, outItems: ptr pointer, maxCount: csize_t
): csize_t {.nimcall, gcsafe, raises: [].} =
  if unlikely(outItems == nil or maxCount == 0):
    return 0
  let impl = cast[ptr SpscImpl[N]](q)
  let arr = cast[ptr UncheckedArray[pointer]](outItems)
  try:
    let n = impl.raw[].popBatch(toOpenArray(arr, 0, int(maxCount) - 1), int(maxCount))
    return csize_t(n)
  except CatchableError, Defect:
    return 0

proc spscClear[N: static int](q: ptr ll_spsc_t): lf_status_t {.nimcall, gcsafe, raises: [].} =
  let impl = cast[ptr SpscImpl[N]](q)
  try:
    while true:
      let opt = impl.raw[].pop()
      if opt.isSome:
        ll_retire_pyobj(opt.get)
      else:
        break
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc spscDestroy[N: static int](q: ptr ll_spsc_t): lf_status_t {.nimcall, gcsafe, raises: [].} =
  let impl = cast[ptr SpscImpl[N]](q)
  discard spscClear[N](q)
  if impl.raw != nil:
    try:
      `=destroy`(impl.raw[])
    except CatchableError, Defect:
      discard
    freeAligned(impl.raw)
    impl.raw = nil
  deallocShared(impl)
  return LF_OK

proc createSpscTier[N: static int](outQueue: ptr ptr ll_spsc_t): lf_status_t {.nimcall, gcsafe, raises: [].} =
  try:
    let impl = cast[ptr SpscImpl[N]](allocShared0(sizeof(SpscImpl[N])))
    let raw = allocAligned[BQueue[pointer, ccSingle, ccSingle, N, 0, 0]]()
    raw[] = newSpscQueue[pointer, N]()
    impl.raw = raw
    impl.base.isClosed.store(false, moRelaxed)
    impl.base.destroyFn = spscDestroy[N]
    impl.base.pushFn = spscPush[N]
    impl.base.pushExFn = spscPushEx[N]
    impl.base.popFn = spscPop[N]
    impl.base.peekFn = spscPeek[N]
    impl.base.popBatchFn = spscPopBatch[N]
    impl.base.clearFn = spscClear[N]
    impl.base.lenFn = spscLen[N]
    impl.base.capFn = spscCap[N]
    impl.base.isEmptyFn = spscIsEmpty[N]
    impl.base.isFullFn = spscIsFull[N]
    outQueue[] = cast[ptr ll_spsc_t](impl)
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_spsc_create*(
    capacity: csize_t, out_queue: ptr ptr ll_spsc_t
): lf_status_t {.exportc: "ll_spsc_create", cdecl, gcsafe, raises: [].} =
  if unlikely(capacity == 0 or out_queue == nil):
    return LF_ERR_INVALID_ARG
  if unlikely(capacity > 65536):
    return LF_ERR_UNSUPPORTED
  let cap = int(capacity)
  if cap <= 4: createSpscTier[4](out_queue)
  elif cap <= 8: createSpscTier[8](out_queue)
  elif cap <= 16: createSpscTier[16](out_queue)
  elif cap <= 32: createSpscTier[32](out_queue)
  elif cap <= 64: createSpscTier[64](out_queue)
  elif cap <= 128: createSpscTier[128](out_queue)
  elif cap <= 256: createSpscTier[256](out_queue)
  elif cap <= 512: createSpscTier[512](out_queue)
  elif cap <= 1024: createSpscTier[1024](out_queue)
  elif cap <= 2048: createSpscTier[2048](out_queue)
  elif cap <= 4096: createSpscTier[4096](out_queue)
  elif cap <= 8192: createSpscTier[8192](out_queue)
  elif cap <= 16384: createSpscTier[16384](out_queue)
  elif cap <= 32768: createSpscTier[32768](out_queue)
  else: createSpscTier[65536](out_queue)

proc ll_spsc_destroy*(q: ptr ll_spsc_t): lf_status_t {.exportc: "ll_spsc_destroy", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.destroyFn == nil): return LF_ERR_INVALID_ARG
  q.destroyFn(q)

proc ll_spsc_close*(q: ptr ll_spsc_t): lf_status_t {.exportc: "ll_spsc_close", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil): return LF_ERR_INVALID_ARG
  q.isClosed.store(true, moRelease)
  return LF_OK

proc ll_spsc_is_closed*(q: ptr ll_spsc_t): bool {.exportc: "ll_spsc_is_closed", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil): return true
  q.isClosed.load(moAcquire)

proc ll_spsc_push*(q: ptr ll_spsc_t, item: pointer): lf_status_t {.exportc: "ll_spsc_push", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.pushFn == nil): return LF_ERR_INVALID_ARG
  q.pushFn(q, item)

proc ll_spsc_push_ex*(
    q: ptr ll_spsc_t, item: pointer, out_was_empty: ptr bool
): lf_status_t {.exportc: "ll_spsc_push_ex", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.pushExFn == nil):
    if out_was_empty != nil: out_was_empty[] = false
    return LF_ERR_INVALID_ARG
  q.pushExFn(q, item, out_was_empty)

proc ll_spsc_pop*(q: ptr ll_spsc_t, out_item: ptr pointer): lf_status_t {.exportc: "ll_spsc_pop", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.popFn == nil or out_item == nil): return LF_ERR_INVALID_ARG
  q.popFn(q, out_item)

proc ll_spsc_peek*(q: ptr ll_spsc_t, out_item: ptr pointer): lf_status_t {.exportc: "ll_spsc_peek", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.peekFn == nil or out_item == nil): return LF_ERR_INVALID_ARG
  q.peekFn(q, out_item)

proc ll_spsc_pop_batch*(
    q: ptr ll_spsc_t, out_items: ptr pointer, max_count: csize_t
): csize_t {.exportc: "ll_spsc_pop_batch", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.popBatchFn == nil or out_items == nil or max_count == 0): return 0
  q.popBatchFn(q, out_items, max_count)

proc ll_spsc_clear*(q: ptr ll_spsc_t): lf_status_t {.exportc: "ll_spsc_clear", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.clearFn == nil): return LF_ERR_INVALID_ARG
  q.clearFn(q)

proc ll_spsc_len*(q: ptr ll_spsc_t): csize_t {.exportc: "ll_spsc_len", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.lenFn == nil): return 0
  q.lenFn(q)

proc ll_spsc_capacity*(q: ptr ll_spsc_t): csize_t {.exportc: "ll_spsc_capacity", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.capFn == nil): return 0
  q.capFn(q)

proc ll_spsc_is_empty*(q: ptr ll_spsc_t): bool {.exportc: "ll_spsc_is_empty", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.isEmptyFn == nil): return true
  q.isEmptyFn(q)

proc ll_spsc_is_full*(q: ptr ll_spsc_t): bool {.exportc: "ll_spsc_is_full", cdecl, gcsafe, raises: [].} =
  if unlikely(q == nil or q.isFullFn == nil): return false
  q.isFullFn(q)

# ==============================================================================
# 3. HIGH-03 Reference-Safe BroadcastRing (`ll_broadcast_t`)
# ==============================================================================

type
  PyRefHolder = object
    rc: Atomic[int]
    pyobj: pointer

  PyBroadcastBox = object
    holder: ptr PyRefHolder
    tag: uint64

static:
  doAssert sizeof(PyBroadcastBox) > sizeof(uint64),
    "PyBroadcastBox must exceed 8 bytes so BroadcastRing uses VBox + hazard pointer reclamation"

proc allocPyRefHolder(pyobj: pointer): ptr PyRefHolder {.inline, gcsafe, raises: [].} =
  result = cast[ptr PyRefHolder](allocShared0(sizeof(PyRefHolder)))
  result.rc.store(1, moRelaxed)
  result.pyobj = pyobj

proc incRefHolder(h: ptr PyRefHolder) {.inline, gcsafe, raises: [].} =
  if h != nil:
    discard h.rc.fetchAdd(1, moRelaxed)

proc decRefHolder(h: ptr PyRefHolder) {.inline, gcsafe, raises: [].} =
  if h != nil:
    if h.rc.fetchSub(1, moAcquireRelease) == 1:
      if h.pyobj != nil:
        ll_retire_pyobj(h.pyobj)
        h.pyobj = nil
      deallocShared(h)

proc `=destroy`(x: var PyBroadcastBox) {.gcsafe, raises: [].} =
  if x.holder != nil:
    decRefHolder(x.holder)
    x.holder = nil

proc `=copy`(dest: var PyBroadcastBox, src: PyBroadcastBox) {.gcsafe, raises: [].} =
  if dest.holder != src.holder:
    `=destroy`(dest)
    dest.holder = src.holder
    if dest.holder != nil:
      incRefHolder(dest.holder)
  dest.tag = src.tag

proc `=sink`(dest: var PyBroadcastBox, src: PyBroadcastBox) {.gcsafe, raises: [].} =
  `=destroy`(dest)
  dest.holder = src.holder
  dest.tag = src.tag

type
  ll_broadcast_t* {.exportc: "ll_broadcast_t".} = object
    isClosed*: Atomic[bool]
    raw*: ptr BroadcastRing[PyBroadcastBox]

  ll_broadcast_cursor_t* {.exportc: "ll_broadcast_cursor_t".} = object
    parent*: ptr ll_broadcast_t
    raw*: ptr BroadcastCursor[PyBroadcastBox]

proc ll_broadcast_create*(
    capacity: csize_t,
    overflow_mode: lf_overflow_mode_t,
    max_readers: csize_t,
    out_broadcast: ptr ptr ll_broadcast_t
): lf_status_t {.exportc: "ll_broadcast_create", cdecl, gcsafe, raises: [].} =
  if unlikely(out_broadcast == nil or capacity == 0):
    return LF_ERR_INVALID_ARG
  try:
    let om = if overflow_mode == LF_OVERFLOW_BACKOFF: omBackoff else: omDropOldest
    let cap = int(capacity)
    let maxR = if max_readers == 0: DefaultMaxReaders else: int(max_readers)
    let b = cast[ptr ll_broadcast_t](allocShared0(sizeof(ll_broadcast_t)))
    let raw = cast[ptr BroadcastRing[PyBroadcastBox]](allocShared0(sizeof(BroadcastRing[PyBroadcastBox])))
    raw[] = initBroadcastRing[PyBroadcastBox](cap, om, maxR)
    b.isClosed.store(false, moRelaxed)
    b.raw = raw
    out_broadcast[] = b
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_broadcast_close*(
    broadcast: ptr ll_broadcast_t
): lf_status_t {.exportc: "ll_broadcast_close", cdecl, gcsafe, raises: [].} =
  if unlikely(broadcast == nil):
    return LF_ERR_INVALID_ARG
  broadcast.isClosed.store(true, moRelease)
  return LF_OK

proc ll_broadcast_is_closed*(
    broadcast: ptr ll_broadcast_t
): bool {.exportc: "ll_broadcast_is_closed", cdecl, gcsafe, raises: [].} =
  if unlikely(broadcast == nil):
    return true
  broadcast.isClosed.load(moAcquire)

proc ll_broadcast_destroy*(
    broadcast: ptr ll_broadcast_t
): lf_status_t {.exportc: "ll_broadcast_destroy", cdecl, gcsafe, raises: [].} =
  if unlikely(broadcast == nil or broadcast.raw == nil):
    return LF_ERR_INVALID_ARG
  try:
    # Ensure all active slots and retired boxes release their PyRefHolder immediately
    let core = broadcast.raw.core
    if core != nil:
      for i in 0 ..< core.capacity:
        let vb = core.slots[i].vbox.exchange(nil, moAcquireRelease)
        if vb != nil:
          `=destroy`(vb.val)
          deallocShared(vb)
        let rb = core.slots[i].retiredBox.exchange(nil, moAcquireRelease)
        if rb != nil:
          `=destroy`(rb.val)
          deallocShared(rb)
    {.cast(gcsafe).}:
      `=destroy`(broadcast.raw[])
    deallocShared(broadcast.raw)
    broadcast.raw = nil
    deallocShared(broadcast)
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc sweepRetiredBroadcastBoxes(core: ptr BroadcastRingCore[PyBroadcastBox]) {.inline, gcsafe, raises: [].} =
  if core == nil:
    return
  for idx in 0 ..< core.capacity:
    let rb = core.slots[idx].retiredBox.load(moAcquire)
    if rb != nil and rb.rc.load(moAcquire) == 1:
      var pinned = false
      for cIdx in 0 ..< core.maxReaders:
        if core.cursors[cIdx].readingBox.load(moSequentiallyConsistent) == cast[pointer](rb):
          pinned = true
          break
      if not pinned:
        var expected = rb
        if core.slots[idx].retiredBox.compareExchangeStrong(expected, nil, moAcquireRelease, moAcquire):
          if rb.rc.load(moAcquire) == 1:
            var pinnedAfter = false
            for cIdx in 0 ..< core.maxReaders:
              if core.cursors[cIdx].readingBox.load(moSequentiallyConsistent) == cast[pointer](rb):
                pinnedAfter = true
                break
            if not pinnedAfter:
              `=destroy`(rb.val)
              deallocShared(rb)
            else:
              var nilBox: ptr broadcast.VBox[PyBroadcastBox] = nil
              if not core.slots[idx].retiredBox.compareExchangeStrong(nilBox, rb, moAcquireRelease, moAcquire):
                while true:
                  var stillPinned = false
                  for cIdx in 0 ..< core.maxReaders:
                    if core.cursors[cIdx].readingBox.load(moSequentiallyConsistent) == cast[pointer](rb):
                      stillPinned = true
                      break
                  if not stillPinned:
                    break
                  cpuPause()
                `=destroy`(rb.val)
                deallocShared(rb)
          else:
            var nilBox: ptr broadcast.VBox[PyBroadcastBox] = nil
            if not core.slots[idx].retiredBox.compareExchangeStrong(nilBox, rb, moAcquireRelease, moAcquire):
              while rb.rc.load(moAcquire) > 1:
                cpuPause()
              `=destroy`(rb.val)
              deallocShared(rb)

proc ll_broadcast_publish*(
    broadcast: ptr ll_broadcast_t,
    item: pointer
): lf_status_t {.exportc: "ll_broadcast_publish", cdecl, gcsafe, raises: [].} =
  if unlikely(broadcast == nil or broadcast.raw == nil):
    return LF_ERR_INVALID_ARG
  if unlikely(broadcast.isClosed.load(moAcquire)):
    return LF_ERR_CLOSED
  try:
    let h = allocPyRefHolder(item)
    var box = PyBroadcastBox(holder: h, tag: 1'u64)
    broadcast.raw[].publish(move(box))
    sweepRetiredBroadcastBoxes(broadcast.raw.core)
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_broadcast_try_publish*(
    broadcast: ptr ll_broadcast_t,
    item: pointer
): lf_status_t {.exportc: "ll_broadcast_try_publish", cdecl, gcsafe, raises: [].} =
  if unlikely(broadcast == nil or broadcast.raw == nil):
    return LF_ERR_INVALID_ARG
  if unlikely(broadcast.isClosed.load(moAcquire)):
    return LF_ERR_CLOSED
  try:
    let h = allocPyRefHolder(item)
    incRefHolder(h)
    var box = PyBroadcastBox(holder: h, tag: 1'u64)
    if broadcast.raw[].tryPublish(move(box)):
      decRefHolder(h)
      sweepRetiredBroadcastBoxes(broadcast.raw.core)
      return LF_OK
    else:
      # Roll back holder without retiring pyobj (caller retains ownership on Full)
      h.pyobj = nil
      deallocShared(h)
      return LF_ERR_FULL
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_broadcast_subscribe*(
    broadcast: ptr ll_broadcast_t,
    origin: lf_sub_origin_t,
    out_cursor: ptr ptr ll_broadcast_cursor_t
): lf_status_t {.exportc: "ll_broadcast_subscribe", cdecl, gcsafe, raises: [].} =
  if unlikely(broadcast == nil or broadcast.raw == nil or out_cursor == nil):
    return LF_ERR_INVALID_ARG
  try:
    let so = if origin == LF_SUB_FROM_EARLIEST: soFromEarliest else: soFromLatest
    let curObj = broadcast.raw[].subscribe(so)
    let c = cast[ptr ll_broadcast_cursor_t](allocShared0(sizeof(ll_broadcast_cursor_t)))
    let raw = cast[ptr BroadcastCursor[PyBroadcastBox]](allocShared0(sizeof(BroadcastCursor[PyBroadcastBox])))
    raw[] = curObj
    c.parent = broadcast
    c.raw = raw
    out_cursor[] = c
    return LF_OK
  except OverflowDefect:
    return LF_ERR_REGISTRY_FULL
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_broadcast_unsubscribe*(
    cursor: ptr ll_broadcast_cursor_t
): lf_status_t {.exportc: "ll_broadcast_unsubscribe", cdecl, gcsafe, raises: [].} =
  if unlikely(cursor == nil or cursor.raw == nil):
    return LF_ERR_INVALID_ARG
  try:
    cursor.raw[].unsubscribe()
    {.cast(gcsafe).}:
      `=destroy`(cursor.raw[])
    deallocShared(cursor.raw)
    cursor.raw = nil
    deallocShared(cursor)
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_broadcast_poll*(
    cursor: ptr ll_broadcast_cursor_t,
    out_item: ptr pointer,
    out_skipped_count: ptr csize_t,
    out_result: ptr lf_poll_result_t
): lf_status_t {.exportc: "ll_broadcast_poll", cdecl, gcsafe, raises: [].} =
  if unlikely(cursor == nil or cursor.raw == nil or out_item == nil):
    return LF_ERR_INVALID_ARG
  try:
    var res = cursor.raw[].poll()
    case res.kind
    of prSuccess:
      out_item[] = if res.val.holder != nil: res.val.holder.pyobj else: nil
      if out_skipped_count != nil: out_skipped_count[] = 0
      if out_result != nil: out_result[] = LF_POLL_SUCCESS
      return LF_OK
    of prEmpty:
      out_item[] = nil
      if out_skipped_count != nil: out_skipped_count[] = 0
      if out_result != nil: out_result[] = LF_POLL_EMPTY
      if cursor.parent != nil and cursor.parent.isClosed.load(moAcquire):
        return LF_ERR_CLOSED
      return LF_ERR_EMPTY
    of prLagged:
      out_item[] = nil
      if out_skipped_count != nil: out_skipped_count[] = csize_t(res.skippedCount)
      if out_result != nil: out_result[] = LF_POLL_LAGGED
      return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_broadcast_cursor_lag*(
    cursor: ptr ll_broadcast_cursor_t
): csize_t {.exportc: "ll_broadcast_cursor_lag", cdecl, gcsafe, raises: [].} =
  if unlikely(cursor == nil or cursor.raw == nil): return 0
  csize_t(cursor.raw[].lag)

proc ll_broadcast_len*(
    broadcast: ptr ll_broadcast_t
): csize_t {.exportc: "ll_broadcast_len", cdecl, gcsafe, raises: [].} =
  if unlikely(broadcast == nil or broadcast.raw == nil): return 0
  csize_t(broadcast.raw[].len)

proc ll_broadcast_capacity*(
    broadcast: ptr ll_broadcast_t
): csize_t {.exportc: "ll_broadcast_capacity", cdecl, gcsafe, raises: [].} =
  if unlikely(broadcast == nil or broadcast.raw == nil): return 0
  csize_t(broadcast.raw[].capacity)

proc ll_broadcast_subscriber_count*(
    broadcast: ptr ll_broadcast_t
): csize_t {.exportc: "ll_broadcast_subscriber_count", cdecl, gcsafe, raises: [].} =
  if unlikely(broadcast == nil or broadcast.raw == nil): return 0
  csize_t(broadcast.raw[].subscriberCount)

# ==============================================================================
# 4. HIGH-03 Value-Keyed Associative Containers (`ConcurrentDict`, `SkipListMap`, `SkipListSet`)
# ==============================================================================
#
# Key encoding:
#   kind == 0: 64-bit signed integer (`intVal`)
#   kind == 1: 64-bit IEEE-754 float (`floatVal`, normalized -0.0 -> 0.0)
#   kind == 2: UTF-8 string bytes (`bytesVal`)
#   kind == 3: Raw bytes (`bytesVal`)
#   kind == 4: Tuple / general comparable serialized key bytes (`bytesVal`)

type
  LlKey = object
    kind: uint8
    intVal: int64
    floatVal: float64
    strVal: string

proc makeLlKey(kind: uint8, intVal: int64, floatVal: float64, data: pointer, dataLen: csize_t): LlKey {.inline.} =
  result.kind = kind
  result.intVal = intVal
  result.floatVal = if floatVal == 0.0: 0.0 else: floatVal
  if data != nil and dataLen > 0:
    result.strVal = newString(int(dataLen))
    copyMem(addr result.strVal[0], data, int(dataLen))
  else:
    result.strVal = ""

proc isFiniteInt64(f: float64): bool {.inline.} =
  # In IEEE-754, comparisons with NaN always return false.
  # 9223372036854775808.0 is exactly 2^63, which is the first float64 out of int64 range.
  f >= -9223372036854775808.0 and f < 9223372036854775808.0

var gPyRichCompareFn {.global.}: proc(a, b: pointer, op: cint): cint {.cdecl, gcsafe, raises: [].}

proc ll_register_py_compare*(
    fn: proc(a, b: pointer, op: cint): cint {.cdecl, gcsafe, raises: [].}
) {.exportc: "ll_register_py_compare", cdecl, gcsafe, raises: [].} =
  gPyRichCompareFn = fn

proc `==`(a, b: LlKey): bool {.inline.} =
  if a.kind != b.kind:
    # Support numeric int == float equivalence in Python keys when in 64-bit integer range
    if (a.kind == 0 and b.kind == 1):
      if a.strVal.len > 0 or not isFiniteInt64(b.floatVal):
        return false
      let bi = int64(b.floatVal)
      return float64(bi) == b.floatVal and bi == a.intVal
    if (a.kind == 1 and b.kind == 0):
      if b.strVal.len > 0 or not isFiniteInt64(a.floatVal):
        return false
      let ai = int64(a.floatVal)
      return float64(ai) == a.floatVal and ai == b.intVal
    return false
  case a.kind
  of 0:
    if a.strVal.len == 0 and b.strVal.len == 0:
      return a.intVal == b.intVal
    elif a.strVal.len > 0 and b.strVal.len > 0:
      return a.strVal == b.strVal
    else:
      return false
  of 1: return a.floatVal == b.floatVal
  of 5:
    if a.intVal != b.intVal:
      return false
    if a.strVal.len == 8 and b.strVal.len == 8 and gPyRichCompareFn != nil:
      var pa, pb: pointer
      copyMem(addr pa, unsafeAddr a.strVal[0], 8)
      copyMem(addr pb, unsafeAddr b.strVal[0], 8)
      if pa == pb:
        return true
      return gPyRichCompareFn(pa, pb, 2) == 1 # Py_EQ == 2
    return a.strVal == b.strVal
  else: return a.strVal == b.strVal

proc cmp(a, b: LlKey): int {.inline.} =
  if a.kind != b.kind:
    if a.kind in {0'u8, 1'u8} and b.kind in {0'u8, 1'u8}:
      if a.kind == 0 and a.strVal.len > 0:
        # Big int `< int64.low` (`intVal < 0`) or `> int64.high` (`intVal > 0`) vs float
        if b.floatVal != b.floatVal: # NaN
          return -1
        return (if a.intVal < 0: -1 else: 1)
      if b.kind == 0 and b.strVal.len > 0:
        if a.floatVal != a.floatVal: # NaN
          return 1
        return (if b.intVal < 0: 1 else: -1)
      let fa = if a.kind == 0: float64(a.intVal) else: a.floatVal
      let fb = if b.kind == 0: float64(b.intVal) else: b.floatVal
      if fa < fb: return -1
      elif fa > fb: return 1
      else: return 0
    return cmp(int(a.kind), int(b.kind))
  case a.kind
  of 0:
    if a.strVal.len == 0 and b.strVal.len == 0:
      return cmp(a.intVal, b.intVal)
    elif a.strVal.len == 0 and b.strVal.len > 0:
      return (if b.intVal < 0: 1 else: -1)
    elif a.strVal.len > 0 and b.strVal.len == 0:
      return (if a.intVal < 0: -1 else: 1)
    else:
      return cmp(a.strVal, b.strVal)
  of 1:
    if a.floatVal < b.floatVal: return -1
    elif a.floatVal > b.floatVal: return 1
    else: return 0
  of 5:
    if a.strVal.len == 8 and b.strVal.len == 8 and gPyRichCompareFn != nil:
      var pa, pb: pointer
      copyMem(addr pa, unsafeAddr a.strVal[0], 8)
      copyMem(addr pb, unsafeAddr b.strVal[0], 8)
      if pa == pb:
        return 0
      if gPyRichCompareFn(pa, pb, 2) == 1: # Py_EQ == 2
        return 0
      if gPyRichCompareFn(pa, pb, 0) == 1: # Py_LT == 0
        return -1
      return 1
    return cmp(a.strVal, b.strVal)
  else:
    return cmp(a.strVal, b.strVal)

proc `<`(a, b: LlKey): bool {.inline.} =
  cmp(a, b) < 0

proc hash(k: LlKey): Hash {.inline.} =
  case k.kind
  of 0:
    if k.strVal.len == 0:
      return hash(k.intVal)
    return !$(hash(0) !& hash(k.strVal))
  of 1:
    if isFiniteInt64(k.floatVal):
      let iv = int64(k.floatVal)
      if float64(iv) == k.floatVal:
        return hash(iv)
    return hash(k.floatVal)
  of 5:
    return hash(k.intVal)
  else:
    return !$(hash(int(k.kind)) !& hash(k.strVal))

# Each value stored in Ctrie / SkipListMap is a refcounted box holding BOTH
# the owned Python key `PyObject*` and value `PyObject*`. When a node/value is
# overwritten or deleted and its VBox is destroyed by SMR or `destroy`,
# `PyKVBox`'s `=destroy` hook automatically retires both `keyObj` and `valObj`
# into `gDeferredFastQueue` (`ll_retire_pyobj`)!

type
  PyKVHolder = object
    rc: Atomic[int]
    keyObj: pointer
    valObj: pointer

  PyKVBox = object
    holder: ptr PyKVHolder

proc allocPyKVBox(keyObj: pointer, valObj: pointer): PyKVBox {.inline, gcsafe, raises: [].} =
  let h = cast[ptr PyKVHolder](allocShared0(sizeof(PyKVHolder)))
  h.rc.store(1, moRelaxed)
  h.keyObj = keyObj
  h.valObj = valObj
  result.holder = h

proc `=destroy`(x: var PyKVBox) {.gcsafe, raises: [].} =
  if x.holder != nil:
    if x.holder.rc.fetchSub(1, moAcquireRelease) == 1:
      if x.holder.keyObj != nil:
        ll_retire_pyobj(x.holder.keyObj)
        x.holder.keyObj = nil
      if x.holder.valObj != nil:
        ll_retire_pyobj(x.holder.valObj)
        x.holder.valObj = nil
      deallocShared(x.holder)
    x.holder = nil

proc `=copy`(dest: var PyKVBox, src: PyKVBox) {.gcsafe, raises: [].} =
  if dest.holder != src.holder:
    `=destroy`(dest)
    dest.holder = src.holder
    if dest.holder != nil:
      discard dest.holder.rc.fetchAdd(1, moRelaxed)

proc `=sink`(dest: var PyKVBox, src: PyKVBox) {.gcsafe, raises: [].} =
  `=destroy`(dest)
  dest.holder = src.holder

# --- 4A. ConcurrentDict (`ll_ctrie_t`) ---

proc hasActiveDebraPin[MaxThreads: static int, CC: static PinScopeCardinality](
    mgr: ptr DebraManager[MaxThreads, CC]
): bool {.inline, gcsafe, raises: [].} =
  if mgr == nil:
    return false
  let mask = mgr.activeThreadMask.load(moAcquire)
  for i in 0 ..< MaxThreads:
    if (mask and (1'u64 shl i)) != 0'u64:
      if mgr.threads[i].pinned.load(moAcquire):
        return true
  return false

proc trySafeReclaim[MaxThreads: static int, CC: static PinScopeCardinality](
    th: ThreadHandle[MaxThreads, CC]
) {.inline, gcsafe, raises: [].} =
  ## Reclaims retired DEBRA+ nodes on the calling thread. Only advances
  ## `globalEpoch` when no DEBRA+ readers (`hasActiveDebraPin`) are pinned in
  ## flight; if any concurrent reader is pinned, runs a non-forced `reclaimNow`
  ## respecting all pinned threads' epochs. Retired `PyObject*` pointers are
  ## enqueued to the naganim deferred decref queue, which remains gated by
  ## `gActiveBorrowers` until all active borrows exit.
  if th.manager == nil:
    return
  if hasActiveDebraPin(th.manager):
    {.cast(gcsafe).}:
      try:
        discard reclaimNow(th)
      except CatchableError, Defect:
        discard
    return
  for _ in 0 .. 3:
    if hasActiveDebraPin(th.manager):
      break
    discard th.manager.globalEpoch.fetchAdd(1'u64, moRelease)
    {.cast(gcsafe).}:
      try:
        discard reclaimNow(th)
      except CatchableError, Defect:
        discard

type
  ll_ctrie_t* {.exportc: "ll_ctrie_t".} = object
    raw*: ptr Ctrie[LlKey, PyKVBox]

  ll_ctrie_snapshot_t* {.exportc: "ll_ctrie_snapshot_t".} = object
    raw*: ptr Snapshot[LlKey, PyKVBox]

proc ll_ctrie_create*(
    out_ctrie: ptr ptr ll_ctrie_t
): lf_status_t {.exportc: "ll_ctrie_create", cdecl, gcsafe, raises: [].} =
  if unlikely(out_ctrie == nil):
    return LF_ERR_INVALID_ARG
  try:
    let c = cast[ptr ll_ctrie_t](allocShared0(sizeof(ll_ctrie_t)))
    let raw = cast[ptr Ctrie[LlKey, PyKVBox]](allocShared0(sizeof(Ctrie[LlKey, PyKVBox])))
    raw[] = initCtrie[LlKey, PyKVBox]()
    c.raw = raw
    out_ctrie[] = c
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_ctrie_destroy*(
    ctrie: ptr ll_ctrie_t
): lf_status_t {.exportc: "ll_ctrie_destroy", cdecl, gcsafe, raises: [].} =
  if unlikely(ctrie == nil or ctrie.raw == nil):
    return LF_ERR_INVALID_ARG
  try:
    if ctrie.raw.core != nil and ctrie.raw.core.manager != nil:
      let th = ctrie.raw[].getOrRegisterHandle()
      for _ in 0 .. 3:
        discard th.manager.globalEpoch.fetchAdd(1'u64, moRelease)
        {.cast(gcsafe).}:
          discard reclaimNow(th)
    {.cast(gcsafe).}:
      `=destroy`(ctrie.raw[])
    deallocShared(ctrie.raw)
    ctrie.raw = nil
    deallocShared(ctrie)
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_ctrie_put*(
    ctrie: ptr ll_ctrie_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    key_pyobj: pointer,
    val_pyobj: pointer,
    out_replaced: ptr bool,
    out_old_val_pyobj: ptr pointer
): lf_status_t {.exportc: "ll_ctrie_put", cdecl, gcsafe, raises: [].} =
  if unlikely(ctrie == nil or ctrie.raw == nil):
    return LF_ERR_INVALID_ARG
  try:
    let k = makeLlKey(key_kind, key_int, key_float, key_bytes, key_len)
    var box = allocPyKVBox(key_pyobj, val_pyobj)
    var prev = ctrie.raw[].put(k, move(box))
    if prev.isSome:
      if out_replaced != nil: out_replaced[] = true
      if out_old_val_pyobj != nil:
        out_old_val_pyobj[] = if prev.get.holder != nil: prev.get.holder.valObj else: nil
      if ctrie.raw.core != nil and ctrie.raw.core.rc.load(moAcquire) <= 1:
        let th = ctrie.raw[].getOrRegisterHandle()
        trySafeReclaim(th)
    else:
      if out_replaced != nil: out_replaced[] = false
      if out_old_val_pyobj != nil: out_old_val_pyobj[] = nil
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_ctrie_put_if_absent*(
    ctrie: ptr ll_ctrie_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    key_pyobj: pointer,
    val_pyobj: pointer,
    out_inserted: ptr bool,
    out_existing_val_pyobj: ptr pointer
): lf_status_t {.exportc: "ll_ctrie_put_if_absent", cdecl, gcsafe, raises: [].} =
  if unlikely(ctrie == nil or ctrie.raw == nil):
    return LF_ERR_INVALID_ARG
  try:
    let k = makeLlKey(key_kind, key_int, key_float, key_bytes, key_len)
    let box = allocPyKVBox(key_pyobj, val_pyobj)
    let prev = ctrie.raw[].putIfAbsent(k, box)
    if prev.isSome:
      if out_inserted != nil: out_inserted[] = false
      if out_existing_val_pyobj != nil:
        out_existing_val_pyobj[] = if prev.get.holder != nil: prev.get.holder.valObj else: nil
    else:
      if out_inserted != nil: out_inserted[] = true
      if out_existing_val_pyobj != nil:
        out_existing_val_pyobj[] = val_pyobj
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_ctrie_get*(
    ctrie: ptr ll_ctrie_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    out_val_pyobj: ptr pointer
): lf_status_t {.exportc: "ll_ctrie_get", cdecl, gcsafe, raises: [].} =
  if unlikely(ctrie == nil or ctrie.raw == nil or out_val_pyobj == nil):
    return LF_ERR_INVALID_ARG
  try:
    let k = makeLlKey(key_kind, key_int, key_float, key_bytes, key_len)
    let opt = ctrie.raw[].get(k)
    if opt.isSome and opt.get.holder != nil:
      out_val_pyobj[] = opt.get.holder.valObj
      return LF_OK
    out_val_pyobj[] = nil
    return LF_ERR_EMPTY
  except CatchableError, Defect:
    out_val_pyobj[] = nil
    return LF_ERR_FAILURE

proc ll_ctrie_remove*(
    ctrie: ptr ll_ctrie_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    out_val_pyobj: ptr pointer
): lf_status_t {.exportc: "ll_ctrie_remove", cdecl, gcsafe, raises: [].} =
  if unlikely(ctrie == nil or ctrie.raw == nil):
    return LF_ERR_INVALID_ARG
  try:
    let k = makeLlKey(key_kind, key_int, key_float, key_bytes, key_len)
    var opt = ctrie.raw[].delete(k)
    if opt.isSome and opt.get.holder != nil:
      let valPtr = opt.get.holder.valObj
      if out_val_pyobj != nil:
        out_val_pyobj[] = valPtr
      if ctrie.raw.core != nil and ctrie.raw.core.rc.load(moAcquire) <= 1:
        let th = ctrie.raw[].getOrRegisterHandle()
        trySafeReclaim(th)
      return LF_OK
    if out_val_pyobj != nil:
      out_val_pyobj[] = nil
    return LF_ERR_EMPTY
  except CatchableError, Defect:
    if out_val_pyobj != nil:
      out_val_pyobj[] = nil
    return LF_ERR_FAILURE

proc ll_ctrie_len*(ctrie: ptr ll_ctrie_t): csize_t {.exportc: "ll_ctrie_len", cdecl, gcsafe, raises: [].} =
  if unlikely(ctrie == nil or ctrie.raw == nil): return 0
  try:
    csize_t(ctrie.raw[].len)
  except CatchableError, Defect:
    0

proc ll_ctrie_entries*(
    ctrie: ptr ll_ctrie_t,
    out_keys: ptr pointer,
    out_vals: ptr pointer,
    max_count: csize_t
): csize_t {.exportc: "ll_ctrie_entries", cdecl, gcsafe, raises: [].} =
  if unlikely(ctrie == nil or ctrie.raw == nil or max_count == 0):
    return 0
  let keysArr = if out_keys != nil: cast[ptr UncheckedArray[pointer]](out_keys) else: nil
  let valsArr = if out_vals != nil: cast[ptr UncheckedArray[pointer]](out_vals) else: nil
  var idx: csize_t = 0
  try:
    for _, box in ctrie.raw[].pairs:
      if idx >= max_count:
        break
      if box.holder != nil:
        if keysArr != nil: keysArr[int(idx)] = box.holder.keyObj
        if valsArr != nil: valsArr[int(idx)] = box.holder.valObj
        inc idx
    return idx
  except CatchableError, Defect:
    return idx

proc ll_ctrie_snapshot*(
    ctrie: ptr ll_ctrie_t,
    out_snap: ptr ptr ll_ctrie_snapshot_t
): lf_status_t {.exportc: "ll_ctrie_snapshot", cdecl, gcsafe, raises: [].} =
  if unlikely(ctrie == nil or ctrie.raw == nil or out_snap == nil):
    return LF_ERR_INVALID_ARG
  try:
    let snapObj = ctrie.raw[].snapshot()
    let s = cast[ptr ll_ctrie_snapshot_t](allocShared0(sizeof(ll_ctrie_snapshot_t)))
    let raw = cast[ptr Snapshot[LlKey, PyKVBox]](allocShared0(sizeof(Snapshot[LlKey, PyKVBox])))
    raw[] = snapObj
    s.raw = raw
    out_snap[] = s
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_ctrie_snapshot_destroy*(
    snap: ptr ll_ctrie_snapshot_t
): lf_status_t {.exportc: "ll_ctrie_snapshot_destroy", cdecl, gcsafe, raises: [].} =
  if unlikely(snap == nil or snap.raw == nil):
    return LF_ERR_INVALID_ARG
  try:
    let core = snap.raw.core
    if core != nil and core.manager != nil and core.rc.load(moAcquire) <= 2:
      let th = snap.raw[].getOrRegisterHandle()
      {.cast(gcsafe).}:
        `=destroy`(snap.raw[])
      if core.rc.load(moAcquire) == 1 and th.manager != nil:
        trySafeReclaim(th)
    else:
      {.cast(gcsafe).}:
        `=destroy`(snap.raw[])
    deallocShared(snap.raw)
    snap.raw = nil
    deallocShared(snap)
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_ctrie_snapshot_get*(
    snap: ptr ll_ctrie_snapshot_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    out_val_pyobj: ptr pointer
): lf_status_t {.exportc: "ll_ctrie_snapshot_get", cdecl, gcsafe, raises: [].} =
  if unlikely(snap == nil or snap.raw == nil or out_val_pyobj == nil):
    return LF_ERR_INVALID_ARG
  try:
    let k = makeLlKey(key_kind, key_int, key_float, key_bytes, key_len)
    let opt = snap.raw[].get(k)
    if opt.isSome and opt.get.holder != nil:
      out_val_pyobj[] = opt.get.holder.valObj
      return LF_OK
    out_val_pyobj[] = nil
    return LF_ERR_EMPTY
  except CatchableError, Defect:
    out_val_pyobj[] = nil
    return LF_ERR_FAILURE

proc ll_ctrie_snapshot_len*(
    snap: ptr ll_ctrie_snapshot_t
): csize_t {.exportc: "ll_ctrie_snapshot_len", cdecl, gcsafe, raises: [].} =
  if unlikely(snap == nil or snap.raw == nil): return 0
  try:
    csize_t(snap.raw[].len)
  except CatchableError, Defect:
    0

proc ll_ctrie_snapshot_entries*(
    snap: ptr ll_ctrie_snapshot_t,
    out_keys: ptr pointer,
    out_vals: ptr pointer,
    max_count: csize_t
): csize_t {.exportc: "ll_ctrie_snapshot_entries", cdecl, gcsafe, raises: [].} =
  if unlikely(snap == nil or snap.raw == nil or max_count == 0):
    return 0
  let keysArr = if out_keys != nil: cast[ptr UncheckedArray[pointer]](out_keys) else: nil
  let valsArr = if out_vals != nil: cast[ptr UncheckedArray[pointer]](out_vals) else: nil
  var idx: csize_t = 0
  try:
    for _, box in snap.raw[].pairs:
      if idx >= max_count:
        break
      if box.holder != nil:
        if keysArr != nil: keysArr[int(idx)] = box.holder.keyObj
        if valsArr != nil: valsArr[int(idx)] = box.holder.valObj
        inc idx
    return idx
  except CatchableError, Defect:
    return idx

# --- 4B. SkipListMap (`ll_skiplist_t`) ---

type
  ll_skiplist_t* {.exportc: "ll_skiplist_t".} = object
    raw*: ptr SkipListMap[LlKey, PyKVBox]

proc ll_skiplist_create*(
    out_map: ptr ptr ll_skiplist_t
): lf_status_t {.exportc: "ll_skiplist_create", cdecl, gcsafe, raises: [].} =
  if unlikely(out_map == nil):
    return LF_ERR_INVALID_ARG
  try:
    let m = cast[ptr ll_skiplist_t](allocShared0(sizeof(ll_skiplist_t)))
    let raw = cast[ptr SkipListMap[LlKey, PyKVBox]](allocShared0(sizeof(SkipListMap[LlKey, PyKVBox])))
    raw[] = initSkipListMap[LlKey, PyKVBox]()
    m.raw = raw
    out_map[] = m
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_skiplist_destroy*(
    map: ptr ll_skiplist_t
): lf_status_t {.exportc: "ll_skiplist_destroy", cdecl, gcsafe, raises: [].} =
  if unlikely(map == nil or map.raw == nil):
    return LF_ERR_INVALID_ARG
  try:
    let th = map.raw[].getOrRegisterHandle()
    if th.manager != nil:
      for _ in 0 .. 3:
        discard th.manager.globalEpoch.fetchAdd(1'u64, moRelease)
        {.cast(gcsafe).}:
          discard reclaimNow(th)
    {.cast(gcsafe).}:
      `=destroy`(map.raw[])
    deallocShared(map.raw)
    map.raw = nil
    deallocShared(map)
    return LF_OK
  except CatchableError, Defect:
    return LF_ERR_FAILURE

proc ll_skiplist_put*(
    map: ptr ll_skiplist_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    key_pyobj: pointer,
    val_pyobj: pointer,
    out_inserted: ptr bool
): lf_status_t {.exportc: "ll_skiplist_put", cdecl, gcsafe, raises: [].} =
  if unlikely(map == nil or map.raw == nil):
    return LF_ERR_INVALID_ARG
  {.cast(gcsafe).}:
    try:
      let k = makeLlKey(key_kind, key_int, key_float, key_bytes, key_len)
      let box = allocPyKVBox(key_pyobj, val_pyobj)
      let inserted = map.raw[].put(k, box)
      if out_inserted != nil:
        out_inserted[] = inserted
      if not inserted:
        let th = map.raw[].getOrRegisterHandle()
        trySafeReclaim(th)
      return LF_OK
    except CatchableError, Defect:
      return LF_ERR_FAILURE

proc ll_skiplist_put_if_absent*(
    map: ptr ll_skiplist_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    key_pyobj: pointer,
    val_pyobj: pointer,
    out_val_pyobj: ptr pointer
): lf_status_t {.exportc: "ll_skiplist_put_if_absent", cdecl, gcsafe, raises: [].} =
  if unlikely(map == nil or map.raw == nil):
    return LF_ERR_INVALID_ARG
  {.cast(gcsafe).}:
    try:
      let k = makeLlKey(key_kind, key_int, key_float, key_bytes, key_len)
      let box = allocPyKVBox(key_pyobj, val_pyobj)
      let settled = map.raw[].computeIfAbsent(k, proc(keyArg: LlKey): PyKVBox {.gcsafe.} =
        box
      )
      if out_val_pyobj != nil:
        out_val_pyobj[] = if settled.holder != nil: settled.holder.valObj else: nil
      return LF_OK
    except:
      return LF_ERR_FAILURE

proc ll_skiplist_get*(
    map: ptr ll_skiplist_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    out_val_pyobj: ptr pointer
): lf_status_t {.exportc: "ll_skiplist_get", cdecl, gcsafe, raises: [].} =
  if unlikely(map == nil or map.raw == nil or out_val_pyobj == nil):
    return LF_ERR_INVALID_ARG
  {.cast(gcsafe).}:
    try:
      let k = makeLlKey(key_kind, key_int, key_float, key_bytes, key_len)
      let opt = map.raw[].get(k)
      if opt.isSome and opt.get.holder != nil:
        out_val_pyobj[] = opt.get.holder.valObj
        return LF_OK
      out_val_pyobj[] = nil
      return LF_ERR_EMPTY
    except CatchableError, Defect:
      out_val_pyobj[] = nil
      return LF_ERR_FAILURE

proc ll_skiplist_remove*(
    map: ptr ll_skiplist_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    out_val_pyobj: ptr pointer
): lf_status_t {.exportc: "ll_skiplist_remove", cdecl, gcsafe, raises: [].} =
  if unlikely(map == nil or map.raw == nil):
    return LF_ERR_INVALID_ARG
  {.cast(gcsafe).}:
    try:
      let k = makeLlKey(key_kind, key_int, key_float, key_bytes, key_len)
      # Hold `opt` (`Option[PyKVBox]`) in a local variable across `delete(k)` and
      # `trySafeReclaim(th)` so `PyKVHolder.rc >= 1` is maintained until `out_val_pyobj[]`
      # has been safely extracted, and `PyKVBox.=destroy` retires `keyObj`/`valObj`
      # cleanly when `opt` exits scope.
      var opt = map.raw[].get(k)
      if map.raw[].delete(k):
        if out_val_pyobj != nil:
          out_val_pyobj[] = if opt.isSome and opt.get.holder != nil: opt.get.holder.valObj else: nil
        let th = map.raw[].getOrRegisterHandle()
        trySafeReclaim(th)
        return LF_OK
      if out_val_pyobj != nil:
        out_val_pyobj[] = nil
      return LF_ERR_EMPTY
    except CatchableError, Defect:
      if out_val_pyobj != nil:
        out_val_pyobj[] = nil
      return LF_ERR_FAILURE

proc ll_skiplist_len*(map: ptr ll_skiplist_t): csize_t {.exportc: "ll_skiplist_len", cdecl, gcsafe, raises: [].} =
  if unlikely(map == nil or map.raw == nil): return 0
  {.cast(gcsafe).}:
    try:
      csize_t(map.raw[].len)
    except CatchableError, Defect:
      0

proc ll_skiplist_entries*(
    map: ptr ll_skiplist_t,
    out_keys: ptr pointer,
    out_vals: ptr pointer,
    max_count: csize_t
): csize_t {.exportc: "ll_skiplist_entries", cdecl, gcsafe, raises: [].} =
  if unlikely(map == nil or map.raw == nil or max_count == 0):
    return 0
  let keysArr = if out_keys != nil: cast[ptr UncheckedArray[pointer]](out_keys) else: nil
  let valsArr = if out_vals != nil: cast[ptr UncheckedArray[pointer]](out_vals) else: nil
  var idx: csize_t = 0
  {.cast(gcsafe).}:
    try:
      for _, box in map.raw[].pairs:
        if idx >= max_count:
          break
        if box.holder != nil:
          if keysArr != nil: keysArr[int(idx)] = box.holder.keyObj
          if valsArr != nil: valsArr[int(idx)] = box.holder.valObj
          inc idx
      return idx
    except CatchableError, Defect:
      return idx

# --- 4C. SkipListSet (`ll_skiplist_set_t`) ---

type
  ll_skiplist_set_t* {.exportc: "ll_skiplist_set_t".} = object
    raw*: ptr SkipListMap[LlKey, PyKVBox]

proc ll_skiplist_set_create*(
    out_set: ptr ptr ll_skiplist_set_t
): lf_status_t {.exportc: "ll_skiplist_set_create", cdecl, gcsafe, raises: [].} =
  if unlikely(out_set == nil):
    return LF_ERR_INVALID_ARG
  ll_skiplist_create(cast[ptr ptr ll_skiplist_t](out_set))

proc ll_skiplist_set_destroy*(
    s: ptr ll_skiplist_set_t
): lf_status_t {.exportc: "ll_skiplist_set_destroy", cdecl, gcsafe, raises: [].} =
  ll_skiplist_destroy(cast[ptr ll_skiplist_t](s))

proc ll_skiplist_set_add*(
    s: ptr ll_skiplist_set_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t,
    item_pyobj: pointer,
    out_added: ptr bool
): lf_status_t {.exportc: "ll_skiplist_set_add", cdecl, gcsafe, raises: [].} =
  ll_skiplist_put(
    cast[ptr ll_skiplist_t](s),
    key_kind, key_int, key_float, key_bytes, key_len,
    item_pyobj, nil, out_added
  )

proc ll_skiplist_set_contains*(
    s: ptr ll_skiplist_set_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t
): bool {.exportc: "ll_skiplist_set_contains", cdecl, gcsafe, raises: [].} =
  var dummy: pointer = nil
  ll_skiplist_get(
    cast[ptr ll_skiplist_t](s),
    key_kind, key_int, key_float, key_bytes, key_len,
    addr dummy
  ) == LF_OK

proc ll_skiplist_set_remove*(
    s: ptr ll_skiplist_set_t,
    key_kind: uint8,
    key_int: int64,
    key_float: float64,
    key_bytes: pointer,
    key_len: csize_t
): lf_status_t {.exportc: "ll_skiplist_set_remove", cdecl, gcsafe, raises: [].} =
  ll_skiplist_remove(
    cast[ptr ll_skiplist_t](s),
    key_kind, key_int, key_float, key_bytes, key_len,
    nil
  )

proc ll_skiplist_set_len*(
    s: ptr ll_skiplist_set_t
): csize_t {.exportc: "ll_skiplist_set_len", cdecl, gcsafe, raises: [].} =
  ll_skiplist_len(cast[ptr ll_skiplist_t](s))

proc ll_skiplist_set_items*(
    s: ptr ll_skiplist_set_t,
    out_items: ptr pointer,
    max_count: csize_t
): csize_t {.exportc: "ll_skiplist_set_items", cdecl, gcsafe, raises: [].} =
  ll_skiplist_entries(cast[ptr ll_skiplist_t](s), out_items, nil, max_count)

# ==============================================================================
# 5. Rate Limiter Property Accessors & C-ABI Symbol Retention
# ==============================================================================

proc lf_token_bucket_capacity*(
    bucket: ptr lf_token_bucket_t
): uint64 {.exportc: "lf_token_bucket_capacity", cdecl, gcsafe, raises: [].} =
  if unlikely(bucket == nil):
    return 0'u64
  if bucket.scale_factor > 0'u64:
    bucket.capacity div bucket.scale_factor
  else:
    bucket.capacity

proc lf_token_bucket_refill_rate*(
    bucket: ptr lf_token_bucket_t
): uint64 {.exportc: "lf_token_bucket_refill_rate", cdecl, gcsafe, raises: [].} =
  if unlikely(bucket == nil):
    return 0'u64
  bucket.refill_rate_per_sec

proc lf_leaky_bucket_burst_tolerance_ns*(
    bucket: ptr lf_leaky_bucket_t
): uint64 {.exportc: "lf_leaky_bucket_burst_tolerance_ns", cdecl, gcsafe, raises: [].} =
  if unlikely(bucket == nil):
    return 0'u64
  bucket.burst_tolerance_ns

proc lf_leaky_bucket_leak_rate*(
    bucket: ptr lf_leaky_bucket_t
): uint64 {.exportc: "lf_leaky_bucket_leak_rate", cdecl, gcsafe, raises: [].} =
  if unlikely(bucket == nil):
    return 0'u64
  bucket.leak_rate_per_sec

proc ll_keepalive_cabi*(): int {.exportc: "ll_keepalive_cabi", cdecl, gcsafe, raises: [].} =
  let ptrs = [
    cast[pointer](lf_bqueue128_create),
    cast[pointer](lf_bqueue128_clear),
    cast[pointer](lf_bqueue128_destroy),
    cast[pointer](lf_bqueue128_push),
    cast[pointer](lf_bqueue128_push_ex),
    cast[pointer](lf_bqueue128_pop),
    cast[pointer](lf_bqueue128_peek),
    cast[pointer](lf_bqueue128_len),
    cast[pointer](lf_bqueue128_capacity),
    cast[pointer](lf_bqueue128_is_empty),
    cast[pointer](lf_bqueue128_is_full),
    cast[pointer](lf_bounded_mpmc_create),
    cast[pointer](lf_unbounded_mpmc_create),
    cast[pointer](lf_queue_close),
    cast[pointer](lf_queue_destroy),
    cast[pointer](lf_producer_acquire),
    cast[pointer](lf_producer_release),
    cast[pointer](lf_consumer_acquire),
    cast[pointer](lf_consumer_release),
    cast[pointer](lf_push),
    cast[pointer](lf_push_ex),
    cast[pointer](lf_pop),
    cast[pointer](lf_pop_batch),
    cast[pointer](lf_queue_len),
    cast[pointer](lf_queue_is_empty),
    cast[pointer](lf_queue_is_closed),
    cast[pointer](lf_streambuffer_create),
    cast[pointer](lf_streambuffer_destroy),
    cast[pointer](lf_streambuffer_capacity),
    cast[pointer](lf_streambuffer_available_read),
    cast[pointer](lf_streambuffer_available_write),
    cast[pointer](lf_streambuffer_is_empty),
    cast[pointer](lf_streambuffer_is_full),
    cast[pointer](lf_streambuffer_try_write),
    cast[pointer](lf_streambuffer_try_read),
    cast[pointer](lf_stream_reserve_write),
    cast[pointer](lf_stream_commit_write),
    cast[pointer](lf_stream_peek_read),
    cast[pointer](lf_stream_consume_read),
    cast[pointer](lf_rendezvous_create),
    cast[pointer](lf_rendezvous_destroy),
    cast[pointer](lf_rendezvous_close),
    cast[pointer](lf_rendezvous_is_closed),
    cast[pointer](lf_rendezvous_send),
    cast[pointer](lf_rendezvous_recv),
    cast[pointer](lf_rendezvous_try_send),
    cast[pointer](lf_rendezvous_try_recv),
    cast[pointer](lf_rendezvous_send_timeout),
    cast[pointer](lf_rendezvous_recv_timeout),
    cast[pointer](lf_taskpool_create),
    cast[pointer](lf_taskpool_destroy),
    cast[pointer](lf_taskpool_spawn),
    cast[pointer](lf_taskpool_parallel_for),
    cast[pointer](lf_taskpool_sync),
    cast[pointer](lf_taskpool_num_workers),
    cast[pointer](lf_token_bucket_create),
    cast[pointer](lf_token_bucket_destroy),
    cast[pointer](lf_token_bucket_reset),
    cast[pointer](lf_token_bucket_try_acquire),
    cast[pointer](lf_token_bucket_acquire_timeout),
    cast[pointer](lf_token_bucket_available),
    cast[pointer](lf_leaky_bucket_create),
    cast[pointer](lf_leaky_bucket_destroy),
    cast[pointer](lf_leaky_bucket_reset),
    cast[pointer](lf_leaky_bucket_try_consume),
    cast[pointer](lf_leaky_bucket_consume_timeout),
    cast[pointer](lf_leaky_bucket_water_level),
    cast[pointer](lf_token_bucket_capacity),
    cast[pointer](lf_token_bucket_refill_rate),
    cast[pointer](lf_leaky_bucket_burst_tolerance_ns),
    cast[pointer](lf_leaky_bucket_leak_rate),
    cast[pointer](naganim_init_runtime),
    cast[pointer](naganim_shutdown_runtime),
    cast[pointer](naganim_flush_deferred_decrefs),
    cast[pointer](naganim_deferred_decref_count),
    cast[pointer](ll_enter_borrow),
    cast[pointer](ll_exit_borrow),
    cast[pointer](ll_register_py_compare),
    cast[pointer](ll_retire_pyobj),
    cast[pointer](ll_pop_deferred_decref),
    cast[pointer](ll_pop_deferred_decref_batch),
    cast[pointer](ll_deferred_decref_count),
    cast[pointer](ll_spsc_create),
    cast[pointer](ll_spsc_destroy),
    cast[pointer](ll_spsc_close),
    cast[pointer](ll_spsc_is_closed),
    cast[pointer](ll_spsc_push),
    cast[pointer](ll_spsc_push_ex),
    cast[pointer](ll_spsc_pop),
    cast[pointer](ll_spsc_peek),
    cast[pointer](ll_spsc_pop_batch),
    cast[pointer](ll_spsc_clear),
    cast[pointer](ll_spsc_len),
    cast[pointer](ll_spsc_capacity),
    cast[pointer](ll_spsc_is_empty),
    cast[pointer](ll_spsc_is_full),
    cast[pointer](ll_broadcast_create),
    cast[pointer](ll_broadcast_close),
    cast[pointer](ll_broadcast_is_closed),
    cast[pointer](ll_broadcast_destroy),
    cast[pointer](ll_broadcast_publish),
    cast[pointer](ll_broadcast_try_publish),
    cast[pointer](ll_broadcast_subscribe),
    cast[pointer](ll_broadcast_unsubscribe),
    cast[pointer](ll_broadcast_poll),
    cast[pointer](ll_broadcast_cursor_lag),
    cast[pointer](ll_broadcast_len),
    cast[pointer](ll_broadcast_capacity),
    cast[pointer](ll_broadcast_subscriber_count),
    cast[pointer](ll_ctrie_create),
    cast[pointer](ll_ctrie_destroy),
    cast[pointer](ll_ctrie_put),
    cast[pointer](ll_ctrie_put_if_absent),
    cast[pointer](ll_ctrie_get),
    cast[pointer](ll_ctrie_remove),
    cast[pointer](ll_ctrie_len),
    cast[pointer](ll_ctrie_entries),
    cast[pointer](ll_ctrie_snapshot),
    cast[pointer](ll_ctrie_snapshot_destroy),
    cast[pointer](ll_ctrie_snapshot_get),
    cast[pointer](ll_ctrie_snapshot_len),
    cast[pointer](ll_ctrie_snapshot_entries),
    cast[pointer](ll_skiplist_create),
    cast[pointer](ll_skiplist_destroy),
    cast[pointer](ll_skiplist_put),
    cast[pointer](ll_skiplist_put_if_absent),
    cast[pointer](ll_skiplist_get),
    cast[pointer](ll_skiplist_remove),
    cast[pointer](ll_skiplist_len),
    cast[pointer](ll_skiplist_entries),
    cast[pointer](ll_skiplist_set_create),
    cast[pointer](ll_skiplist_set_destroy),
    cast[pointer](ll_skiplist_set_add),
    cast[pointer](ll_skiplist_set_contains),
    cast[pointer](ll_skiplist_set_remove),
    cast[pointer](ll_skiplist_set_len),
    cast[pointer](ll_skiplist_set_items)
  ]
  ptrs.len

let gKeepAliveCabi* {.exportc: "ll_keepalive_cabi_val", dynlib.} = ll_keepalive_cabi()

{.pop.}
