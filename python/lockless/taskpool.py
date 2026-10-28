"""Work-stealing TaskPool backed by the Nim Chase-Lev C-ABI."""

from __future__ import annotations

import ctypes
import inspect
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import Future
from pathlib import Path
from typing import Any, TypeVar

from ._cabi import (
    LF_OK,
    CheckStatusError,
    Closed,
    drain_deferred_decrefs,
    ffi,
    lib,
)

T = TypeVar("T")
R = TypeVar("R")


def _load_cdll() -> ctypes.CDLL:
    pkg_dir = Path(__file__).resolve().parent
    for name in ("liblockless_ext.dylib", "liblockless_ext.so", "lockless_ext.dll"):
        p = pkg_dir / name
        if p.exists():
            cdll = ctypes.CDLL(str(p))
            cdll.lf_taskpool_sync.argtypes = [ctypes.c_void_p]
            cdll.lf_taskpool_sync.restype = ctypes.c_int32
            cdll.lf_taskpool_destroy.argtypes = [ctypes.c_void_p]
            cdll.lf_taskpool_destroy.restype = ctypes.c_int32
            cdll.lf_taskpool_parallel_for.argtypes = [
                ctypes.c_void_p,
                ctypes.c_size_t,
                ctypes.c_size_t,
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_size_t,
            ]
            cdll.lf_taskpool_parallel_for.restype = ctypes.c_int32
            return cdll
    raise RuntimeError(f"Could not load CDLL from {pkg_dir}")


_cdll = _load_cdll()

_TASKS_LOCK = threading.Lock()
_NEXT_TASK_ID = 1
_PENDING_TASKS: dict[
    int, tuple[Future[Any], Callable[..., Any], tuple[Any, ...], dict[str, Any]]
] = {}
_FOR_TASKS: dict[int, tuple[Callable[..., Any], int, list[BaseException]]] = {}


@ffi.callback("void(void*)")
def _task_trampoline(arg: Any) -> None:
    task_id = int(ffi.cast("uintptr_t", arg))
    with _TASKS_LOCK:
        entry = _PENDING_TASKS.pop(task_id, None)
    if entry is None:
        return
    fut, fn, args, kwargs = entry
    if not fut.set_running_or_notify_cancel():
        return
    try:
        res = fn(*args, **kwargs)
    except BaseException as exc:
        fut.set_exception(exc)
    else:
        fut.set_result(res)


@ffi.callback("void(size_t, void*)")
def _for_trampoline(index: int, arg: Any) -> None:
    for_id = int(ffi.cast("uintptr_t", arg))
    with _TASKS_LOCK:
        entry = _FOR_TASKS.get(for_id)
    if entry is None:
        return
    fn, arity, errors = entry
    if errors:
        return
    try:
        idx = int(index)
        if arity == 2:
            fn(idx, idx + 1)
        else:
            fn(idx)
    except BaseException as exc:
        with _TASKS_LOCK:
            errors.append(exc)


_FOR_TRAMPOLINE_ADDR = int(ffi.cast("uintptr_t", _for_trampoline))


class TaskPool:
    """Chase-Lev work-stealing thread pool (`TaskPool`).

    Parameters
    ----------
    num_workers : int, default 0
        Number of worker threads. ``0`` auto-detects the hardware core count.
    """

    __slots__ = ("_destroyed", "_ptr")

    def __init__(self, num_workers: int = 0) -> None:
        if not isinstance(num_workers, int) or isinstance(num_workers, bool) or num_workers < 0:
            raise ValueError(f"num_workers must be a non-negative integer, got {num_workers}")
        out = ffi.new("lf_taskpool_t**")
        rc = lib.lf_taskpool_create(num_workers, out)
        CheckStatusError.check(rc, "TaskPool.__init__")
        self._ptr = out[0]
        self._destroyed = False

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("TaskPool has been shut down")

    @property
    def num_workers(self) -> int:
        self._check_alive()
        return int(lib.lf_taskpool_num_workers(self._ptr))

    @property
    def is_closed(self) -> bool:
        return self._destroyed or self._ptr == ffi.NULL

    def submit(self, fn: Callable[..., R], /, *args: Any, **kwargs: Any) -> Future[R]:
        """Spawns ``fn(*args, **kwargs)`` onto the work-stealing pool and returns a ``Future``."""
        self._check_alive()
        global _NEXT_TASK_ID
        fut: Future[R] = Future()
        with _TASKS_LOCK:
            task_id = _NEXT_TASK_ID
            _NEXT_TASK_ID += 1
            _PENDING_TASKS[task_id] = (fut, fn, args, kwargs)

        rc = lib.lf_taskpool_spawn(self._ptr, _task_trampoline, ffi.cast("void*", task_id))
        if rc != LF_OK:
            with _TASKS_LOCK:
                _PENDING_TASKS.pop(task_id, None)
            CheckStatusError.check(rc, "TaskPool.submit")
        return fut

    def map(
        self,
        fn: Callable[[T], R],
        iterable: Iterable[T],
        *,
        timeout: float | None = None,
    ) -> list[R]:
        """Applies ``fn`` to every item in ``iterable`` across the work-stealing pool."""
        self._check_alive()
        futs = [self.submit(fn, item) for item in iterable]
        self.sync()
        return [f.result(timeout=timeout) for f in futs]

    def parallel_for(
        self,
        start: int,
        stop: int,
        fn: Callable[..., Any],
        *,
        chunk_size: int = 0,
    ) -> None:
        """Executes ``fn`` over ``range(start, stop)`` using work-stealing chunk subdivision.

        ``fn`` may accept either a single index ``fn(i)`` or a chunk range ``fn(chunk_start, chunk_stop)``.
        """
        self._check_alive()
        if (
            not isinstance(start, int)
            or not isinstance(stop, int)
            or not isinstance(chunk_size, int)
        ):
            raise TypeError("start, stop, and chunk_size must be integers")
        if chunk_size < 0:
            raise ValueError("chunk_size must be non-negative")
        if stop <= start:
            return

        try:
            sig = inspect.signature(fn)
            pos_params = [
                p
                for p in sig.parameters.values()
                if p.kind
                in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]
            has_varargs = any(
                p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values()
            )
            arity = 2 if (len(pos_params) >= 2 or has_varargs) else 1
        except (ValueError, TypeError):
            arity = 1

        global _NEXT_TASK_ID
        errors: list[BaseException] = []
        with _TASKS_LOCK:
            for_id = _NEXT_TASK_ID
            _NEXT_TASK_ID += 1
            _FOR_TASKS[for_id] = (fn, arity, errors)

        pool_addr = int(ffi.cast("uintptr_t", self._ptr))
        try:
            rc = _cdll.lf_taskpool_parallel_for(
                pool_addr,
                start,
                stop - 1,
                _FOR_TRAMPOLINE_ADDR,
                for_id,
                chunk_size,
            )
        finally:
            with _TASKS_LOCK:
                _FOR_TASKS.pop(for_id, None)

        CheckStatusError.check(rc, "TaskPool.parallel_for")
        if errors:
            raise errors[0]

    def sync(self) -> None:
        """Blocks (releasing the GIL) until all currently spawned tasks have completed."""
        self._check_alive()
        pool_addr = int(ffi.cast("uintptr_t", self._ptr))
        rc = _cdll.lf_taskpool_sync(pool_addr)
        drain_deferred_decrefs()
        CheckStatusError.check(rc, "TaskPool.sync")

    def shutdown(self, wait: bool = True) -> None:
        """Synchronizes in-flight tasks and destroys the underlying worker pool."""
        if not self._destroyed and self._ptr != ffi.NULL:
            pool_addr = int(ffi.cast("uintptr_t", self._ptr))
            if wait:
                _cdll.lf_taskpool_sync(pool_addr)
            self._ptr = ffi.NULL
            self._destroyed = True
            _cdll.lf_taskpool_destroy(pool_addr)
            drain_deferred_decrefs()

    close = shutdown
    destroy = shutdown

    def __enter__(self) -> TaskPool:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown(wait=True)

    def __del__(self) -> None:
        try:
            self.shutdown(wait=False)
        except Exception:
            pass
