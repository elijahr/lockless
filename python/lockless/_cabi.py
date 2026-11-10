"""C-ABI bindings, status codes, exceptions, and PyObject* reference helpers for lockless."""

from __future__ import annotations

import ctypes
import ctypes.util
import math
import struct
import sys
import threading
from pathlib import Path
from typing import Any

from cffi import FFI

from ._cdef_generated import CDEF

_PKG_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PKG_DIR.parent.parent
for _naganim_cand in (
    _REPO_ROOT.parent / "naganim" / "python",
    _REPO_ROOT.parent.parent.parent / "naganim" / "python",
    _REPO_ROOT.parent.parent.parent.parent / "naganim" / "python",
):
    if (_naganim_cand / "naganim" / "__init__.py").exists() and str(_naganim_cand) not in sys.path:
        sys.path.insert(0, str(_naganim_cand))

try:
    from naganim._cffi_loader import NaganimRuntimeError as _NaganimRuntimeError

    NaganimRuntimeError: type[RuntimeError] = _NaganimRuntimeError
except ImportError:

    class _FallbackNaganimRuntimeError(RuntimeError):
        """Fallback runtime error when naganim Python package is not installed."""

    NaganimRuntimeError = _FallbackNaganimRuntimeError

# ==============================================================================
# Exception Hierarchy
# ==============================================================================


class LocklessError(Exception):
    """Base exception for all lockless errors."""


class Empty(LocklessError):
    """Raised when a non-blocking pop/recv/poll encounters an empty container."""


class Full(LocklessError):
    """Raised when a non-blocking push/send/publish encounters a full container."""


class Closed(LocklessError):
    """Raised when an operation is attempted on a closed queue, channel, or ring."""


# ==============================================================================
# C-ABI Status Codes & Constants
# ==============================================================================

LF_ERR_PANIC = -2
LF_ERR_FAILURE = -1
LF_OK = 0
LF_ERR_EMPTY = 1
LF_ERR_FULL = 2
LF_ERR_CLOSED = 3
LF_ERR_INVALID_ARG = 4
LF_ERR_REGISTRY_FULL = 5
LF_ERR_UNSUPPORTED = 6
LF_ERR_TIMEOUT = 7

LF_OVERFLOW_DROP_OLDEST = 0
LF_OVERFLOW_BACKOFF = 1
LF_OVERFLOW_BACKPRESSURE = 1

LF_SUB_FROM_LATEST = 0
LF_SUB_FROM_EARLIEST = 1

LF_POLL_SUCCESS = 0
LF_POLL_EMPTY = 1
LF_POLL_LAGGED = 2


def check_status(status: int, msg: str = "lockless C-ABI error") -> None:
    """Translates a non-zero C-ABI status code into the corresponding Python exception."""
    if status == LF_OK:
        return
    if status == LF_ERR_EMPTY:
        raise Empty("Container is empty")
    if status == LF_ERR_FULL:
        raise Full("Container is full")
    if status == LF_ERR_CLOSED:
        raise Closed("Container is closed")
    if status == LF_ERR_INVALID_ARG:
        raise ValueError(f"{msg}: invalid argument (LF_ERR_INVALID_ARG)")
    if status == LF_ERR_REGISTRY_FULL:
        raise RuntimeError(f"{msg}: thread/cursor registry full (LF_ERR_REGISTRY_FULL)")
    if status == LF_ERR_UNSUPPORTED:
        raise ValueError(f"{msg}: unsupported capacity or configuration (LF_ERR_UNSUPPORTED)")
    if status == LF_ERR_TIMEOUT:
        raise TimeoutError(f"{msg}: operation timed out")
    raise LocklessError(f"{msg} (status={status})")


class CheckStatusError:
    check = staticmethod(check_status)


# ==============================================================================
# CFFI Declarations & Shared Library Loading (Generated via HeaderKit)
# ==============================================================================

ffi = FFI()
ffi.cdef(CDEF)


def _resolve_library_path() -> Path:
    import subprocess

    pkg_dir = Path(__file__).resolve().parent
    repo_root = pkg_dir.parent.parent
    nim_src = repo_root / "src" / "lockless_ext.nim"
    header_src = repo_root / "include" / "lockless_ext.h"
    src_mtimes = [p.stat().st_mtime for p in (nim_src, header_src) if p.exists()]
    latest_src_mtime = max(src_mtimes) if src_mtimes else 0.0

    candidates = [
        pkg_dir / "liblockless_ext.dylib",
        pkg_dir / "liblockless_ext.so",
        pkg_dir / "lockless_ext.dll",
    ]
    for path in candidates:
        if path.exists():
            if latest_src_mtime > 0.0 and nim_src.exists() and path.stat().st_mtime < latest_src_mtime:
                break
            return path

    if nim_src.exists():
        ext = (
            ".dylib" if sys.platform == "darwin" else (".dll" if sys.platform == "win32" else ".so")
        )
        out_path = pkg_dir / f"liblockless_ext{ext}"
        lockfree_src = repo_root.parent / "lockfree" / "src"
        if not lockfree_src.exists():
            lockfree_src = repo_root.parent.parent.parent.parent / "lockfree" / "src"
        naganim_src = repo_root.parent / "naganim" / "src"
        if not naganim_src.exists():
            naganim_src = repo_root.parent.parent.parent.parent / "naganim" / "src"
        cmd = [
            "nim",
            "c",
            "--app:lib",
            "--noMain",
            "--mm:orc",
            "--threads:on",
            "-d:useMalloc",
            "-d:noSignalHandler",
            "-d:release",
            f"--path:{lockfree_src}",
            f"--path:{naganim_src}",
            f"-o:{out_path}",
            str(nim_src),
        ]
        subprocess.run(cmd, check=True, cwd=str(repo_root))
        if out_path.exists():
            return out_path

    raise RuntimeError(f"Failed to locate compiled liblockless_ext shared library in {pkg_dir}")


def _configure_cdll(lib_path: Path) -> ctypes.CDLL:
    handle = ctypes.CDLL(str(lib_path))

    handle.lf_rendezvous_send_timeout.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int32,
        ctypes.POINTER(ctypes.c_uint64),
    ]
    handle.lf_rendezvous_send_timeout.restype = ctypes.c_int32

    handle.lf_rendezvous_recv_timeout.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_int32,
        ctypes.POINTER(ctypes.c_uint64),
    ]
    handle.lf_rendezvous_recv_timeout.restype = ctypes.c_int32

    handle.lf_token_bucket_acquire_timeout.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint64,
        ctypes.c_int64,
    ]
    handle.lf_token_bucket_acquire_timeout.restype = ctypes.c_bool

    handle.lf_leaky_bucket_consume_timeout.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint64,
        ctypes.c_int64,
    ]
    handle.lf_leaky_bucket_consume_timeout.restype = ctypes.c_bool

    handle.lf_taskpool_sync.argtypes = [ctypes.c_void_p]
    handle.lf_taskpool_sync.restype = ctypes.c_int32

    handle.lf_taskpool_destroy.argtypes = [ctypes.c_void_p]
    handle.lf_taskpool_destroy.restype = ctypes.c_int32

    handle.lf_taskpool_parallel_for.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    handle.lf_taskpool_parallel_for.restype = ctypes.c_int32

    return handle


def _load_library() -> tuple[Any, ctypes.CDLL]:
    lib_path = _resolve_library_path()
    cffi_lib = ffi.dlopen(str(lib_path))
    ctypes_cdll = _configure_cdll(lib_path)
    return cffi_lib, ctypes_cdll


lib, cdll = _load_library()
if int(lib.naganim_init_runtime()) != 0:
    raise NaganimRuntimeError("naganim_init_runtime() failed while initializing lockless")

# ==============================================================================
# CPython PyObject* Reference Accounting & Wait-Free Deferred DecRef Drain
# ==============================================================================

_Py_IncRef = ctypes.pythonapi.Py_IncRef
_Py_IncRef.argtypes = [ctypes.py_object]
_Py_IncRef.restype = None

_Py_DecRef_Ptr = ctypes.pythonapi.Py_DecRef
_Py_DecRef_Ptr.argtypes = [ctypes.c_void_p]
_Py_DecRef_Ptr.restype = None

_PyObject_RichCompareBool = ctypes.pythonapi.PyObject_RichCompareBool
_PyObject_RichCompareBool.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int]
_PyObject_RichCompareBool.restype = ctypes.c_int

_PyErr_Clear = ctypes.pythonapi.PyErr_Clear
_PyErr_Clear.argtypes = []
_PyErr_Clear.restype = None


@ffi.callback("int(void*, void*, int)")  # type: ignore[untyped-decorator]
def _py_rich_compare_cb(a: Any, b: Any, op: int) -> int:
    addr_a = int(ffi.cast("uintptr_t", a))
    addr_b = int(ffi.cast("uintptr_t", b))
    if addr_a == addr_b:
        return 1 if op == 2 else 0
    res = int(_PyObject_RichCompareBool(addr_a, addr_b, op))
    if res < 0:
        _PyErr_Clear()
        return 0
    return res


lib.ll_register_py_compare(_py_rich_compare_cb)

# Sentinel wrapper for `None` or `0` (NULL pointer) so raw `void*` slots can
# distinguish an empty NULL slot from a Python `None` payload.
_NONE_SENTINEL = object()

_DRAIN_BATCH_SIZE = 256
_borrow_tls = threading.local()


def _tls_borrow_depth() -> int:
    return int(getattr(_borrow_tls, "depth", 0))


def enter_borrow() -> None:
    """Marks the start of a C-ABI call that returns a borrowed `PyObject*`."""
    _borrow_tls.depth = getattr(_borrow_tls, "depth", 0) + 1
    lib.ll_enter_borrow()


def exit_borrow() -> None:
    """Marks the completion of unboxing a borrowed `PyObject*` under the GIL."""
    depth = getattr(_borrow_tls, "depth", 1) - 1
    _borrow_tls.depth = depth
    lib.ll_exit_borrow()
    if depth == 0:
        drain_deferred_decrefs()


def drain_deferred_decrefs() -> int:
    """Drains all pending retired `PyObject*` pointers and calls `Py_DecRef` under the GIL."""
    if _tls_borrow_depth() > 0 or lib.ll_deferred_decref_count() == 0:
        return 0
    total = 0
    buf = ffi.new("void*[]", _DRAIN_BATCH_SIZE)
    while _tls_borrow_depth() == 0:
        n = int(lib.ll_pop_deferred_decref_batch(buf, _DRAIN_BATCH_SIZE))
        if n == 0:
            break
        for i in range(n):
            addr = int(ffi.cast("uintptr_t", buf[i]))
            if addr != 0:
                _Py_DecRef_Ptr(addr)
        total += n
        if n < _DRAIN_BATCH_SIZE:
            break
    return total


def box_pyobj(obj: Any) -> Any:
    """Increments refcount of `obj` (or sentinel if None) and returns its `void*` CData pointer."""
    drain_deferred_decrefs()
    target = _NONE_SENTINEL if obj is None else obj
    _Py_IncRef(target)
    return ffi.cast("void*", id(target))


def unbox_pyobj_steal(ptr_cdata: Any) -> Any:
    """Transfers ownership from a container `void*` (`+1` ref) to a Python return value."""
    addr = int(ffi.cast("uintptr_t", ptr_cdata))
    if addr == 0:
        drain_deferred_decrefs()
        return None
    obj = ctypes.cast(addr, ctypes.py_object).value
    _Py_DecRef_Ptr(addr)
    drain_deferred_decrefs()
    if obj is _NONE_SENTINEL:
        return None
    return obj


def unbox_pyobj_borrow(ptr_cdata: Any) -> Any:
    """Reads a container `void*` without consuming the container's owned reference."""
    addr = int(ffi.cast("uintptr_t", ptr_cdata))
    if addr == 0:
        drain_deferred_decrefs()
        return None
    obj = ctypes.cast(addr, ctypes.py_object).value
    drain_deferred_decrefs()
    if obj is _NONE_SENTINEL:
        return None
    return obj


def decref_pyobj_ptr(ptr_cdata: Any) -> None:
    """Decrements the reference count of a `void*` `PyObject*` pointer if non-null."""
    addr = int(ffi.cast("uintptr_t", ptr_cdata))
    if addr != 0:
        _Py_DecRef_Ptr(addr)
    drain_deferred_decrefs()


# C-ABI callback passed to `lf_bqueue128_create` and `lf_bounded_mpmc_create` /
# `lf_unbounded_mpmc_create` so `clear()` and `destroy()` automatically enqueue
# remaining `PyObject*` pointers into the wait-free deferred decref queue.
@ffi.callback("void(void*, void*)")  # type: ignore[untyped-decorator]
def _pyobj_destructor_cb(item: Any, _user_data: Any) -> None:
    if item != ffi.NULL:
        lib.ll_retire_pyobj(item)


# ==============================================================================
# Key Encoding Helper for ConcurrentDict, SkipListMap, and SkipListSet
# ==============================================================================


def _encode_ordered_int(val: int) -> bytes:
    """Encodes any Python int into order-preserving lexicographical bytes."""
    if val == 0:
        return b"\x20"
    if val > 0:
        blen = (val.bit_length() + 7) // 8
        return b"\x30" + blen.to_bytes(8, "big") + val.to_bytes(blen, "big")
    mag = -val
    blen = (mag.bit_length() + 7) // 8
    inv_len = (0xFFFFFFFFFFFFFFFF - blen).to_bytes(8, "big")
    inv_mag = ((1 << (blen * 8)) - 1 - mag).to_bytes(blen, "big")
    return b"\x10" + inv_len + inv_mag


def _encode_ordered_float(val: float) -> bytes:
    """Encodes an IEEE-754 float64 into order-preserving lexicographical bytes."""
    if math.isnan(val):
        return b"\x39\xff\xff\xff\xff\xff\xff\xff\xff"
    if val == 0.0:
        val = 0.0
    # If val is an exact integer, use the unified integer encoding so `1 == 1.0` inside tuples!
    if math.isfinite(val) and val.is_integer():
        return _encode_ordered_int(int(val))
    u = int(struct.unpack(">Q", struct.pack(">d", val))[0])
    if u & (1 << 63):
        u ^= 0xFFFFFFFFFFFFFFFF
    else:
        u ^= 1 << 63
    return b"\x28" + u.to_bytes(8, "big")


def _encode_tuple_elem(elem: Any, *, ordered: bool) -> bytes:
    """Encodes a tuple element into order-preserving bytes without pickle."""
    if elem is None:
        if ordered:
            raise TypeError("'<' not supported between instances of 'NoneType'")
        return b"\x05"
    if isinstance(elem, bool):
        return _encode_ordered_int(1 if elem else 0)
    if isinstance(elem, int):
        return _encode_ordered_int(elem)
    if isinstance(elem, float):
        return _encode_ordered_float(elem)
    if isinstance(elem, str):
        raw = elem.encode("utf-8").replace(b"\x00", b"\x00\xff")
        return b"\x40" + raw + b"\x00\x00"
    if isinstance(elem, bytes):
        raw = elem.replace(b"\x00", b"\x00\xff")
        return b"\x50" + raw + b"\x00\x00"
    if isinstance(elem, tuple):
        inner = b"".join(_encode_tuple_elem(x, ordered=ordered) for x in elem)
        return b"\x60" + inner + b"\x00"
    if ordered:
        _ = elem < elem
    raise TypeError(
        f"Unsupported non-scalar tuple element type {type(elem).__name__!r}; "
        "tuple keys must contain int, float, bool, str, bytes, or nested tuples"
    )


def encode_key(key: Any, *, ordered: bool = False) -> tuple[int, int, float, bytes]:
    """Encodes a hashable/comparable Python key into `(kind, int_val, float_val, bytes_val)`."""
    # Check hashable first so unhashable types (e.g. list, dict) raise TypeError immediately
    h = hash(key)
    if isinstance(key, bool):
        return (0, 1 if key else 0, 0.0, b"")
    if isinstance(key, int):
        if -9223372036854775808 <= key <= 9223372036854775807:
            return (0, key, 0.0, b"")
        return (0, -1 if key < 0 else 1, 0.0, _encode_ordered_int(key))
    if isinstance(key, float):
        return (1, 0, key, b"")
    if isinstance(key, str):
        return (2, 0, 0.0, key.encode("utf-8"))
    if isinstance(key, bytes):
        return (3, 0, 0.0, key)
    if isinstance(key, tuple):
        encoded = b"".join(_encode_tuple_elem(e, ordered=ordered) for e in key) + b"\x00"
        return (4, 0, 0.0, encoded)
    if ordered:
        _ = key < key
    return (5, h, 0.0, struct.pack("=Q", id(key)))
