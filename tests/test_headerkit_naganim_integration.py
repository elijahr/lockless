from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import lockless
import lockless._cabi as cabi
import lockless._cdef_generated as cdef_generated
import lockless.channels as channels_mod
import lockless.ratelimit as ratelimit_mod
import lockless.taskpool as taskpool_mod
from headerkit import Function, generate
from lockless._codegen import (
    DEFAULT_CDEF_PY_PATH,
    DEFAULT_CDEF_PYI_PATH,
    DEFAULT_HEADER_PATH,
    generate_cdef_and_pyi,
    parse_lockless_header,
    write_generated_bindings,
)


def test_headerkit_codegen_pipeline_and_cdef_parity() -> None:
    header_ir = parse_lockless_header(DEFAULT_HEADER_PATH)
    func_names = [decl.name for decl in header_ir.declarations if isinstance(decl, Function)]
    assert "naganim_init_runtime" in func_names
    assert "ll_spsc_push_ex" in func_names
    assert "ll_broadcast_publish" in func_names
    assert "ll_ctrie_put" in func_names
    assert "lf_taskpool_parallel_for" in func_names

    enriched_ir = parse_lockless_header(
        DEFAULT_HEADER_PATH,
        jsondoc_entries=[{"name": "naganim_init_runtime", "type": "skProc", "description": "Init runtime."}],
    )
    init_fn = next(
        decl for decl in enriched_ir.declarations if isinstance(decl, Function) and decl.name == "naganim_init_runtime"
    )
    assert init_fn.doc == "Init runtime."
    assert init_fn.source_meta is not None and init_fn.source_meta.source_language == "nim"

    cdef_str, pyi_str = generate_cdef_and_pyi(DEFAULT_HEADER_PATH, enrich_from_nim=False)
    assert cdef_str == cdef_generated.CDEF
    assert DEFAULT_CDEF_PY_PATH.exists() is True
    assert DEFAULT_CDEF_PYI_PATH.exists() is True

    raw_cdef = generate(
        header_path=DEFAULT_HEADER_PATH,
        writer_name="cffi",
        include_dirs=[str(DEFAULT_HEADER_PATH.parent)],
        no_cache=True,
    )
    assert raw_cdef.replace("_Bool", "bool") == cdef_str
    assert "def naganim_init_runtime() -> int: ..." in pyi_str
    assert "def ll_spsc_push_ex(" in pyi_str

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_py = Path(tmp_dir) / "_cdef_generated.py"
        tmp_pyi = Path(tmp_dir) / "_cdef_generated.pyi"
        out_py, out_pyi = write_generated_bindings(
            DEFAULT_HEADER_PATH,
            tmp_py,
            tmp_pyi,
            enrich_from_nim=False,
        )
        assert out_py == tmp_py
        assert out_pyi == tmp_pyi
        assert tmp_py.read_text(encoding="utf-8") == DEFAULT_CDEF_PY_PATH.read_text(encoding="utf-8")
        assert tmp_pyi.read_text(encoding="utf-8") == pyi_str


def test_shared_cdll_singleton_across_submodules() -> None:
    assert channels_mod.cdll is cabi.cdll
    assert ratelimit_mod.cdll is cabi.cdll
    assert taskpool_mod.cdll is cabi.cdll


def test_naganim_deferred_decref_bridge_and_borrow_guard() -> None:
    lockless.drain_deferred_decrefs()
    assert int(cabi.lib.naganim_deferred_decref_count()) == 0
    assert int(cabi.lib.ll_deferred_decref_count()) == 0

    payload = {"bridge": "naganim", "seq": [1, 2, 3]}
    base_rc = sys.getrefcount(payload)

    raw_ptr = cabi.box_pyobj(payload)
    assert sys.getrefcount(payload) == base_rc + 1

    # Retire the boxed object via the Nim -> naganim deferred decref bridge
    cabi.lib.ll_retire_pyobj(raw_ptr)
    assert int(cabi.lib.naganim_deferred_decref_count()) == 1
    assert int(cabi.lib.ll_deferred_decref_count()) == 1

    # Negative control: while a borrow guard is active, drain_deferred_decrefs must not drain
    assert int(cabi.lib.naganim_active_borrowers()) == 0
    cabi.enter_borrow()
    try:
        assert int(cabi.lib.naganim_active_borrowers()) == 1
        drained_during_borrow = lockless.drain_deferred_decrefs()
        assert drained_during_borrow == 0
        assert int(cabi.lib.naganim_deferred_decref_count()) == 1
        assert sys.getrefcount(payload) == base_rc + 1
    finally:
        cabi.exit_borrow()
    assert int(cabi.lib.naganim_active_borrowers()) == 0

    # Exiting the outermost borrow automatically drains the deferred decref queue
    assert int(cabi.lib.naganim_deferred_decref_count()) == 0
    assert int(cabi.lib.ll_deferred_decref_count()) == 0
    assert sys.getrefcount(payload) == base_rc
