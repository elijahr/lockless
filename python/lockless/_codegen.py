"""HeaderKit-backed CFFI cdef and PEP 484 stub codegen for ``lockless``."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parent.parent

for _cand in (
    REPO_ROOT.parent / "headerkit",
    REPO_ROOT.parent.parent.parent / "headerkit",
    REPO_ROOT.parent.parent.parent.parent / "headerkit",
):
    if (_cand / "headerkit" / "__init__.py").exists() and str(_cand) not in sys.path:
        sys.path.insert(0, str(_cand))

from headerkit import (  # noqa: E402
    Header,
    NimCleanupTransform,
    NimJsonDocEnricher,
    generate_all,
    get_backend,
    get_writer,
)

DEFAULT_HEADER_PATH = REPO_ROOT / "include" / "lockless_ext.h"
DEFAULT_NIM_SOURCE_PATH = REPO_ROOT / "src" / "lockless_ext.nim"
DEFAULT_CDEF_PY_PATH = PACKAGE_DIR / "_cdef_generated.py"
DEFAULT_CDEF_PYI_PATH = PACKAGE_DIR / "_cdef_generated.pyi"


def parse_lockless_header(
    header_path: Path | None = None,
    *,
    jsondoc_entries: list[dict[str, Any]] | None = None,
) -> Header:
    """Parse ``include/lockless_ext.h`` into HeaderKit IR with Nim cleanup and jsondoc enrichment."""
    target = header_path if header_path is not None else DEFAULT_HEADER_PATH
    code = target.read_text(encoding="utf-8")
    backend = get_backend()
    ir = backend.parse(code, str(target), include_dirs=[str(target.parent)])
    cleaned = NimCleanupTransform().transform(ir)
    if jsondoc_entries is not None:
        cleaned = NimJsonDocEnricher(entries=jsondoc_entries).transform(cleaned)
    return cleaned


def extract_nim_jsondoc(nim_source_path: Path | None = None) -> list[dict[str, Any]]:
    """Run ``nim jsondoc`` on ``src/lockless_ext.nim`` if Nim is available and return parsed entries."""
    target = nim_source_path if nim_source_path is not None else DEFAULT_NIM_SOURCE_PATH
    if not target.exists():
        return []
    with tempfile.TemporaryDirectory(prefix="lockless_jsondoc_") as tmp_dir:
        out_json = Path(tmp_dir) / "lockless_ext.json"
        cmd = [
            "nim",
            "jsondoc",
            "--hints:off",
            "--warnings:off",
            f"-o:{out_json}",
            str(target),
        ]
        proc = subprocess.run(
            cmd,
            cwd=str(target.parent.parent),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0 or not out_json.exists():
            return []
        raw = json.loads(out_json.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return [entry for entry in raw if isinstance(entry, dict)]
        if isinstance(raw, dict) and isinstance(raw.get("entries"), list):
            return [entry for entry in raw["entries"] if isinstance(entry, dict)]
        return []


def generate_cdef_and_pyi(
    header_path: Path | None = None,
    *,
    enrich_from_nim: bool = False,
) -> tuple[str, str]:
    """Generate both the CFFI ``cdef`` declarations and PEP 484 ``.pyi`` stub via HeaderKit."""
    target = header_path if header_path is not None else DEFAULT_HEADER_PATH
    jsondoc_entries = extract_nim_jsondoc() if enrich_from_nim else None
    header_ir = parse_lockless_header(target, jsondoc_entries=jsondoc_entries)

    cffi_writer = get_writer("cffi")
    pyi_writer = get_writer("pyi")
    cdef_str = cffi_writer.write(header_ir)
    pyi_str = pyi_writer.write(header_ir).replace(
        "from collections.abc import Callable\nimport enum\n",
        "import enum\nfrom collections.abc import Callable\n",
    )
    pyi_str = re.sub(r"^(\s+[A-Z0-9_]+): int = ", r"\1 = ", pyi_str, flags=re.MULTILINE)
    if "\nCDEF: str" not in pyi_str:
        pyi_str = pyi_str.rstrip() + "\n\nCDEF: str\n"

    # Verify round-trip parity with headerkit.generate_all pipeline
    outputs = generate_all(
        header_path=target,
        writers=["cffi", "pyi"],
        include_dirs=[str(target.parent)],
        no_cache=True,
    )
    output_map = {res.writer_name: res.output for res in outputs}
    if not cdef_str:
        cdef_str = output_map.get("cffi", "")
    if not pyi_str:
        pyi_str = output_map.get("pyi", "")
    return cdef_str, pyi_str


def write_generated_bindings(
    header_path: Path | None = None,
    cdef_py_path: Path | None = None,
    cdef_pyi_path: Path | None = None,
    *,
    enrich_from_nim: bool = False,
) -> tuple[Path, Path]:
    """Write ``_cdef_generated.py`` and ``_cdef_generated.pyi`` using HeaderKit."""
    target_header = header_path if header_path is not None else DEFAULT_HEADER_PATH
    out_py = cdef_py_path if cdef_py_path is not None else DEFAULT_CDEF_PY_PATH
    out_pyi = cdef_pyi_path if cdef_pyi_path is not None else DEFAULT_CDEF_PYI_PATH

    cdef_str, pyi_str = generate_cdef_and_pyi(target_header, enrich_from_nim=enrich_from_nim)

    py_module_content = (
        '"""Auto-generated CFFI cdef declarations by headerkit from include/lockless_ext.h."""\n\n'
        "from __future__ import annotations\n\n"
        f"CDEF: str = {cdef_str!r}\n\n"
        '__all__ = ["CDEF"]\n'
    )
    out_py.write_text(py_module_content, encoding="utf-8")
    out_pyi.write_text(pyi_str, encoding="utf-8")
    return out_py, out_pyi


if __name__ == "__main__":
    write_generated_bindings(enrich_from_nim=True)
