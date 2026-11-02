"""HeaderKit-backed CFFI cdef and PEP 484 stub codegen for ``lockless``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from headerkit import (
    Header,
    NimCleanupTransform,
    NimJsonDocEnricher,
    generate,
    get_writer,
    json_to_header,
)

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parent.parent

DEFAULT_HEADER_PATH = REPO_ROOT / "include" / "lockless_ext.h"
DEFAULT_NIM_SOURCE_PATH = REPO_ROOT / "src" / "lockless_ext.nim"
DEFAULT_CDEF_PY_PATH = PACKAGE_DIR / "_cdef_generated.py"
DEFAULT_CDEF_PYI_PATH = PACKAGE_DIR / "_cdef_generated.pyi"


def parse_lockless_header(
    header_path: Path | None = None,
    *,
    jsondoc_entries: list[dict[str, Any]] | None = None,
    enrich_from_nim: bool = False,
) -> Header:
    """Parse ``include/lockless_ext.h`` into HeaderKit IR via ``headerkit.generate``."""
    target = header_path if header_path is not None else DEFAULT_HEADER_PATH
    transforms: list[Any] = [NimCleanupTransform()]
    if jsondoc_entries is not None:
        transforms.append(NimJsonDocEnricher(entries=jsondoc_entries))
    elif enrich_from_nim and DEFAULT_NIM_SOURCE_PATH.exists():
        transforms.append(NimJsonDocEnricher(nim_source_path=DEFAULT_NIM_SOURCE_PATH))
    ir_json = generate(
        header_path=target,
        writer_name="json",
        include_dirs=[str(target.parent)],
        transforms=transforms,
        no_cache=True,
    )
    return json_to_header(ir_json)


def generate_cdef_and_pyi(
    header_path: Path | None = None,
    *,
    enrich_from_nim: bool = False,
) -> tuple[str, str]:
    """Generate both the CFFI ``cdef`` declarations and PEP 484 ``.pyi`` stub via HeaderKit."""
    target = header_path if header_path is not None else DEFAULT_HEADER_PATH
    header_ir = parse_lockless_header(target, enrich_from_nim=enrich_from_nim)

    cffi_writer = get_writer("cffi")
    pyi_writer = get_writer("pyi", module_var="CDEF")
    cdef_str = cffi_writer.write(header_ir)
    pyi_str = pyi_writer.write(header_ir)
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
