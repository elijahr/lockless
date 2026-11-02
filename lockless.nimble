# Package

version       = "0.1.0"
author        = "Axiomantic"
description   = "C-ABI extension backing the lockless Python concurrency & DSP package"
license       = "MIT"
srcDir        = "src"

# Dependencies

requires "nim >= 2.0.0"
requires "lockfree >= 0.6.6 & < 0.7.0" # WIP pin: https://github.com/elijahr/lockfree#feat/headerkit-roundtable-v2
requires "typestates >= 0.14.0 & < 0.15.0"
requires "naganim >= 0.1.0" # WIP pin: https://github.com/elijahr/naganim#feat/headerkit-roundtable-v2

task build_ext, "Build liblockless_ext shared library for Python":
  let ext =
    when defined(macosx):
      "dylib"
    elif defined(windows):
      "dll"
    else:
      "so"
  exec "nim c --app:lib --noMain --mm:orc --threads:on -d:useMalloc -d:noSignalHandler -d:release --out:python/lockless/liblockless_ext." & ext & " src/lockless_ext.nim"

