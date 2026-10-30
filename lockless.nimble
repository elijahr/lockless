# Package

version       = "0.1.0"
author        = "Axiomantic"
description   = "C-ABI extension backing the lockless Python concurrency & DSP package"
license       = "MIT"
srcDir        = "src"

# Dependencies

requires "nim >= 2.0.0"
requires "lockfree >= 0.6.5 & < 0.7.0"
requires "typestates >= 0.14.0 & < 0.15.0"
