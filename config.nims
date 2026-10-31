--mm:orc
--threads:on
--define:useMalloc
--define:noSignalHandler

import std/os

let siblingLockfree = thisDir() / ".." / "lockfree" / "src"
let siblingNaganim = thisDir() / ".." / "naganim" / "src"
let siblingDebra = thisDir() / ".." / "nim-debra" / "src"
let siblingTypestates = thisDir() / ".." / "typestates" / "src"

let devRoot = thisDir() / ".." / ".." / ".." / ".."
let devLockfree = devRoot / "lockfree" / "src"
let devNaganim = devRoot / "naganim" / "src"
let devDebra = devRoot / "nim-debra" / "src"
let devTypestates = devRoot / "typestates" / "src"

if dirExists(siblingNaganim):
  switch("path", siblingNaganim)
elif dirExists(devNaganim):
  switch("path", devNaganim)

if dirExists(siblingLockfree):
  switch("path", siblingLockfree)
elif dirExists(devLockfree):
  switch("path", devLockfree)

if dirExists(siblingDebra):
  switch("path", siblingDebra)
elif dirExists(devDebra):
  switch("path", devDebra)

if dirExists(siblingTypestates):
  switch("path", siblingTypestates)
elif dirExists(devTypestates):
  switch("path", devTypestates)

when defined(macosx):
  switch("passL", "-Wl,-undefined,dynamic_lookup")
