# require python3
# -*- coding: utf-8 -*-

"""
Version of the core, MAJOR.MINOR.PATCH.
MAJOR.MINOR is shared with automato-core-js (src/automato/core/version.js): the two cores with the same MAJOR.MINOR do the same things
(same calls, same wire behavior). Change it in both when the shared API or the protocol changes (MAJOR for incompatible changes).
PATCH is per implementation: fixes and internal changes of this core only.
"""

VERSION = '1.0.0'

def api_level(version = VERSION):
  """
  The shared part of a version, "MAJOR.MINOR" (None if the version is not valid)
  """
  parts = str(version).split('.') if version else []
  return '.'.join(parts[:2]) if len(parts) >= 2 else None
