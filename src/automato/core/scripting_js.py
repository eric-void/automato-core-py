# require python3
# -*- coding: utf-8 -*-

"""
Evaluation of the JavaScript snippets of definitions (js:/jsf:), with one of two engines:
- "js2py" (default): scripting_js2py, Js2Py (pure Python, ES5; the PyPI release stops at Python 3.11, a fork is needed up to 3.13)
- "quickjs": scripting_quickjs, QuickJS through the quickjs-ng package (C extension, ES2023)

The engine is chosen by the "scripting-engine" key of the configuration (read by system.boot), or by the AUTOMATO_SCRIPTING_ENGINE
environment variable when the configuration does not set it. If the chosen engine cannot be loaded the other one is used (with an error
in the log), so a node always starts.

Only the selected engine is imported. Other modules use this one (script_eval, script_exec, exports); engine attributes (stats as
script_js_compiled_hits, ...) are read through it.
"""

import importlib
import logging
import os

ENGINES = {
  'js2py': 'automato.core.scripting_js2py',
  'quickjs': 'automato.core.scripting_quickjs',
}
DEFAULT_ENGINE = 'js2py'

exports = {}

engine = None
_engine_module = None

def default_engine():
  return os.environ.get('AUTOMATO_SCRIPTING_ENGINE') or DEFAULT_ENGINE

def set_engine(name = None):
  """
  Select the engine (None = AUTOMATO_SCRIPTING_ENGINE or the default). Returns the name of the engine actually loaded.
  """
  global engine, _engine_module
  name = name or default_engine()
  candidates = [name] + [e for e in ENGINES if e != name]
  if name not in ENGINES:
    logging.error('scripting_js> unknown scripting engine "{name}" (valid: {valid}), using "{default}"'.format(name = name, valid = ', '.join(ENGINES), default = DEFAULT_ENGINE))
    candidates = [DEFAULT_ENGINE] + [e for e in ENGINES if e != DEFAULT_ENGINE]
  error = None
  for e in candidates:
    if e not in ENGINES:
      continue
    try:
      module = importlib.import_module(ENGINES[e])
    except Exception as ex:
      logging.exception('scripting_js> cannot load scripting engine "{engine}"'.format(engine = e))
      error = error or ex
      continue
    if e != name and name in ENGINES:
      logging.error('scripting_js> scripting engine "{name}" not available, using "{engine}"'.format(name = name, engine = e))
    module.exports = exports
    engine = e
    _engine_module = module
    logging.info('scripting_js> scripting engine: {engine}'.format(engine = e))
    return e
  raise error

def _get():
  if _engine_module is None:
    set_engine()
  if _engine_module.exports is not exports:
    # system._reset() replaces the exports dict
    _engine_module.exports = exports
  return _engine_module

def script_eval(code, context = {}, to_dict = False, cache = False):
  """
  Evaluate an expression ("js:...", or a function body with "jsf:...") with the variables of context. Returns the result as python
  values (dict, list, str, ...), None on errors (logged).
  """
  return _get().script_eval(code, context, to_dict, cache)

def script_exec(code, context = {}, return_context = True):
  """
  Execute a script with the variables of context. Returns the context variables (after the script: it can change them), or None on errors.
  @param return_context True to return all the context variables, ['name', ...] to return only some of them
  """
  return _get().script_exec(code, context, return_context)

def __getattr__(name):
  # Attributes of the selected engine (ex: script_js_compiled_hits, used by system stats)
  if name.startswith('__'):
    raise AttributeError(name)
  return getattr(_get(), name)
