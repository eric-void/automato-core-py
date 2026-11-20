# require python3
# -*- coding: utf-8 -*-

"""
QuickJS engine of scripting_js ("scripting-engine": "quickjs"): use it through automato.core.scripting_js.
It needs the quickjs-ng package (C extension: on platforms without a PyPI wheel, as OpenWrt armv7, see automato-node-py/bin/build-quickjs-wheel.sh).

It gives the same results of the Js2Py engine (scripting_js2py) on the snippets of definitions:
- values cross the Python <-> JS boundary as primitives (bool, int, float, str, None) or as JSON (dict, list)
- python None is JS undefined (as in Js2Py), for context variables and results of python functions
- object properties set to undefined are returned (as None), NaN and Infinity are kept
- integral numbers are returned as int
Differences (Js2Py is the wrong one, QuickJS behaves as browsers): python helpers receive python values (with Js2Py d() and
strftime() with one argument do not work), ES2023 syntax is supported, exports values are updated at every call (Js2Py keeps the
values of when the exports keys last changed).
There is no time limit (as with Js2Py): quickjs-ng does not allow calls to python functions when a time limit is set.

A QuickJS runtime must not be used by several threads, and musl threads have a small default stack: all the JS code runs on a single
dedicated thread (with an 8 MB stack), calls from other threads wait for it.
"""

import logging
import threading
import concurrent.futures
import json
import math
import random
import string

import quickjs

from automato.core import system
from automato.core import utils

exports = {}

script_js_compiled = {}
script_js_compiled_hits = 0
script_js_compiled_miss = 0
SCRIPT_JS_COMPILED_MAXSIZE = 1000
SCRIPT_JS_COMPILED_PURGETIME = 3600

_THREAD_STACK_SIZE = 8 * 1024 * 1024
_executor = None
_executor_lock = threading.Lock()
_worker_ident = None

_ctx = None
_ctx_exports_keys = None
_ctx_context_keys = None
_helpers = {}

def _run(fn, *args):
  global _executor, _worker_ident
  if threading.get_ident() == _worker_ident:
    # Called by a python helper during a script
    return fn(*args)
  if _executor is None:
    with _executor_lock:
      if _executor is None:
        old = threading.stack_size()
        try:
          threading.stack_size(_THREAD_STACK_SIZE)
          ex = concurrent.futures.ThreadPoolExecutor(max_workers = 1, thread_name_prefix = 'scripting_quickjs')
          ident = ex.submit(threading.get_ident).result()
        finally:
          threading.stack_size(old)
        _worker_ident = ident
        _executor = ex
  return _executor.submit(fn, *args).result()

_NUM = '__automato_num__'
_HELPERS_JS = """
globalThis.__pyenc = function(v) {
  return JSON.stringify(v === undefined ? null : v, function(k, x) {
    if (x === undefined || typeof x === 'function') return null;
    if (typeof x === 'number' && !isFinite(x)) return {'""" + _NUM + """': String(x)};
    return x;
  });
};
globalThis.__pydec = function(s) {
  return JSON.parse(s, function(k, x) {
    return (x !== null && typeof x === 'object' && '""" + _NUM + """' in x) ? Number(x['""" + _NUM + """']) : x;
  });
};
globalThis.__pyset_undefined = function(k) { globalThis[k] = undefined; };
globalThis.__pywrap = function(f) { return function() { var r = f.apply(null, arguments); return r === null ? undefined : r; }; };
"""

def _json_num_encode(v):
  if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
    return {_NUM: 'NaN' if math.isnan(v) else ('Infinity' if v > 0 else '-Infinity')}
  if isinstance(v, dict):
    return {str(k): _json_num_encode(x) for k, x in v.items()}
  if isinstance(v, (list, tuple)):
    return [_json_num_encode(x) for x in v]
  return v

def _json_num_decode(d):
  if len(d) == 1 and _NUM in d:
    return float(d[_NUM].replace('Infinity', 'inf'))
  return d

def _to_js(v):
  if v is None or isinstance(v, (bool, int, float, str)):
    return v
  if isinstance(v, quickjs.Object):
    return v
  if isinstance(v, (dict, list, tuple)):
    try:
      return _ctx.parse_json(json.dumps(v, allow_nan = False, default = str))
    except ValueError:
      return _helpers['dec'](json.dumps(_json_num_encode(v), default = str))
  if isinstance(v, (bytes, bytearray)):
    return v.decode('utf-8', errors = 'replace')
  return str(v)

def _num_to_py(v):
  if isinstance(v, float) and v.is_integer() and abs(v) < 2 ** 53:
    return int(v)
  return v

def _to_py(v):
  if isinstance(v, quickjs.Object):
    return json.loads(_helpers['enc'](v), object_hook = _json_num_decode, parse_float = lambda s: _num_to_py(float(s)))
  return _num_to_py(v)

def _wrap_callable(fn):
  def wrapped(*args):
    return _to_js(fn(*[_to_py(a) for a in args]))
  return wrapped

def _base_globals():
  return {
    'd': utils.read_duration,
    't': _parse_datetime,
    'uniqid': _uniqid,
    'strftime': _strftime,
    'array_sum': utils.array_sum,
    'array_avg': utils.array_avg,
    'array_min': utils.array_min,
    'array_max': utils.array_max,
    'round': round,
    'is_dict': _is_dict,
    'is_array': _is_array,
    'print': _print,
    'str': str,
    'camel_to_snake_case': _camel_to_snake_case,
    'payload_transfer': _payload_transfer,
    '_': _translate,
  }

def _set_var(k, v):
  if v is None:
    _helpers['set_undefined'](k)
  else:
    _ctx.set(k, _to_js(v))

def _set_global(k, v):
  if callable(v) and not isinstance(v, quickjs.Object):
    _ctx.add_callable('__py_' + k, _wrap_callable(v))
    _ctx.eval('globalThis[' + json.dumps(k) + '] = __pywrap(globalThis[' + json.dumps('__py_' + k) + ']);')
  else:
    _set_var(k, v)

def _prepare_context(context):
  global _ctx, _ctx_exports_keys, _ctx_context_keys, script_js_compiled
  if _ctx is None or _ctx_exports_keys != list(exports.keys()):
    logging.debug("scripting_quickjs> Inizializing new script context")
    _ctx = quickjs.Context()
    _ctx.eval(_HELPERS_JS)
    _helpers['enc'] = _ctx.get('__pyenc')
    _helpers['dec'] = _ctx.get('__pydec')
    _helpers['set_undefined'] = _ctx.get('__pyset_undefined')
    for k, v in {** _base_globals(), ** exports}.items():
      _set_global(k, v)
    _ctx_exports_keys = list(exports.keys())
    _ctx_context_keys = None
    script_js_compiled = {}
  else:
    # exports values can change over time (ex: astral "is_day"), functions are already registered
    for k, v in exports.items():
      if not callable(v):
        _set_var(k, v)

  if _ctx_context_keys:
    for k in _ctx_context_keys:
      if k not in context:
        _set_var(k, None)
  for k in context:
    _set_var(k, context[k])
  _ctx_context_keys = list(context.keys())
  _ctx.set('now', system.time())

def script_eval(code, context = {}, to_dict = False, cache = False):
  """
  @param to_dict, cache Not used (results are always python values, there is no result cache)
  """
  _s = system._stats_start()
  if code.startswith('js:'):
    ret = _script_exec_js(code[3:], context, do_eval = True)
  elif code.startswith('jsf:'):
    ret = _script_exec_js(code[4:], context, do_eval = True, do_eval_function = True)
  else:
    ret = _script_exec_js(code, context, do_eval = True)
  system._stats_end('scripting_js.script_eval', _s)
  return ret['return'] if 'return' in ret else None

def script_exec(code, context = {}, return_context = True):
  """
  @param return_context Used to access modified context variables (context passed could be NOT modified by script). Use True to return all context variables, ['name', ...] to return only variables referenced, False to return no variables
  @return modified context variables if return_context set
  """
  if code.startswith('js:'):
    code = code[3:]
  ret = _script_exec_js(code, context, do_eval = False, return_context = return_context)
  return ret['context'] if not ret['error'] else None

def _script_exec_js(code, context = {}, do_eval = True, do_eval_function = False, return_context = False):
  """
  @return { 'error': boolean, 'return': evalued expression if do_eval = True, 'context': modificed context variables if return_context set }
  """
  _s = system._stats_start()
  try:
    return _run(_script_exec_js_worker, code, context, do_eval, do_eval_function, return_context)
  except Exception:
    logging.exception('scripting_js> error executing js script: {code}\ncontext: {context}\n'.format(code = code, context = str(context)))
    return {'error': True, 'return': None, 'context': {}}
  finally:
    system._stats_end('scripting_js.script_' + ('eval' if do_eval else 'exec')+ '(quickjs)', _s)

def _script_exec_js_worker(code, context, do_eval, do_eval_function, return_context):
  global script_js_compiled, script_js_compiled_hits, script_js_compiled_miss
  context = context if isinstance(context, dict) else {}
  _prepare_context(context)

  key = ('f:' if do_eval_function else ('e:' if do_eval else 'x:')) + code
  if key in script_js_compiled:
    script_js_compiled_hits += 1
  else:
    script_js_compiled_miss += 1
    if len(script_js_compiled) > SCRIPT_JS_COMPILED_MAXSIZE:
      t = SCRIPT_JS_COMPILED_PURGETIME
      while len(script_js_compiled) > SCRIPT_JS_COMPILED_MAXSIZE * 0.9:
        script_js_compiled = {x:script_js_compiled[x] for x in script_js_compiled if script_js_compiled[x]['used'] > system.time() - t}
        t = t / 2 if t > 1 else -1
    src = code
    if do_eval:
      # Newline before "}": a snippet can end with a // comment
      src = ('PyJsEvalResult = ' + code) if not do_eval_function else ('PyJsEvalResult = function() {' + code + '\n}()')
    script_js_compiled[key] = {'compiled': _ctx.compile(src)}
  compiled = script_js_compiled[key]
  compiled['used'] = system.time()

  ret = {'error': False, 'return': None, 'context': {}}
  _ctx.eval_bytecode(compiled['compiled'])
  if do_eval:
    ret['return'] = _to_py(_ctx.get('PyJsEvalResult'))
  if return_context:
    for k in return_context if isinstance(return_context, list) else context.keys():
      ret['context'][k] = _to_py(_ctx.get(k))
  return ret

def _var_to_python(v):
  return _to_py(v)

def _parse_datetime(v):
  return utils.parse_datetime(v)

def _strftime(timestamp, tformat = '%Y-%m-%d %H:%M:%S'):
  return utils.strftime(timestamp, tformat)

def _translate(v):
  return v

def _print(v):
  print(str(v))

def _is_dict(v):
  return isinstance(v, dict)

def _is_array(v):
  return isinstance(v, list)

def _camel_to_snake_case(v):
  return utils.camel_to_snake_case(v)

uniqid_seed = ''.join(random.choice(string.ascii_lowercase) for m in range(5))
uniqid_c = 0

def _uniqid():
  global uniqid_seed, uniqid_c
  uniqid_c = uniqid_c + 1
  return system.default_node_name + ':' + uniqid_seed + ':' + str(uniqid_c)

def _payload_transfer(base_result, payload, keys, empty_result = None):
  result = {}
  if isinstance(keys, list):
    for k in keys:
      if k in payload:
        result[k] = payload[k]
  elif isinstance(keys, dict):
    for k in keys:
      if k in payload:
        result[keys[k] if keys[k] else k] = payload[k]
  return {** base_result, ** result} if result else empty_result
