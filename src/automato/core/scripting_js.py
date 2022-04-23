# require python3
# -*- coding: utf-8 -*-

# NOTE script_eval_cache DISABLED because it was not working property:
# - not working properly with some payload values - probably float ones - the same event was emitted with different payloads
# - script_js_compiled optimizations works much better, and it seems there is no need for a cache layer right now
# TODO Impostare anche gli "exports"

import logging
import js2py
import threading
import re
import json
import random
import string

from automato.core import system
from automato.core import utils

# WARN: Un eval di questo tipo: "js: false ? do() : null" eseguirà "do()" comunque (anche se poi il valore di ritorno non verrà preso come return della funziona). Tenere in considerazione la cosa.
# Si evita facendo: "jsf: if (false) r = do(); return r"

# Enable "use_compilation_plan", an undocumented feature (20190626: NOT WORKING)
# @see https://github.com/PiotrDabkowski/Js2Py/blob/master/js2py/evaljs.py - https://github.com/PiotrDabkowski/Js2Py/blob/master/js2py/translators/translator.py
js2py_use_compilation_plan = False

"""
NOTE script_eval_cache DISABLED (not working properly, and new optimization to script evaluating are much better

script_eval_cache = {}
script_eval_cache_lock = threading.Lock()
script_eval_cache_hits = 0
script_eval_cache_miss = 0
script_eval_cache_disabled = 0
script_eval_cache_skipped = 0
script_eval_codecontext_signatures = {}
SCRIPT_EVAL_CACHE_MAXSIZE = 1024
SCRIPT_EVAL_CACHE_PURGETIME = 3600
"""
script_context_instance = None
script_context_instance_context_keys = None
script_context_instance_exports_keys = None
script_context_instance_lock = threading.Lock()
script_js_compiled = {}
script_js_compiled_lock = threading.Lock()
script_js_compiled_hits = 0
script_js_compiled_miss = 0
SCRIPT_JS_COMPILED_MAXSIZE = 1000
SCRIPT_JS_COMPILED_PURGETIME = 3600

exports = {}

def script_context(context = {}):
  global script_context_instance, script_context_instance_context_keys, script_context_instance_exports_keys
  if not script_context_instance or script_context_instance_exports_keys != list(exports.keys()):
    logging.debug("scripting> Inizializing new script context")
    script_context_instance = js2py.EvalJs({
      'now': system.time(),
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
      
      ** exports,
    })
    
    script_context_instance.__context = None
    script_context_instance_context_keys = None
    script_context_instance_exports_keys = list(exports.keys())

  if isinstance(context, js2py.evaljs.EvalJs):
    context = context.__context
  if isinstance(context, js2py.base.JsObjectWrapper):
    context = context.to_dict()
  if script_context_instance_context_keys:
    for k in script_context_instance_context_keys:
      if k not in context:
        script_context_instance._context['var'][k] = None
  for k in context:
    script_context_instance._context['var'][k] = context[k]

  script_context_instance.__context = context
  script_context_instance_context_keys = list(context.keys())
  
  script_context_instance._context['var']['now'] = system.time()

  return script_context_instance

def script_eval(code, context = {}, to_dict = False, cache = False):
  """
  @param cache Code excecution can be cached (this parameter is NOT used right now)
  """
  
  _s = system._stats_start()
  ret = _script_eval_int(code, context, cache)
  if ret and to_dict and isinstance(ret, js2py.base.JsObjectWrapper):
    ret = ret.to_dict()

  system._stats_end('scripting_js.script_eval', _s)
  return ret

def _script_eval_int(code, context = {}, cache = False):
  """
  NOTE script_eval_cache DISABLED (not working properly, and new optimization to script evaluating are much better
  
  global script_eval_cache, script_eval_cache_lock, script_eval_cache_hits, script_eval_cache_miss, script_eval_cache_skipped, script_eval_cache_disabled, script_eval_codecontext_signatures
  
  # TODO UNSUPPORTED: If context is the result of another exec/eval call, i must extract the real context from it. I'm skipping it right now
  # NOTE FUTURE: context._var._obj.own è un oggetto di tipo Scope (vedi https://github.com/PiotrDabkowski/Js2Py/blob/master/js2py/base.py#L1066) che contiene TUTTO l'ambiente js (quindi le variabili di context, ma anche tutto il resto)
  # Es: context._var._obj.own.keys() mi da tutti i nomi. Da li posso estrarre il context aggiornato
  if cache:
    if len(script_eval_cache) > SCRIPT_EVAL_CACHE_MAXSIZE:
      with script_eval_cache_lock:
        t = SCRIPT_EVAL_CACHE_PURGETIME
        while len(script_eval_cache) > SCRIPT_EVAL_CACHE_MAXSIZE:
          script_eval_cache = {x:script_eval_cache[x] for x in script_eval_cache if script_eval_cache[x]['used'] > system.time() - t}
          t = t / 2 if t > 1 else -1

    if isinstance(context, js2py.evaljs.EvalJs):
      cache = False
    # TODO i can skip context with elements not str|int|bool, but i should consider also dict of them (and dict of dict? and lists?). At the moment i cache everything, and see what happens (probabily a lot of uncacheable contents will be cached, but they will be deleted by cache purge...)
    #else:
    #  for x in context:
    #    if not (isinstance(context[x], str) or isinstance(context[x], int) or isinstance(context[x], bool)):
    #      cache = False
    #      break
    
    if cache:
      context_sorted = sorted(context)
      
      # CONTEXT: part contains the first and second-level keys of context object, in the form { key: '', key: { dictkey: ''}}
      codecontext_signature = "CODE:" + code + ",CONTEXT:" + str({x: ('' if not isinstance(context[x], dict) else {y: '' for y in sorted(context[x])}) for x in context_sorted})
      
      with script_eval_cache_lock:
        if not codecontext_signature in script_eval_codecontext_signatures:
          # This struct contains the usage of context keys (first and second level) in the code. If a key is present, with value '', that key is used in the code as is. If not present, it's not used. If it's a dict, it reflects the usage of subkeys.
          script_eval_codecontext_signatures[codecontext_signature] = { x: ('' if not isinstance(context[x], dict) or _script_code_uses_full_var(code, x) else {y: '' for y in sorted(context[x]) if re.search(r'\b' + y + r'\b', code) }) for x in context_sorted if re.search(r'\b' + x + r'\b', code) }

        #OBSOLETE: key = "CONTEXT:" + str({x:context[x] for x in context_sorted}) + ",CODE:" + code
        key = "CONTEXT:" + str({x: (context[x] if script_eval_codecontext_signatures[codecontext_signature][x] == '' else {y: context[x][y] for y in sorted(context[x]) if y in script_eval_codecontext_signatures[codecontext_signature][x]}) for x in context_sorted if x in script_eval_codecontext_signatures[codecontext_signature]}) + ",CODE:" + code
      
      keyhash = utils.md5_hexdigest(key)
      with script_eval_cache_lock:
        if keyhash in script_eval_cache and script_eval_cache[keyhash]['key'] == key:
          script_eval_cache[keyhash]['used'] = system.time()
          script_eval_cache_hits += 1
          return script_eval_cache[keyhash]['result']
      script_eval_cache_miss += 1
      
      #logging.debug("scripting> SCRIPT EVAL CACHE MISSED: " + key)
      
    else:
      script_eval_cache_skipped += 1
  else:
    script_eval_cache_disabled += 1
  """
  
  # TODO Supporto per altri linguaggi
  if code.startswith('js:'):
    ret = _script_exec_js(code[3:], context, do_eval = True)
  elif code.startswith('jsf:'):
    ret = _script_exec_js(code[4:], context, do_eval = True, do_eval_function = True)
  else:
    ret = _script_exec_js(code, context, do_eval = True)
    
  """
  NOTE script_eval_cache DISABLED (not working properly, and new optimization to script evaluating are much better
  
  if cache and not ret['error']:
    with script_eval_cache_lock:
      script_eval_cache[keyhash] = { 'key': key, 'used': system.time(), 'result': ret['return'] }
  """
  return ret['return'] if 'return' in ret else None
  
"""
NOTE script_eval_cache DISABLED (not working properly, and new optimization to script evaluating are much better

def _script_code_uses_full_var(code, var):
  # Return if code uses the var, without dict key reference ("payload[x]" or "x in payload" uses key reference, "payload" not)
  parts = re.split(r'\b' + var + r'\b', code)
  for i in range(0, len(parts) - 1):
    if not re.search(r'\b(typeof|in)\s*$', parts[i]) and not re.search(r'^(\.|\[)', parts[i + 1]):
      return True
  return False
"""

def script_exec(code, context = {}, return_context = True):
  """
  @param return_context Used to access modified context variables (context passed could be NOT modified by script). Use True to return all context variables, ['name', ...] to return only variables referenced, False to return no variables
  @return modified context variables if return_context set
  """
  # TODO Supporto per altri linguaggi
  if code.startswith('js:'):
    code = code[3:]
  ret = _script_exec_js(code, context, do_eval = False, return_context = return_context)
  return ret['context'] if not ret['error'] else None

def _script_exec_js(code, context = {}, do_eval = True, do_eval_function = False, return_context = False):
  """
  @param return_context Used to access modified context variables (context passed could be NOT modified by script). Use True to return all context variables, ['name', ...] to return only variables referenced, False to return no variables
  @return { 'error': boolean, 'return': evalued expression if do_eval = True, 'context': modificed context variables if return_context set }
  """
  # @see https://github.com/PiotrDabkowski/Js2Py/blob/b16d7ce90ac9c03358010c1599c3e87698c9993f/js2py/evaljs.py#L174 (execute method)
  global script_js_compiled, script_js_compiled_hits, script_js_compiled_miss, script_js_compiled_lock, script_context_instance_lock
  
  _s = system._stats_start()
  try:
    keyhash = utils.md5_hexdigest(code)
    if keyhash in script_js_compiled:
      script_js_compiled_hits += 1
    else:
      script_js_compiled_miss += 1
      
      if len(script_js_compiled) > SCRIPT_JS_COMPILED_MAXSIZE:
        with script_js_compiled_lock:
          t = SCRIPT_JS_COMPILED_PURGETIME
          while len(script_js_compiled) > SCRIPT_JS_COMPILED_MAXSIZE * 0.9:
            script_js_compiled = {x:script_js_compiled[x] for x in script_js_compiled if script_js_compiled[x]['used'] > system.time() - t}
            t = t / 2 if t > 1 else -1
            
      if do_eval:
        #code = 'PyJsEvalResult = eval(%s)' % json.dumps(code) # Metodo originale usato da js2py, molto lento
        if not do_eval_function:
          code = 'PyJsEvalResult = ' + code
        else:
          code = 'PyJsEvalResult = function() {' + code + '}()'

      code = js2py.translators.translate_js(code, '', use_compilation_plan=js2py_use_compilation_plan)
      
      script_js_compiled[keyhash] = {'compiled': compile(code, '<EvalJS snippet>', 'exec')}
    
    script_js_compiled[keyhash]['used'] = system.time()
    
    ret = {'error': False, 'return': None, 'context': {}}
    with script_context_instance_lock:
      contextjs = script_context(context)
      exec(script_js_compiled[keyhash]['compiled'], contextjs._context)
      if do_eval:
        ret['return'] = _var_to_python(contextjs['PyJsEvalResult'])
      if return_context:
        for k in return_context if isinstance(return_context, list) else ((context.__context.to_dict() if isinstance(context.__context, js2py.base.JsObjectWrapper) else context.__context) if isinstance(context, js2py.evaljs.EvalJs) else (context.to_dict() if isinstance(context, js2py.base.JsObjectWrapper) else context)).keys():
          ret['context'][k] = contextjs[k]

    return ret
  except:
    """
    cdebug = {}
    for k in contextjs.__context:
      cdebug[k] = contextjs[k]
    logging.exception('scripting_js> error executing js script: {code}\ncontext: {context}\ncontextjs: {contextjs}\n'.format(code = code, context = str(context if not isinstance(context, js2py.evaljs.EvalJs) else (str(context.__context) + ' (WARN! this is the source context, but changes could have been made before this call, because a result of another call has been passed!)')), contextjs = cdebug))
    """
    logging.exception('scripting_js> error executing js script: {code}\ncontext: {context}\n'.format(code = code, context = str(context if not isinstance(context, js2py.evaljs.EvalJs) else (str(context.__context) + ' (WARN! this is the source context, but changes could have been made before this call, because a result of another call has been passed!)'))))
    return {'error': True, 'return': None, 'context': {}}
  finally:
    system._stats_end('scripting_js.script_' + ('eval' if do_eval else 'exec')+ '(js2py)', _s)

def _var_to_python(v):
  if isinstance(v, js2py.base.PyJs):
    v = v.to_python()
  if isinstance(v, js2py.base.JsObjectWrapper):
    if v._obj.Class in ['Array', 'Int8Array', 'Uint8Array', 'Uint8ClampedArray', 'Int16Array', 'Uint16Array', 'Int32Array', 'Uint32Array', 'Float32Array', 'Float64Array']:
      v = v.to_list()
    else:
      v = v.to_dict()
  return v
  
def _parse_datetime(v):
  return utils.parse_datetime(_var_to_python(v))

def _strftime(timestamp, tformat = '%Y-%m-%d %H:%M:%S'):
  return utils.strftime(_var_to_python(timestamp), _var_to_python(tformat))

def _translate(v):
  return v

def _print(v):
  print(str(v))

def _is_dict(v):
  if isinstance(v, js2py.base.PyJsObject): # Only "PyJSObject" is correct, NOT PyJs(*)
    v = v.to_python()
  if isinstance(v, js2py.base.JsObjectWrapper):
    v = v.to_dict()
  return isinstance(v, dict)

def _is_array(v):
  return isinstance(v, js2py.base.PyJsArray) or isinstance(v, list)

def _camel_to_snake_case(v):
  return utils.camel_to_snake_case(_var_to_python(v))

def _parse_float(v):
  try:
    return float(v)
  except ValueError:
    return None

uniqid_seed = ''.join(random.choice(string.ascii_lowercase) for m in range(5))
uniqid_c = 0

def _uniqid():
  global uniqid_seed, uniqid_c
  uniqid_c = uniqid_c + 1
  return system.default_node_name + ':' + uniqid_seed + ':' + str(uniqid_c)

def _parse_int(v):
  try:
    return float(v)
  except ValueError:
    return None

def _payload_transfer(base_result, payload, keys, empty_result = None):
  base_result = _var_to_python(base_result)
  payload = _var_to_python(payload)
  keys = _var_to_python(keys)
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
