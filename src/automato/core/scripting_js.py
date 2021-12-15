# require python3
# -*- coding: utf-8 -*-

# TODO Impostare anche gli "exports"

import logging
import js2py
import threading
import re
import json

from automato.core import system
from automato.core import utils

# WARN: Un eval di questo tipo: "js: false ? do() : null" eseguirà "do()" comunque (anche se poi il valore di ritorno non verrà preso come return della funziona). Tenere in considerazione la cosa.
# Si evita facendo: "js: if (false) r = do(); r"

# Enable "use_compilation_plan", an undocumented feature (20190626: NOT WORKING)
# @see https://github.com/PiotrDabkowski/Js2Py/blob/master/js2py/evaljs.py - https://github.com/PiotrDabkowski/Js2Py/blob/master/js2py/translators/translator.py
js2py_use_compilation_plan = False

script_eval_cache = {}
script_eval_cache_lock = threading.Lock()
script_eval_cache_hits = 0
script_eval_cache_miss = 0
script_eval_cache_disabled = 0
script_eval_cache_skipped = 0
script_eval_codecontext_signatures = {}
SCRIPT_EVAL_CACHE_MAXSIZE = 1024
SCRIPT_EVAL_CACHE_PURGETIME = 3600

#script_eval_quick_count = 0

exports = {}

def script_context(context = {}):
  if isinstance(context, js2py.evaljs.EvalJs):
    return context
    #context = context.__context
  #if isinstance(context, js2py.base.JsObjectWrapper):
  #  context = context.to_dict()
  
  c = js2py.EvalJs({
    'now': system.time(),
    'd': utils.read_duration,
    't': _parse_datetime,
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
    '_': _translate,
    
    ** exports,
    ** context
  })
  c.__context = context
  return c

"""
def script_eval(code, context = {}, to_dict = False):
  # TODO Supporto per altri linguaggi
  if code.startswith('js:'):
    code = code[3:]
  contextjs = script_context(context)
  try:
    ret = _var_to_python(contextjs.eval(code, use_compilation_plan = js2py_use_compilation_plan))
    if to_dict and ret and isinstance(ret, js2py.base.JsObjectWrapper):
      ret = ret.to_dict()
    return ret
  except:
    logging.exception('error evaluating js script: \n' + code + '\n context: ' + str(context) + '\n')
"""

def script_eval(code, context = {}, to_dict = False, cache = False):
  #global script_eval_quick_count
  _s = system._stats_start()
  """
  ret = _script_eval_quick(code, context)
  if ret and 'return' in ret:
    script_eval_quick_count = script_eval_quick_count + 1
    ret = ret['return']
  else:
  """
  ret = _script_eval_int(code, context, cache)
  if ret and to_dict and isinstance(ret, js2py.base.JsObjectWrapper):
    ret = ret.to_dict()

  system._stats_end('scripting_js.script_eval', _s)
  return ret

def _script_eval_int(code, context = {}, cache = False):
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
      
      """
      contextkey = {}
      for v in context_sorted:
        if isinstance(context[v], dict):
          contextkey[v] = {}
          for vv in context[v]:
            contextkey[v][vv] = ''
        else:
          contextkey[v] = ''
      """
      # CONTEXT: part contains the first and second-level keys of context object, in the form { key: '', key: { dictkey: ''}}
      codecontext_signature = "CODE:" + code + ",CONTEXT:" + str({x: ('' if not isinstance(context[x], dict) else {y: '' for y in sorted(context[x])}) for x in context_sorted})
      
      with script_eval_cache_lock:
        if not codecontext_signature in script_eval_codecontext_signatures:
          """
          script_eval_codecontext_signatures[codecontext_signature] = {}
          for v in context_sorted:
            if re.search(r'\b' + v + r'\b', code):
              if isinstance(context[v], dict):
                script_eval_codecontext_signatures[codecontext_signature][v] = {}
                for vv in context[v]:
                  if re.search(r'\b' + vv + r'\b', code):
                    script_eval_codecontext_signatures[codecontext_signature][v][vv] = ''
              else:
                script_eval_codecontext_signatures[codecontext_signature][v] = ''
          """
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
  
  # TODO Supporto per altri linguaggi
  if code.startswith('js:'):
    code = code[3:]

  ret = _script_exec_js(code, context, do_eval = True)
  if cache and not ret['error']:
    with script_eval_cache_lock:
      script_eval_cache[keyhash] = { 'key': key, 'used': system.time(), 'result': ret['return'] }
  return ret['return'] if 'return' in ret else None
  
def _script_code_uses_full_var(code, var):
  """
  Return if code uses the var, without dict key reference ("payload[x]" or "x in payload" uses key reference, "payload" not)
  """
  #return re.search(r'\b' + var + r'(\.|\[)', code)
  parts = re.split(r'\b' + var + r'\b', code)
  for i in range(0, len(parts) - 1):
    if not re.search(r'\b(typeof|in)\s*$', parts[i]) and not re.search(r'^(\.|\[)', parts[i + 1]):
      return True
  return False

def script_exec(code, context = {}):
  # TODO Supporto per altri linguaggi
  if code.startswith('js:'):
    code = code[3:]
  _script_exec_js(code, context, do_eval = False)


script_js_compiled = {}
script_js_compiled_lock = threading.Lock()
script_js_compiled_hits = 0
script_js_compiled_miss = 0
SCRIPT_JS_COMPILED_MAXSIZE = 1000
SCRIPT_JS_COMPILED_PURGETIME = 3600

def _script_exec_js(code, context = {}, do_eval = True):
  global script_js_compiled, script_js_compiled_hits, script_js_compiled_miss, script_js_compiled_lock
  
  contextjs = script_context(context)
  _s = system._stats_start()
  try:
    #ret = _var_to_python(contextjs.eval(code, use_compilation_plan = js2py_use_compilation_plan))
    # @see https://github.com/PiotrDabkowski/Js2Py/blob/b16d7ce90ac9c03358010c1599c3e87698c9993f/js2py/evaljs.py#L174 (execute method)
    
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
        code = 'PyJsEvalResult = eval(%s)' % json.dumps(code)
      code = js2py.translators.translate_js(code, '', use_compilation_plan=js2py_use_compilation_plan)
      script_js_compiled[keyhash] = {'compiled': compile(code, '<EvalJS snippet>', 'exec')}
      
    script_js_compiled[keyhash]['used'] = system.time()
    exec(script_js_compiled[keyhash]['compiled'], contextjs._context)
    #exec(code, contextjs._context)
    
    ret = _var_to_python(contextjs['PyJsEvalResult']) if do_eval else None
    return {'return': ret, 'error': False}
  except:
    cdebug = {}
    for k in contextjs.__context:
      cdebug[k] = contextjs[k]
    logging.exception('scripting_js> error executing js script: {code}\ncontext: {context}\ncontextjs: {contextjs}\n'.format(code = code, context = str(context if not isinstance(context, js2py.evaljs.EvalJs) else (str(context.__context) + ' (WARN! this is the source context, but changes could have been made before this call, because a result of another call has been passed!)')), contextjs = cdebug))
    return {'error': True}
  finally:
    system._stats_end('scripting_js.script_' + ('eval' if do_eval else 'exec')+ '(js2py)', _s)

def _var_to_python(v):
  if isinstance(v, js2py.base.PyJs):
    v = v.to_python()
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

def _parse_int(v):
  try:
    return float(v)
  except ValueError:
    return None

"""
def _script_eval_quick(code, context):
  if code == '({value: payload})':
    return {'return': {'value': context['payload']}}
  for k in ['hp', 'day_plugs1', 'night_plugs', 'basement_light', 'basement_plugs', 'external', 'irrigation_pump', 'basement_pump', 'hp_heating', 'hp_dhw', 'other']:
    if code == "js:('" + k + "' in payload ? {port: '" + k + "', energy: parseFloat(payload['" + k + "']['energy_result']), power: parseInt(payload['" + k + "']['power_last'])} : null)":
      return {'return': {'port': k, 'energy': _parse_float(context['payload'][k]['energy_result']), 'power': _parse_int(context['payload'][k]['power_last'])} if k in context['payload'] else None }
  if code == 'js:({power: parseFloat(payload), port: matches[1] ? matches[2] : "0"})':
    return {'return': { 'power': _parse_float(context['payload']), 'port': context["matches"][2] if context["matches"][1] else "0" }}
  if code == 'js:({energy: parseFloat(payload) / 60000, energy_reported: parseFloat(payload), port: matches[1] ? matches[2] : "0"})':
    return {'return': { 'energy': _parse_float(context['payload']) / 60000, 'energy_reported': _parse_float(context['payload']), 'port': context["matches"][2] if context["matches"][1] else "0" }}
  if code == 'js:({energy: parseFloat(payload) / 1000, energy_reported: parseFloat(payload), port: matches[1] ? matches[2] : "0"})':
    return {'return': { 'energy': _parse_float(context['payload']) / 1000, 'energy_reported': _parse_float(context['payload']), 'port': context["matches"][2] if context["matches"][1] else "0" }}
  if code == 'js:({energy_returned: parseFloat(payload) / 1000, energy_returned_reported: parseFloat(payload), port: matches[1] ? matches[2] : "0"})':
    return {'return': { 'energy_returned': _parse_float(context['payload']) / 1000, 'energy_returned_reported': _parse_float(context['payload']), 'port': context["matches"][2] if context["matches"][1] else "0" }}
  if code == 'js:payload == "on" ? ({value: 1, port: matches[1]}) : (payload == "off" || payload == "overpower" ? ({value: 0, port: matches[1]}) : false)':
    return {'return': {'value': 1, 'port': context['matches'][1]} if context['payload'] == 'on' else ({'value': 0, 'port': context['matches'][1]} if context['payload'] == 'off' or context['payload'] == 'overpower' else False)}
"""
