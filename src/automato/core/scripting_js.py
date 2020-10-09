# require python3
# -*- coding: utf-8 -*-

# TODO Impostare anche gli "exports"

import logging
import js2py
import hashlib
import threading

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
SCRIPT_EVAL_CACHE_MAXSIZE = 1024
SCRIPT_EVAL_CACHE_PURGETIME = 3600

exports = {}

def script_context(context = {}):
  if isinstance(context, js2py.evaljs.EvalJs):
    return context
  c = js2py.EvalJs({
    'now': system.time(),
    'd': utils.read_duration,
    't': _parse_datetime,
    'strftime': _strftime,
    'print': _print,
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
  ret = _script_eval_int(code, context, cache)
  if ret and to_dict and ret and isinstance(ret, js2py.base.JsObjectWrapper):
    ret = ret.to_dict()
  return ret

def _script_eval_int(code, context = {}, cache = False):
  global script_eval_cache, script_eval_cache_lock, script_eval_cache_hits, script_eval_cache_miss, script_eval_cache_skipped, script_eval_cache_disabled

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
      key = "CONTEXT:" + str({x:context[x] for x in sorted(context)}) + ",CODE:" + code
      keyhash = hashlib.md5(key.encode('utf-8')).hexdigest()
      with script_eval_cache_lock:
        if keyhash in script_eval_cache and script_eval_cache[keyhash]['key'] == key:
          script_eval_cache[keyhash]['used'] = system.time()
          script_eval_cache_hits += 1
          return script_eval_cache[keyhash]['result']
      script_eval_cache_miss += 1
      
      logging.debug("scripting> SCRIPT EVAL CACHE MISSED: " + key)
      
    else:
      script_eval_cache_skipped += 1
  else:
    script_eval_cache_disabled += 1
  
  # TODO Supporto per altri linguaggi
  if code.startswith('js:'):
    code = code[3:]
  contextjs = script_context(context)
  _s = system._stats_start()
  try:
    ret = _var_to_python(contextjs.eval(code, use_compilation_plan = js2py_use_compilation_plan))
    if cache:
      with script_eval_cache_lock:
        script_eval_cache[keyhash] = { 'key': key, 'used': system.time(), 'result': ret }
    return ret
  except:
    cdebug = {}
    for k in contextjs.__context:
      cdebug[k] = contextjs[k]
    logging.exception('scripting_js> error evaluating js script: {code}\ncontext: {context}\ncontextjs: {contextjs}\n'.format(code = code, context = str(context if not isinstance(context, js2py.evaljs.EvalJs) else (context.__context + ' (WARN! this is the source context, but changes could have been made before this call, because a result of another call has been passed!)')), contextjs = cdebug))
  finally:
    system._stats_end('scripting_js.script_eval', _s)
    

def script_exec(code, context = {}, to_dict = False):
  # TODO Supporto per altri linguaggi
  if code.startswith('js:'):
    code = code[3:]
  contextjs = script_context(context)
  _s = system._stats_start()
  try:
    return contextjs.execute(code, use_compilation_plan = js2py_use_compilation_plan)
  except:
    logging.exception('scripting_js> error executing js script: {code}\ncontext: {context}\n'.format(code = code, context = context))
  finally:
    system._stats_end('scripting_js.script_exec', _s)
    

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
