# require python3
# -*- coding: utf-8 -*-

"""
JavaScript snippets on both engines (js2py and quickjs): an engine not installed is skipped.
The other suites use the engine of AUTOMATO_SCRIPTING_ENGINE (default js2py): run them with AUTOMATO_SCRIPTING_ENGINE=quickjs too.
"""

import importlib
import logging
import math
import threading
import unittest

from automato.core import scripting_js, system

from helpers import CoreTestCase

def available(engine):
  try:
    importlib.import_module(scripting_js.ENGINES[engine])
    return True
  except Exception:
    return False

class EngineCases(object):
  """
  Cases that must give the same results on every engine
  """
  ENGINE = None

  def setUp(self):
    if not available(self.ENGINE):
      self.skipTest(self.ENGINE + ' not installed')
    logging.disable(logging.CRITICAL)
    self.assertEqual(scripting_js.set_engine(self.ENGINE), self.ENGINE)
    self.exports = scripting_js.exports
    scripting_js.exports = {}

  def tearDown(self):
    scripting_js.exports = self.exports
    scripting_js.set_engine()
    logging.disable(logging.NOTSET)

  def ev(self, code, **context):
    return scripting_js.script_eval(code, context)

  def test_expression(self):
    self.assertEqual(self.ev('js:1 + 2'), 3)
    self.assertEqual(self.ev("js:'a' + 'b'"), 'ab')
    self.assertIs(self.ev('js:1 > 2'), False)

  def test_object_result(self):
    self.assertEqual(self.ev("js:({ value: payload['x'] * 2, port: matches[1] })", payload = {'x': 3}, matches = ['t/a', 'a']), {'value': 6, 'port': 'a'})
    self.assertEqual(self.ev('js:[1, "a", [true, null]]'), [1, 'a', [True, None]])

  def test_function(self):
    code = "jsf: let params = payload ? {} : null; if (payload) params['pump_speed'] = parseFloat(payload); return params"
    self.assertEqual(self.ev(code, payload = '12.5'), {'pump_speed': 12.5})
    self.assertIsNone(self.ev(code, payload = ''))

  def test_integral_numbers_are_int(self):
    v = self.ev("js:params['value'] + 1", params = {'value': 1})
    self.assertEqual(v, 2)
    self.assertIsInstance(v, int)
    self.assertIsInstance(self.ev('js:({ v: 0.5 + 1.5 })')['v'], int)
    self.assertEqual(self.ev('js:0.5 + 0.25'), 0.75)

  def test_none_is_undefined(self):
    self.assertEqual(self.ev('js:typeof payload', payload = None), 'undefined')
    self.assertEqual(self.ev("js:payload + '1'", payload = None), 'undefined1')
    self.assertIsNone(self.ev("js:(typeof payload == 'object') && 'battery' in payload ? { value: payload['battery'] } : null", payload = None))

  def test_missing_context_variable_is_undefined(self):
    self.assertEqual(self.ev('js:a', a = 1), 1)
    self.assertEqual(self.ev('js:typeof a'), 'undefined')

  def test_undefined_properties_are_kept(self):
    self.assertEqual(self.ev("js:({ a: payload['nope'], b: 1 })", payload = {}), {'a': None, 'b': 1})

  def test_nan(self):
    self.assertTrue(math.isnan(self.ev("js:({ v: parseFloat('x') })")['v']))
    self.assertTrue(math.isnan(self.ev("js:parseFloat('x')")))
    self.assertTrue(math.isnan(self.ev("js:payload['v']", payload = {'v': float('nan')})))

  def test_exec_changes_context(self):
    # Js2Py returns its own objects (system passes them to script_eval as context): compare python values
    def ex(code, context, return_context = True):
      ret = scripting_js.script_exec(code, context, return_context)
      return {k: scripting_js._var_to_python(v) for k, v in ret.items()}
    self.assertEqual(ex("js:params['value'] = params['value'] + 1", {'params': {'value': 1, 'port': 'a'}}), {'params': {'value': 2, 'port': 'a'}})
    self.assertEqual(ex("js: params = {value: 'orto.', timer_to: '15m'}", {'params': {}}), {'params': {'value': 'orto.', 'timer_to': '15m'}})
    self.assertEqual(ex('js:x = 1', {'params': {}}, ['params']), {'params': {}})
    # The result can be the context of the next script (system does it for action "init" and "payload")
    ctx = scripting_js.script_exec("js:params['value'] = 'b'", {'params': {'value': 'a'}})
    self.assertEqual(scripting_js.script_eval("js:params['value'] + '!'", ctx), 'b!')

  def test_helpers(self):
    self.assertEqual(self.ev("js:payload_transfer({ a: 1 }, payload, ['value', 'nope'])", payload = {'value': 5, 'other': 1}), {'a': 1, 'value': 5})
    self.assertEqual(self.ev("js:payload_transfer({ a: 1 }, payload, { value: 'v' })", payload = {'value': 5}), {'a': 1, 'v': 5})
    self.assertIsNone(self.ev("js:payload_transfer({ a: 1 }, payload, ['nope'])", payload = {'value': 5}))
    self.assertEqual(self.ev("js:t('2014-04-07T13:58:10.104Z')"), 1396879090)
    self.assertEqual(self.ev('js:round(2.567, 1)'), 2.6)
    self.assertIs(self.ev('js:is_dict(payload)', payload = {'a': 1}), True)
    self.assertIs(self.ev('js:is_dict(payload)', payload = 'a'), False)
    self.assertIs(self.ev('js:is_array(matches)', matches = ['a']), True)
    self.assertEqual(self.ev("js:camel_to_snake_case('fooBar')"), 'foo_bar')
    self.assertEqual(self.ev("js:_('hello')"), 'hello')
    self.assertEqual(self.ev('js:typeof now'), 'number')

  def test_exports(self):
    # With Js2Py python functions receive Js2Py values: convert them, as the helpers of the core do
    scripting_js.exports = {'k': 5, 'double': lambda x: scripting_js._var_to_python(x) * 2}
    self.assertEqual(self.ev('js:double(k) + 1'), 11)

  def test_errors_return_none(self):
    self.assertIsNone(self.ev('js:nope_function()'))
    self.assertIsNone(self.ev('js:({ a: '))
    self.assertIsNone(scripting_js.script_exec('js:nope_function()', {'params': {}}))
    # The engine still works after an error
    self.assertEqual(self.ev('js:1'), 1)

  def test_stats(self):
    self.ev('js:"stats test"')
    self.ev('js:"stats test"')
    self.assertGreater(scripting_js.script_js_compiled_hits, 0)
    self.assertGreater(len(scripting_js.script_js_compiled), 0)

class Js2PyTest(EngineCases, unittest.TestCase):
  ENGINE = 'js2py'

class QuickJsTest(EngineCases, unittest.TestCase):
  ENGINE = 'quickjs'

  def test_modern_syntax(self):
    self.assertEqual(self.ev('js:[1, 2].map(x => x * 2)'), [2, 4])
    self.assertEqual(self.ev('js:({ ...payload, b: 2 })', payload = {'a': 1}), {'a': 1, 'b': 2})
    self.assertEqual(self.ev('js:payload?.a?.b ?? "none"', payload = {}), 'none')

  def test_function_ending_with_comment(self):
    self.assertEqual(self.ev('jsf:return 1 // one'), 1)

  def test_helpers_get_python_values(self):
    # With Js2Py helpers receive Js2Py strings: d() returns -1
    self.assertEqual(self.ev("js:d('5m')"), 300)
    self.assertEqual(self.ev("js:array_sum([1, 2.5])"), 3.5)

  def test_exports_values_are_updated(self):
    scripting_js.exports = {'is_day': True}
    self.assertIs(self.ev('js:is_day'), True)
    scripting_js.exports['is_day'] = False
    self.assertIs(self.ev('js:is_day'), False)

  def test_threads(self):
    results, errors = {}, []
    def run(i):
      try:
        for j in range(50):
          v = self.ev("js:({ i: payload['i'], j: payload['j'] })", payload = {'i': i, 'j': j})
          if v != {'i': i, 'j': j}:
            errors.append((i, j, v))
        results[i] = True
      except Exception as e:
        errors.append(e)
    threads = [threading.Thread(target = run, args = (i, )) for i in range(8)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()
    self.assertEqual(errors, [])
    self.assertEqual(len(results), 8)

class EngineSelectionTest(unittest.TestCase):
  def setUp(self):
    logging.disable(logging.CRITICAL)

  def tearDown(self):
    scripting_js.set_engine()
    logging.disable(logging.NOTSET)

  def test_unknown_engine_uses_default(self):
    if not available(scripting_js.DEFAULT_ENGINE):
      self.skipTest(scripting_js.DEFAULT_ENGINE + ' not installed')
    self.assertEqual(scripting_js.set_engine('nope'), scripting_js.DEFAULT_ENGINE)
    self.assertEqual(scripting_js.script_eval('js:1 + 1'), 2)

  def test_unavailable_engine_uses_the_other(self):
    if not available('js2py'):
      self.skipTest('js2py not installed')
    engines = dict(scripting_js.ENGINES)
    try:
      scripting_js.ENGINES['quickjs'] = 'automato.core.nope_module'
      self.assertEqual(scripting_js.set_engine('quickjs'), 'js2py')
    finally:
      scripting_js.ENGINES.clear()
      scripting_js.ENGINES.update(engines)

  def test_only_selected_engine_is_used(self):
    for engine in scripting_js.ENGINES:
      if available(engine):
        scripting_js.set_engine(engine)
        self.assertEqual(scripting_js.engine, engine)
        self.assertIs(scripting_js._get(), importlib.import_module(scripting_js.ENGINES[engine]))

class ConfigTest(CoreTestCase):
  config = {'scripting-engine': 'quickjs'}

  def setUp(self):
    if not available('quickjs'):
      self.skipTest('quickjs not installed')
    super().setUp()

  def tearDown(self):
    super().tearDown()
    scripting_js.set_engine()

  def test_engine_from_config(self):
    self.assertEqual(scripting_js.engine, 'quickjs')
    # system._reset() exports are used by the engine
    system.exports['test_export'] = 7
    self.assertEqual(scripting_js.script_eval('js:test_export * 2'), 14)

if __name__ == '__main__':
  unittest.main()
