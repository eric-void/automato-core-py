# require python3
# -*- coding: utf-8 -*-

import logging
import unittest

from automato.core import utils

class ReadDurationTest(unittest.TestCase):
  def test_values(self):
    for v, expected in [(10, 10), ('10', 10), ('10s', 10), ('5m', 300), ('2h', 7200), ('1d', 86400), ('1w', 604800), ('3M', 180), ('x', -1), ('1.5h', -1), (None, -1), ('', -1)]:
      with self.subTest(v = v):
        self.assertEqual(utils.read_duration(v), expected)

class JsonTest(unittest.TestCase):
  def setUp(self):
    logging.disable(logging.CRITICAL)

  def tearDown(self):
    logging.disable(logging.NOTSET)

  def test_import_invalid_returns_none(self):
    self.assertIsNone(utils.json_import('{"a": '))

  def test_import_nan_is_none(self):
    self.assertEqual(utils.json_import('{"a": NaN}'), {'a': None})

  def test_export_nan_is_null(self):
    self.assertEqual(utils.json_export({'a': float('nan')}), '{"a": null}')

class SignatureTest(unittest.TestCase):
  def test_deep_dicts_with_different_key_order_have_the_same_signature(self):
    a = {'x': {'b': {'d': 1, 'c': 2}, 'a': 1}, 'y': [1, 2]}
    b = {'y': [1, 2], 'x': {'a': 1, 'b': {'c': 2, 'd': 1}}}
    self.assertEqual(utils.data_signature(a), utils.data_signature(b))

  def test_different_data_have_different_signatures(self):
    self.assertNotEqual(utils.data_signature({'x': {'b': {'c': 1}}}), utils.data_signature({'x': {'b': {'c': 2}}}))

class ArrayTest(unittest.TestCase):
  def test_sum(self):
    self.assertEqual(utils.array_sum([1.234, None, 2]), 3.234)
    self.assertEqual(utils.array_sum([1.234, 2], 1), 3.2)
    self.assertIsNone(utils.array_sum([None]))

  def test_avg_min_max(self):
    self.assertEqual(utils.array_avg([1, 2, None, 6]), 3)
    self.assertEqual(utils.array_avg([1, 2, 100], valid_max = 10), 1.5)
    self.assertEqual(utils.array_min([None, 3, 1]), 1)
    self.assertEqual(utils.array_max([None, 3, 1]), 3)

class DataCopyTest(unittest.TestCase):
  def test_copy_is_deep_and_equal(self):
    v = {'a': [1, {'b': 2.5, 'c': None}], 'd': True, 'e': 'x'}
    c = utils.data_copy(v)
    self.assertEqual(c, v)
    c['a'][1]['b'] = 3
    c['a'].append(4)
    self.assertEqual(v['a'], [1, {'b': 2.5, 'c': None}])

  def test_other_types_are_deep_copied(self):
    class Obj(object):
      def __init__(self):
        self.l = [1]
    v = {'t': (1, [2]), 'o': Obj()}
    c = utils.data_copy(v)
    self.assertEqual(c['t'], (1, [2]))
    c['o'].l.append(2)
    self.assertEqual(v['o'].l, [1])

class ParseDatetimeTest(unittest.TestCase):
  def test_values(self):
    self.assertEqual(utils.parse_datetime('1396879090'), 1396879090)
    self.assertEqual(utils.parse_datetime(1396879090123), 1396879090)
    self.assertEqual(utils.parse_datetime('2014-04-07T13:58:10.104Z'), 1396879090)
    self.assertEqual(utils.parse_datetime(''), 0)
    self.assertEqual(utils.parse_datetime('not a date'), 0)

if __name__ == '__main__':
  unittest.main()
