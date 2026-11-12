# require python3
# -*- coding: utf-8 -*-

import logging
import unittest

from automato.core import system

class TopicMatchesTest(unittest.TestCase):
  def setUp(self):
    logging.disable(logging.CRITICAL)

  def tearDown(self):
    logging.disable(logging.NOTSET)

  def test_topic_rules(self):
    for rule, topic, payload, matched, topic_matches in [
      ('a/b', 'a/b', None, True, [True]),
      ('a/b', 'a/c', None, False, []),
      ('a/+/c', 'a/x/c', None, True, [True]),
      ('a/#', 'a/x/y', None, True, [True]),
      ('a/#', 'b/x', None, False, []),
      ('/^a\\/(.*)\\/c$/', 'a/xyz/c', None, True, ['a/xyz/c', 'xyz']),
      ('/^a\\/(.*)$/', 'b/xyz', None, False, []),
      ('a/b[on|off]', 'a/b', 'on', True, [True]),
      ('a/b[on|off]', 'a/b', 'toggle', False, [True]),
      ('a/b[/^v[0-9]+$/]', 'a/b', 'v12', True, [True]),
      ('a/b[/^v[0-9]+$/]', 'a/b', 'vx', False, [True]),
      ("a/b[js: payload['x'] == 1]", 'a/b', {'x': 1}, True, [True]),
      ("a/b[js: payload['x'] == 1]", 'a/b', {'x': 2}, False, [True]),
    ]:
      with self.subTest(rule = rule, topic = topic, payload = payload):
        m = system.topic_matches(rule, topic, payload)
        self.assertEqual(m['matched'], matched)
        self.assertEqual(m['topic_matches'], topic_matches)

  def test_payload_filter_ignored_without_payload(self):
    self.assertTrue(system.topic_matches('a/b[on|off]', 'a/b')['matched'])

  def test_rule_preferred(self):
    # same priority: exact rule wins over wildcards
    self.assertTrue(system.topic_rule_preferred('a/b', {'type': 'x'}, 'a/#', {'type': 'x'}))
    self.assertFalse(system.topic_rule_preferred('a/#', {'type': 'x'}, 'a/b', {'type': 'x'}))
    # higher priority wins
    self.assertTrue(system.topic_rule_preferred('a/#', {'type': 'x'}, 'a/b', {}))
    self.assertTrue(system.topic_rule_preferred('a/#', {'topic_match_priority': 5}, 'a/b', {'type': 'x'}))

class ReferencesTest(unittest.TestCase):
  def test_decode_event_reference(self):
    self.assertEqual(system.decode_event_reference('e.ev'), {'entry': 'e', 'event': 'ev', 'condition': None})
    self.assertEqual(system.decode_event_reference("e@n.ev(js: params['port'] == 1)"), {'entry': 'e@n', 'event': 'ev', 'condition': "js: params['port'] == 1"})
    self.assertEqual(system.decode_event_reference('.ev', default_entry_id = 'x'), {'entry': 'x', 'event': 'ev', 'condition': None})
    self.assertIsNone(system.decode_event_reference('e'))
    self.assertEqual(system.decode_event_reference('e(c)', no_event = True), {'entry': 'e', 'event': None, 'condition': 'c'})

  def test_decode_action_reference(self):
    self.assertEqual(system.decode_action_reference("e.output-set(js: params['value'] = 1)"), {'entry': 'e', 'action': 'output-set', 'init': "js: params['value'] = 1"})
    self.assertIsNone(system.decode_action_reference('e'))

  def test_action_to_event_reference(self):
    self.assertEqual(system.transform_action_reference_to_event_reference("e.output-set(js: params['x'] = 1; params['y'] = 'a';)"), "e.output(js: params['x'] == 1&& params['y'] == 'a')")

  def test_action_to_event_reference_keeps_comparisons(self):
    self.assertEqual(system.transform_action_reference_to_event_reference("e.output-set(js: params['x'] = params['a'] == 1 ? 1 : 0; params['y'] = params['b'] != 2; params['z'] = params['c'] <= 3)"),
      "e.output(js: params['x'] == params['a'] == 1 ? 1 : 0&& params['y'] == params['b'] != 2&& params['z'] == params['c'] <= 3)")

  def test_event_to_action_reference(self):
    self.assertEqual(system.transform_event_reference_to_action_reference("e.output(js: params['x'] == 1 && params['y'] == 2)"), "e.output-set(js: params['x'] = 1 ; params['y'] = 2)")

  def test_generate_event_reference(self):
    self.assertEqual(system.generate_event_reference('e@n', 'output', {'keys': {'port': '1'}}), 'e@n.output(js: params["port"] == "1")')
    self.assertEqual(system.generate_event_reference('e@n', 'output', {'keys': {}}), 'e@n.output')

if __name__ == '__main__':
  unittest.main()
