# require python3
# -*- coding: utf-8 -*-

import threading
import unittest

from automato.core import system

from helpers import CoreTestCase, receive

DEVICE = {
  'device': 'dev',
  'publish': {
    'dev/state': {
      'type': 'object',
      'events': {
        'output': "js:({ port: payload['port'], value: payload['value'] })",
      },
    },
    'dev/#': {},
  },
  'subscribe': {
    'dev/set': {
      'description': 'set',
      'response': ['dev/state'],
      'actions': {
        'output-set': "js:({ port: params['port'], value: params['value'] })",
      },
    },
  },
}

class EntryDefinitionsTest(CoreTestCase):
  definitions = { 'dev@test': DEVICE }

  def test_exact_topic_returns_its_definition(self):
    e = self.entry('dev')
    self.assertEqual(system.entry_topic_published_definition(e, 'dev/state')['type'], 'object')
    self.assertEqual(system.entry_topic_subscription_definition(e, 'dev/set')['description'], 'set')
    self.assertTrue(system.entry_is_publisher_of(e, 'dev/other'))
    self.assertFalse(system.entry_is_subscribed_to(e, 'other/set'))

  def test_index_is_consistent_after_reload(self):
    system.entry_reload('dev@test')
    self.assertEqual(system.index_topic_published['dev/state']['entries'], ['dev@test'])
    system.entry_unload('dev@test')
    self.assertEqual(system.index_topic_published['dev/state']['entries'], [])
    self.assertEqual(system.entries_publishers_of('dev/state'), {})

  def test_exported_definition_is_filtered(self):
    d = system.entries_definition_exportable()['dev@test']
    self.assertIn('events', d['publish']['dev/state'])
    self.assertNotIn('device', d)

class MessagesTest(CoreTestCase):
  definitions = { 'dev@test': DEVICE }

  def test_first_published_message_without_previous_calls(self):
    m = system.Message('dev/state', {'port': '0', 'value': 1})
    pm = m.firstPublishedMessage()
    self.assertIsNotNone(pm)
    # The not internal definition is preferred to "dev/#"
    self.assertEqual(pm.topic_rule, 'dev/state')

  def test_current_received_message(self):
    self.assertIsNone(system.current_received_message())
    seen = []
    system.on_message(lambda m: seen.append(system.current_received_message().topic))
    receive('dev/state', {'port': '0', 'value': 1})
    self.assertEqual(seen, ['dev/state'])
    # Other threads don't see the message processed by the mqtt thread
    other = []
    t = threading.Thread(target = lambda: other.append(system.current_received_message()))
    t.start(); t.join()
    self.assertEqual(other, [None])

  def test_action_publishes_payload(self):
    self.assertTrue(system.do_action('dev.output-set', {'port': '1', 'value': 1}))
    self.assertEqual(self.broker.published[-1]['topic'], 'dev/set')
    self.assertEqual(system.utils.json_import(self.broker.published[-1]['payload']), {'port': '1', 'value': 1})

class EventsTest(CoreTestCase):
  config = { 'listen_all_events': True }
  definitions = { 'dev@test': DEVICE }

  def test_keys_and_changed_params(self):
    events = []
    system.on_event('dev.output', lambda entry, eventname, eventdata, caller, pm: events.append(eventdata), self.entry('dev'))
    receive('dev/state', {'port': '0', 'value': 1})
    receive('dev/state', {'port': '1', 'value': 1})
    receive('dev/state', {'port': '0', 'value': 1})
    receive('dev/state', {'port': '0', 'value': 0})
    self.assertEqual([e['keys'] for e in events], [{'port': '0'}, {'port': '1'}, {'port': '0'}, {'port': '0'}])
    self.assertEqual([e['changed_params'] for e in events], [{'value': 1}, {'value': 1}, {}, {'value': 0}])
    self.assertEqual(system.entry_event_get('dev', 'output', condition = "js: params['port'] == '1'"), {'port': '1', 'value': 1})

  def test_listener_condition(self):
    events = []
    system.on_event("dev.output(js: params['port'] == '1')", lambda entry, eventname, eventdata, caller, pm: events.append(eventdata['params']), self.entry('dev'))
    receive('dev/state', {'port': '0', 'value': 1})
    receive('dev/state', {'port': '1', 'value': 1})
    self.assertEqual(events, [{'port': '1', 'value': 1}])

  def test_listeners_receive_independent_copies(self):
    seen = []
    def listener1(entry, eventname, eventdata, caller, pm):
      eventdata['params']['value'] = 'changed'
    system.on_event('dev.output', listener1, self.entry('dev'))
    system.on_event('dev.output', lambda entry, eventname, eventdata, caller, pm: seen.append(eventdata['params']['value']), self.entry('dev'))
    receive('dev/state', {'port': '0', 'value': 1})
    self.assertEqual(seen, [1])

class ListenersOnReloadTest(CoreTestCase):
  config = { 'listen_all_events': True }
  definitions = { 'dev@test': DEVICE, 'listener@test': { 'item': 'listener' } }

  def test_listeners_survive_reload_of_listened_entry(self):
    events = []
    system.on_event('dev.output', lambda entry, eventname, eventdata, caller, pm: events.append(eventdata['params']['value']), self.entry('listener'))
    # A new definition (for example new metadata of a remote entry) reloads the entry
    self.load({ 'dev@test': { **DEVICE, 'caption': 'Changed' } })
    self.assertEqual(self.entry('dev').caption, 'Changed')
    receive('dev/state', {'port': '0', 'value': 1})
    self.assertEqual(events, [1])

  def test_listeners_are_removed_with_the_entry_that_registered_them(self):
    events = []
    system.on_event('dev.output', lambda entry, eventname, eventdata, caller, pm: events.append(1), self.entry('listener'))
    system.entry_unload('listener@test')
    receive('dev/state', {'port': '0', 'value': 1})
    self.assertEqual(events, [])

class EventsGroupsTest(CoreTestCase):
  definitions = {
    'grouped@test': {
      'device': 'grouped',
      'publish': {
        'grouped/state': {
          'type': 'object',
          'events': {
            'output': "js:({ value: payload['value'], extra: payload['extra'] })",
            'output:group': 10,
          },
        },
      },
    },
  }

  def setUp(self):
    super().setUp()
    system.time_pause()

  def tearDown(self):
    system.time_resume()
    super().tearDown()

  def test_events_are_grouped(self):
    events = []
    system.on_event('grouped.output', lambda entry, eventname, eventdata, caller, pm: events.append((caller, eventdata['params'])), self.entry('grouped'))
    receive('grouped/state', {'value': 1, 'extra': 'a'})
    receive('grouped/state', {'value': 2, 'extra': 'b'})
    self.assertEqual(events, [])
    system.events_groups_check(system.time() + 5)
    self.assertEqual(events, [])
    system.events_groups_check(system.time() + 10)
    self.assertEqual(events, [('group', {'value': 2, 'extra': 'b'})])

  def test_listener_generating_grouped_event_does_not_deadlock(self):
    entry = self.entry('grouped')
    calls = []
    def listener(e, eventname, eventdata, caller, pm):
      calls.append(1)
      if len(calls) == 1:
        # A listener generating (synchronously) another event of the same group
        system._entry_event_publish(entry, 'output', {'value': 3, 'extra': 'c'}, system.time() + 10)
    system.on_event('grouped.output', listener, entry)
    receive('grouped/state', {'value': 1, 'extra': 'a'})
    t = threading.Thread(target = lambda: system.events_groups_check(system.time() + 10), daemon = True)
    t.start()
    t.join(5)
    self.assertFalse(t.is_alive(), 'deadlock in events_groups_check')
    self.assertEqual(calls, [1])

class SubscribeResponseTest(CoreTestCase):
  definitions = { 'dev@test': DEVICE, 'caller@test': { 'item': 'caller' } }

  def test_no_response(self):
    calls = []
    self.assertTrue(system.subscribe_response(self.entry('caller'), system.Message('dev/set', {}), callback = lambda *args: calls.append('response'), no_response_callback = lambda *args: calls.append('no_response')))
    system._subscription_timer_step(system.timems() + 1000)
    self.assertEqual(calls, [])
    system._subscription_timer_step(system.timems() + 10000)
    self.assertEqual(calls, ['no_response'])
    self.assertEqual(system.subscribed_response, [])

  def test_response(self):
    calls = []
    system.subscribe_response(self.entry('caller'), system.Message('dev/set', {}), callback = lambda entry, id, message, final, request: calls.append(('response', message.topic, final)), no_response_callback = lambda *args: calls.append('no_response'))
    receive('dev/state', {'port': '0', 'value': 1})
    system._subscription_timer_step(system.timems() + 10000)
    self.assertEqual(calls, [('response', 'dev/state', True)])

  def test_error_in_callback_does_not_stop_other_timeouts(self):
    calls = []
    def failing(*args):
      raise RuntimeError('test')
    system.subscribe_response(self.entry('caller'), system.Message('dev/set', {}), no_response_callback = failing)
    system.subscribe_response(self.entry('caller'), system.Message('dev/set', {}), no_response_callback = lambda *args: calls.append('no_response'))
    system._subscription_timer_step(system.timems() + 10000)
    self.assertEqual(calls, ['no_response'])

  def test_subscription_with_same_id(self):
    message = system.Message('dev/set', {})
    self.assertTrue(system.subscribe_response(self.entry('caller'), message, id = 'x'))
    self.assertFalse(system.subscribe_response(self.entry('caller'), message, id = 'x'))

if __name__ == '__main__':
  unittest.main()
