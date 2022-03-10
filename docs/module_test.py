# require python3
# -*- coding: utf-8 -*-

from automato.core import test
from automato.core import system
# optional: from automato.node import node_system

def test_init():
  test.add_node_config({
    "listen_all_events": True,
    "entries": [
      { "module": "scripting" },
      {
        "caption": "Device",
        "device": "test",
        "...": "...",
      },
    ]
  })
      
  # ADVANCED: test a topic is publish during node initialization (before test_run)
  test.assertx('init', assertSubscribe = { 'device/toggle/get': '' }, wait = False)

def test_run(entries):
  # ADVANCED wait for assertx done in test_init
  test.waitRunning()
  
  # Publish a topic and then wait asserts
  test.assertPublish('s1', 'entry_b/pub1', 'test', 
    assertEvents = {'test_event': {'port': 'test1'}}, # Check events are emitted, linked to published topic, with events params specifed
    assertSomeEvents = {'test_event': {'port': 'test1'}}, # Check events are emitted, linked to published topic, and events params cointans the specified one
    assertNotEvents [ 'test_events' ], # No one of these events should be emitted

    assertSubscribe = { # Check this topic are published after the first one, and message IS the specified one
      'notify/info/entry_b/pub1': 'Entry B published pub1=test'
      'val': ('d', 10, 2), # Value should be in the range [10..12] (if second parameter is omitted it will be considered as "1")
      'reason': ('re', '.*test-device.*dead.*'), # regexp
      'x': ('*', ), # Match is always true ('x' should exist, but all values are correct)
      'x': (), # Like above (match always true)
    }),
    assertSubscribeSomePayload = {'item/test-toggle': {'state': 1, 'timer-to': 0 }}, # Check this topic are published, and message CONTAINS the specified one
    assertSubscribeNotReceive = ['device/test-device/health', 'item/test-item/health'], # Check this topic are NOT published
    
    assertNotification = ['level', 'string'], # Check notification 
    # or
    assertNotification = ['level', ('re', '.*'), 'topic'], # Check notification only on that topic message published
    
    assertChild = ['entry_a_on_test_event1'], # Check specified child is executed
    assertNotChild = ['entry_a_on_test_event2'], # Check child is NOT executed
    timeoutms = 2000, # wait this ms for assert checks
    wait = True, # if False, function call returns and you should call "test.waitRunning()" manually to wait for running asserts and end the test
  )

  # Do an action on a specific entry
  test.assertAction('s6', 'entry_b', 'test_action', { 'value': '1' }, init = 'js:params["val2"] = "0"', 
    assertEventsTopic = 'subs/entry_b/response', assertEvents = {'test_action_response': {}} # To check for events you should specify an events topic (if NOT, it will check on all events emitted)
    assertSubscribe = {'subs/entry_b': 'test10', 'subs/entry_b/response': 'ok'},
    assertChild = ['entry_b_on_subscribed_message', 'entry_b_publish', 'entry_b_on_events_passthrough'], 
  )
  # NOTE: use assertEventsData = True to check full events data (not only "params" but also "changed_params")
  
  # Generic assert with no action performed (no publish / subscribe / action)
  test.assertx('t1', 
    waitPublish = {'topic1': 'payload1:', ...}, # Do some "waitPublish" commands before the rest of the start starts
    assertSubscribe = {'device/test-rf-1/detected': ''}, assertEventsTopic = 'device/test-rf-1/detected', assertEvents = {'detected': {}, 'input': {'value': 1, 'temporary': 1}}, wait = False)
  # ... DO SOMETHING
  # ... example: node_system.run_step()
  test.waitRunning()
  
  # Generic eq assert
  test.assertx('t5', assertEq = [(a, 'test'), (b, None)])
  
  # Wait the specific topic/message is published
  test.waitPublish("device/test-device/check", "online")
  
  # To make a direct call to a module handler
  entries['name@TEST'].module.callback(...)
  
  # To change an instance config
  entries['name@TEST'].config['...'] = '...'
  
  # Move ahead clock time by that number of seconds
  system.time_offset(5)
  # Pause the clock
  system.time_pause()
  # Resume the clock
  system.time_resume()

def on_some_callback(entry, eventname, params):
  # Check if a specific test is running
  if test.isRunning('s1'):
    # Enable the specific child (so the "assertChild" will be successfull)
    test.assertChild('entry__on_test_event1', 
      # Check variable values
      assertEq = [(entry.id, "entry_b@TEST"), (eventname, "test_event"), (params, {"port": "test1"})]
    )


