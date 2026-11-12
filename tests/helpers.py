# require python3
# -*- coding: utf-8 -*-

"""
Helpers for code-oriented tests: the core runs without a broker (mqtt.connect/publish are replaced), messages are injected directly.
"""

import json
import logging
import unittest

from automato.core import mqtt, system, utils

class FakeBroker(object):
  def __init__(self):
    self.published = []

  def connect(self, callback = None):
    # Simulates a connection established (phase 2), so code waiting for it can go on
    if callback:
      callback(2)

  def publish(self, topic, payload, qos = 0, retain = False):
    if not isinstance(payload, str) and payload is not None:
      payload = utils.json_export(payload)
    self.published.append({'topic': topic, 'payload': payload, 'qos': qos, 'retain': retain})

  def install(self):
    mqtt.connect = self.connect
    mqtt.publish = self.publish

def receive(topic, payload, retain = False):
  """
  Inject a message, as if received by the broker
  """
  raw = payload if isinstance(payload, (str, bytes)) else json.dumps(payload)
  system._on_mqtt_message(topic, raw, mqtt._decodePayload(raw), 0, retain, None, system.timems())

class CoreTestCase(unittest.TestCase):
  """
  Boots the core with "definitions" ({ entry_id: definition }) and a fake broker.
  The subscription timer thread is stopped: tests call system._subscription_timer_step() directly.
  """
  config = {}
  definitions = {}

  def setUp(self):
    logging.disable(logging.CRITICAL)
    self.broker = FakeBroker()
    system.set_config({'name': 'test', 'entries': [], 'console-log': False, **self.config})
    system.boot()
    self.broker.install()
    system.init(lambda: None)
    system.destroyed = True
    system.subscription_thread_stop.set()
    system.subscription_thread.join()
    system.destroyed = False
    if self.definitions:
      self.load(self.definitions)

  def tearDown(self):
    system.destroy()
    logging.disable(logging.NOTSET)

  def load(self, definitions):
    system.entry_load(definitions, False, False, False)

  def entry(self, entry_id):
    return system.entry_get(entry_id)
