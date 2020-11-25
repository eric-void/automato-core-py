# require python3
# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
import time
import json
import re
import datetime
import logging
import string
import random
import threading
import queue
import copy

import struct

from automato.core import system
from automato.core import utils

settings = {}
connected = 0
connected_since = 0
client = False
cache = {}
disconnect_queue = []
destroyed = False

# To avoid deadlocks and race conditions con mqtt client (possible in multi-thread environment with on_message and publish calls collisions) we use a "communication queue" to serialize all calls to on_message and publish
mqtt_communication_thread = False
mqtt_communication_queue = None
mqtt_communication_queue_only_subscribe = True # False = use queue for publish and subscribe, True = use it only for subscribe

# Used by test environment, to "pause" the mqtt listening thread
mqtt_subscribe_pause_on_topic = None

def init(lsettings = None):
  global settings, connected, connected_since, client, cache, disconnect_queue, mqtt_communication_queue, mqtt_communication_thread
  settings = {
    'client_id': 'device',
    'client_id_random_postfix': True,
    'broker_host': '127.0.0.1',
    'broker_port': 1883,
    'broker_user': '',
    'broker_pass': '',
    'publish_before_connection_policy': 'connect', # connect, queue, error
    'message-logger': '',
    'warning_slow_queue_ms': 5000,
    # all: True,
    # check_broken_connection: 5, # Number of seconds that a message published should be self-received. If not, the connection is considered broken and a reconnect attempt will be made. Use only with "all" = True
  };
  connected = 0
  connected_since = 0
  client = False
  cache = {}
  disconnect_queue = []
  destroyed = False

  mqtt_communication_queue = queue.Queue()
  mqtt_communication_thread = threading.Thread(target = _mqtt_communication_thread, daemon = True)
  mqtt_communication_thread.start()

  if lsettings:
    config(lsettings)

def destroy():
  global destroyed
  if connected:
    disconnect()
  destroyed = True

def config(lsettings):
  global settings
  settings = {**settings, **lsettings}

# callback(phase): 1 = connection started, 2 = connection established, 3 = first message received
def connect(callback = None):
  global connected, connected_since, client, settings
  
  settings['_connect_callback'] = callback
  client_id = settings['client_id']
  if settings['client_id_random_postfix']:
    client_id = client_id + '-' + (''.join(random.choice(string.ascii_lowercase) for m in range(5)))
  client = mqtt.Client(client_id)
  client.username_pw_set(settings['broker_user'], settings['broker_pass'])
  client.on_connect = _onConnect
  client.on_disconnect = _onDisconnect
  client.on_message = _onMessage
  connected = 0
  connected_since = 0
  while connected == 0:
    try:
      logging.debug('connecting to mqtt broker {host}:{port}, with client_id: {client_id} ...'.format(host = settings['broker_host'], port = settings['broker_port'], client_id = client_id))
      client.connect(settings['broker_host'], settings['broker_port'])
      connected = 1
      if '_connect_callback' in settings and settings['_connect_callback']:
        settings['_connect_callback'](connected)
    except:
      logging.exception('failed connection, will retry in 5 seconds ...')
      system.sleep(5)
  client.loop_start()

def _onConnect(client, userdata, flags, rc):
  global connected, connected_since, disconnect_queue, settings
  logging.debug('connected')
  if "all" in settings and settings['all']:
    client.subscribe("#")
  elif "subscribe" in settings:
    for v in settings["subscribe"]:
      if v != "[all]" and v != "[else]":
        client.subscribe(v)
  connected = 2
  connected_since = system.time()
  if '_connect_callback' in settings and settings['_connect_callback']:
    settings['_connect_callback'](connected)
  if disconnect_queue:
    logging.debug("there is something to publish in the queue")
    for q in disconnect_queue:
      publish(q[0], q[1], q[2], q[3])
    disconnect_queue = []

def _onDisconnect(client, userdata, rc):
  global connected, connected_since
  connected = 0
  connected_since = 0
  if rc != 0:
    logging.error('unexpected disconnection from mqtt broker')
  else:
    logging.debug('disconnected from mqtt broker')

def queueDelay():
  """
  @return ms of delay in current messages queue
  """
  global mqtt_communication_queue
  try:
    return system.timems() - mqtt_communication_queue.queue[0]['timems'] if mqtt_communication_queue.queue else 0
  except:
    return 0

def _mqtt_communication_thread():
  global connected_since, destroyed, settings, mqtt_communication_queue, mqtt_subscribe_pause_on_topic, cache
  #logging.debug("THREAD IN")
  while not destroyed:
    #logging.debug("THREAD .")
    d = mqtt_communication_queue.get()
    #logging.debug("QUEUE GOT: " + str(d))
    delay = system.timems() - d['timems']
    # Ignoring first 60 seconds from mqtt connections for warnings: i can receive a lot of retained messages, a slowdown is normal!
    if system.time() > connected_since + 60 and delay > settings['warning_slow_queue_ms']:
      logging.warning("Slow mqtt_communication_queue, last message fetched in {ms} ms: {msg}".format(ms = delay, msg = d))
    if d['type'] == 'publish':
      _mqtt_communication_publish(d['topic'], d['payload'], d['qos'], d['retain'])

    elif d['type'] == 'subscribe':
      try:
        decoded = _decodePayload(d['payload'])
        logging.getLogger(settings["message-logger"]).info('received message{retained} {topic} = {payload}{delay}'.format(topic = d['topic'], payload = d['payload'], retained = ' (retained)' if d['retain'] else '', delay = ' (' + str(delay) + 'ms delay)' if delay > 0 else ''))
        if "cache" in settings and settings['cache']:
          cache[d['topic']] = decoded
        if "subscribe" in settings:
          done = False
          for v in settings["subscribe"]:
            if v != "[all]" and v != "[else]" and callable(settings["subscribe"][v]):
              is_regex = v.startswith("/") and v.endswith("/")
              matches = _topicMatchesToList(re.search(v[1:-1], d['topic'])) if is_regex else []
              if (not is_regex and topicMatchesMQTT(v, d['topic'])) or (is_regex and matches):
                settings["subscribe"][v](d['topic'], d['payload'], copy.deepcopy(decoded), d['qos'], d['retain'], matches, d['timems'])
                done = True
          if not done and "[else]" in settings["subscribe"]:
            settings["subscribe"]["[else]"](d['topic'], d['payload'], copy.deepcopy(decoded), d['qos'], d['retain'], None, d['timems'])
          if "[all]" in settings["subscribe"]:
            settings["subscribe"]["[all]"](d['topic'], d['payload'], copy.deepcopy(decoded), d['qos'], d['retain'], None, d['timems'])
      except:
        logging.exception('error in message handling')
      if mqtt_subscribe_pause_on_topic and d['topic'] == mqtt_subscribe_pause_on_topic:
        while mqtt_subscribe_pause_on_topic and d['topic'] == mqtt_subscribe_pause_on_topic:
          system.sleep(.1)
        system.sleep(.1)

def _mqtt_communication_publish(topic, payload, qos, retain):
  logging.debug("publishing on broker {topic} (qos = {qos}, retain = {retain})".format(topic = topic, qos = qos, retain = retain))
  if settings["message-logger"] != '':
    logging.getLogger(settings["message-logger"]).info("publishing on broker {topic} = {payload} (qos = {qos}, retain = {retain})".format(topic = topic, payload = str(payload), qos = qos, retain = retain))
  try:
    client.publish(topic, payload, qos, retain)
  except:
    logging.exception("error publishing topic on broker")

def _onMessage(client, userdata, msg):
  global connected, mqtt_communication_queue
  #logging.debug("QUEUE PUT: " + str({'type': 'subscribe', 'topic': msg.topic, 'payload': msg.payload, 'retain': msg.retain, 'qos': msg.qos}))
  mqtt_communication_queue.put({'type': 'subscribe', 'topic': msg.topic, 'payload': msg.payload, 'retain': msg.retain, 'qos': msg.qos, 'timems': system.timems()})
  if connected < 3:
    connected = 3
    if '_connect_callback' in settings and settings['_connect_callback']:
      settings['_connect_callback'](connected)
  if msg.retain and connected_since > 0 and system.time() > connected_since + 30:
    logging.error("MQTT> Received a retained messages at boot finished: {topic} = {payload}".format(topic = msg.topic, payload = msg.payload))

def topicMatches(topic, pattern):
  is_regex = pattern.startswith("/") and pattern.endswith("/")
  if is_regex:
    return _topicMatchesToList(re.search(pattern[1:-1], topic))
  return [True] if topicMatchesMQTT(pattern, topic) else []

def topicMatchesMQTT(pattern, topic):
  return mqtt.topic_matches_sub(pattern, topic);

def _topicMatchesToList(m):
  return ([m.group(0)] + list(m.groups())) if m else []

def _decodePayload(v):
  try:
    v = v.decode("utf-8")
  except AttributeError:
    pass
  if type(v) == str and v != '' and ((v[0] == '[' and v[-1] == ']') or (v[0] == '{' and v[-1] == '}')):
    try:
      return utils.json_import(v)
    except:
      logging.exception('error decoding payload')
      return None
  return _magicCast(v)

def _magicCast(v):
  return v
"""
  try:
    r2 = float(v)
    try:
      r1 = int(v)
    except:
      return r2
    if r1 == r2 or abs(r1 - r2) < 0.001:
      return r1
    else:
      return r2
  except:
    return str(v)
"""

def _timePayload(v):
  try:
    v = v.decode("utf-8")
  except AttributeError:
    pass
  if isinstance(v, str) and ":" in v:
    vv = datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S")
    return int(time.mktime(vv.timetuple()))
  else:
    try:
      v = int(v)
    except:
      return None
    if v > 1E12:
      v = int(v / 1000)
    return v

def disconnect():
  global connected, connected_since, client
  client.loop_stop()
  client.disconnect()
  connected = 0
  connected_since = 0

def publish(topic, payload, qos = 0, retain = False):
  global connected, client, settings, disconnect_queue
  if not isinstance(payload, str) and payload != None:
    payload = utils.json_export(payload)
  if (connected == 0):
    if settings['publish_before_connection_policy'] == 'connect':
      logging.debug("connecting broker and publishing {topic} (qos = {qos}, retain = {retain})".format(topic = topic, qos = qos, retain = retain))
      if settings["message-logger"] != '':
        logging.getLogger(settings["message-logger"]).info("publishing on broker {topic} = {payload} (qos = {qos}, retain = {retain})".format(topic = topic, payload = str(payload), qos = qos, retain = retain))
      connect()
      client.publish(topic, payload, qos, retain)
      disconnect()
    elif settings['publish_before_connection_policy'] == 'queue':
      logging.debug("broker not connected, adding publish action to queue: {topic} (qos = {qos}, retain = {retain})".format(topic = topic, qos = qos, retain = retain))
      if settings["message-logger"] != '':
        logging.getLogger(settings["message-logger"]).info("adding publish action to queue: {topic} = {payload} (qos = {qos}, retain = {retain})".format(topic = topic, payload = str(payload), qos = qos, retain = retain))
      replaced = False
      for qi, qd in enumerate(disconnect_queue):
        if qd[0] == topic:
          disconnect_queue[qi] = [topic, payload, qos, retain]
          replaced = True
          break
      if not replaced:
        disconnect_queue.append([topic, payload, qos, retain])
    else:
      raise RuntimeError('Tried to publish something before connecting')
  else:
    if mqtt_communication_queue_only_subscribe:
      _mqtt_communication_publish(topic, payload, qos, retain)
    else:
      mqtt_communication_queue.put({'type': 'publish', 'topic': topic, 'payload': payload, 'qos': qos, 'retain': retain, 'timems': system.timems()})
    
def get(topic, expr = False, default = None):
  if not topic in cache:
    return default
  if not expr:
    return cache[topic]
  if not '[' in expr:
    expr = '["' + expr.replace('.', '"]["') + '"]'
  try:
    return _magicCast(eval("cache[topic]" + expr))
  except:
    return default
  
def getTime(topic, expr = False, default = None):
  ret = get(topic, expr, default)
  return _timePayload(ret) if ret is not None else default
  
def cachePrint():
  print(str(cache))
