# require python3
# -*- coding: utf-8 -*-

"""
Manage notifications messages and response topics
"""

import logging
import threading
import collections.abc
import datetime
import re

from automato.core import system
from automato.core import utils
from automato.core import scripting_js
from automato.node import node_system as node

notifications_last_topic_payloads = {}
notifications_topic_next_level = {}
destroyed = False

notifications_levels = {
  "debug": 20,
  "response": 40,
  "info": 50,
  "warn": 60,
  "error": 70,
  "critical": 90,
}

def init():
  global notifications_last_topic_payloads, notifications_topic_next_level, destroyed
  notifications_last_topic_payloads = {}
  notifications_topic_next_level = {}
  destroyed = False

def destroy():
  global destroyed
  destroyed = True
  
def entry_normalize(entry):
  for k in ['notify', 'notify_level', 'notify_handler', 'notify_change_level', 'notify_change_duration']:
    if k in entry.definition:
      for topic in entry.definition['publish']:
        if k not in entry.definition['publish'][topic]:
          entry.definition['publish'][topic][k] = entry.definition[k]
      for topic in entry.definition['subscribe']:
        if k not in entry.definition['subscribe'][topic]:
          entry.definition['subscribe'][topic][k] = entry.definition[k]

def notification_build(published_message):
  global notifications_last_topic_payloads, notifications_topic_next_level
  
  entry = published_message.entry
  ldef = published_message.definition
  topic = published_message.topic
  payload = published_message.payload
  matches = published_message.matches
  
  set_notifications_topic_next_level = False
  defaults = {}
  
  if 'notify_if' in ldef:
    for expr in ldef['notify_if']:
      v = scripting_js.script_eval(expr, {"topic": topic, "payload": payload, "matches": matches}, cache = True);
      #v = entry.script_eval(expr, payload = getPayloadItem(payload, ldef), matches = matches, caption = entry.caption)
      if v:
        for k in ldef['notify_if'][expr]:
          defaults[k] = ldef['notify_if'][expr][k]
        if 'notify_next_level' in ldef['notify_if'][expr]:
          set_notifications_topic_next_level = ldef['notify_if'][expr]['notify_next_level']

  for k in ['notify', 'notify_level', 'notify_handler', 'notify_change_level', 'notify_change_duration', 'payload']:
    if k in ldef and k not in defaults:
      defaults[k] = ldef[k]
  
  string = None
  if not string and 'notify_handler' in defaults:
    #handler = node.get_handler(entry, defaults['notify_handler'])
    #if handler:
    #  string = handler(entry, topic, getPayloadItem(payload, defaults)) # TODO notify_handler deve essere rifatto via JS, gli deve essere passato anche matches, e chi lo usa deve essere modificato di conseguenza
    string = scripting_js.script_eval(defaults['notify_handler'], {"topic": topic, "payload": payload, "matches": matches}, cache = True);
    
  elif not string and 'notify' in defaults and defaults['notify'] and isinstance(defaults['notify'], str):
    string = defaults['notify'].format(payload = getPayloadItem(payload, defaults), _ = None if isinstance(payload, dict) else getPayloadItem({'payload': payload}, defaults), matches = matches, caption = entry.caption)
  
  if string:
    changed = topic in notifications_last_topic_payloads and notifications_last_topic_payloads[topic][0] != string
    if changed and 'notify_change_level' in defaults and ('notify_change_duration' not in defaults or system.time() - notifications_last_topic_payloads[topic][1] > utils.read_duration(defaults['notify_change_duration'])):
      defaults['notify_level'] = defaults['notify_change_level']
      notifications_last_topic_payloads[topic] = [string, system.time()]
    else:
      notifications_last_topic_payloads[topic] = [string, notifications_last_topic_payloads[topic][1] if topic in notifications_last_topic_payloads else 0]
  
  if topic in notifications_topic_next_level and notifications_topic_next_level[topic]:
    notify_level = notifications_topic_next_level[topic]
  else:
    notify_level = defaults['notify_level'] if 'notify_level' in defaults else 'info'
  
  notifications_topic_next_level[topic] = set_notifications_topic_next_level
  
  return { 'notification_slevel': notify_level, 'notification_level': notifications_levels[notify_level], 'notification_string': string if string else None }

#########################################################################################################################################################

# Classe usata per wrappare il payload da mandare alle notifications, in modo da poter chiamare velocemente dei formattatori sui valori interni
#
# Formattatori supportati:
# - {payload!'default'} Se payload c'è non usa la stringa 'default'
# - {payload!strftime(%Y-%m-%d %H:%M:%S)} oppure {payload!strftime}
# - {payload!caption} : nei definition "publish" del topic deve essere presente "payload": { "FIELD": { "caption": "..." } }
#
class PayloadDict(collections.abc.MutableMapping, dict):
  def __getitem__(self, key):
    default = '-'
    m = re.search(r'^(.*)!(strftime|caption|\'.*\')(?:|\((.*)\))$', key)
    if m and dict.__contains__(self, m.group(1)):
      if m.group(2) == 'strftime':
        try:
          v = int(dict.__getitem__(self, m.group(1)))
          return datetime.datetime.fromtimestamp(v).strftime(m.group(3) if m.group(3) else '%Y-%m-%d %H:%M:%S') if v > 0 else '-'
        except:
          return default
      elif m.group(2) == 'caption' and self.definition and 'payload' in self.definition and m.group(1) in self.definition['payload']:
        v = str(dict.__getitem__(self, m.group(1)))
        try:
          if v in self.definition['payload'][m.group(1)] and 'caption' in self.definition['payload'][m.group(1)][v]:
            return self.definition['payload'][m.group(1)][v]['caption']
          elif int(v) in self.definition['payload'][m.group(1)] and 'caption' in self.definition['payload'][m.group(1)][int(v)]:
            return self.definition['payload'][m.group(1)][int(v)]['caption']
          else:
            return default
        except ValueError:
          return default
      else:
        key = m.group(1)
    elif m and m.group(2)[0] == "'":
      default = m.group(2)[1:-1]
    if dict.__contains__(self, key):
      return dict.__getitem__(self, key)
    return default

  def __setitem__(self, key, payload):
    dict.__setitem__(self, key, payload)
    
  def __delitem__(self, key):
    dict.__delitem__(self, key)
    
  def __iter__(self):
    return dict.__iter__(self)
  
  def __len__(self):
    return dict.__len__(self)
  
  def __contains__(self, x):
    return dict.__contains__(self, x)
  
  def load(self, adict, definition):
    self.definition = definition
    for k in adict:
      if isinstance(adict[k], dict):
        p = PayloadDict()
        p.load(adict[k], None)
        dict.__setitem__(self, k, p)
      else:
        dict.__setitem__(self, k, adict[k])

def getPayloadItem(v, definition):
  if isinstance(v, dict):
    p = PayloadDict()
    p.load(v, definition)
    return p
  return v
