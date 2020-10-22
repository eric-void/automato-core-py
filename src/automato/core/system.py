#!/usr/bin/python3
# require python3
# -*- coding: utf-8 -*-

# Use True to define that field as "mergeable" (when generating a single declaration from multiple entry topic declarations), False as "non mergeable" (that field will not be in merged declaration, even if there is only a single declaration)
ENTRY_DEFINITION_EXPORTABLE = {
  'type': True,
  'caption': True,
  'description': True,
  'config': True,
  'required': True,
  'publish': {
    'description': True,
    'type': True,
    'qos': True,
    'retain': True,
    'payload': True,
    'payload_transform': True,
    'notify': True,
    'notify_handler': True,
    'notify_type': True,
    'notify_level': True,
    'notify_change_level': True,
    'notify_change_duration': True,
    'notify_if': True,
    "events": True,
    "topic_match_priority": True,
  },
  'subscribe': {
    'description': True,
    "topic_syntax": True,
    "topic_syntax_description": True,
    "payload_syntax": True,
    'response': True,
    'type': True,
    'notify_type': True,
    'notify_level': True,
    'notify_change_level': True,
    'notify_if': True,
    "actions": True,
    "topic_match_priority": True,
  },
  'events_passthrough': True,
}
PRIMITIVE_TYPES = (int, str, bool, float, dict, list)
EVENT_KEYS = ['port']

index_topic_cache = { 'hits': 0, 'miss': 0, 'data': {} }
INDEX_TOPIC_CACHE_MAXSIZE = 1024
INDEX_TOPIC_CACHE_PURGETIME = 3600

import time as core_time
import logging
import sys
import re
import copy
import json
import threading

from automato.core import mqtt
from automato.core import system_extra
from automato.core import scripting_js
from automato.core import notifications
from automato.core import utils

destroyed = False
test_mode = False

def set_config(_config):
  global config
  config = _config

def boot():
  global config
  
  system_extra.init_locale(config)
  system_extra.init_logging(config)
  _reset()
  
  mqtt.init()
  mqtt_config = {
    'client_id': 'automato-node',
    'cache': True, 
    'all': True, 
    'subscribe': { '[all]': _on_mqtt_message },
    'publish_before_connection_policy': 'queue', # connect, queue, error
    'check_broken_connection': 5,
  }
  if 'messages-log' in config:
    mqtt_config["message-logger"] = 'messages'
    
  mqtt.config(mqtt_config) # pre configuration (if someone tries to use broker before init_mqtt - for example in a module init hook - it looks at this config)
  if 'mqtt' in config:
    mqtt.config(config['mqtt'])

def _reset():
  global handler_on_entry_load, handler_on_entry_install, handler_on_loaded, handler_on_initialized, handler_on_message, handler_on_all_events
  handler_on_entry_load = []
  handler_on_entry_install = []
  handler_on_loaded = []
  handler_on_initialized = []
  handler_on_message = []
  handler_on_all_events = []

def on_entry_load(handler):
  global handler_on_entry_load
  handler_on_entry_load.append(handler)

def on_entry_install(handler):
  global handler_on_entry_install
  handler_on_entry_install.append(handler)

def on_loaded(handler):
  global handler_on_loaded
  handler_on_loaded.append(handler)

def on_initialized(handler):
  global handler_on_initialized
  handler_on_initialized.append(handler)
  
def on_message(handler):
  global handler_on_message
  handler_on_message.append(handler)

def on_all_events(handler):
  global handler_on_all_events
  handler_on_all_events.append(handler)

def _mqtt_connect_callback(callback, phase):
  if phase == 2:
    callback()

def init(callback):
  global destroyed, config, all_entries, all_nodes, exports, subscriptions, last_entry_and_events_for_received_mqtt_message, events_listeners, events_published, events_published_lock, index_topic_published, index_topic_subscribed, default_node_name, subscribed_response, subscription_thread
  destroyed = False
  all_entries = {}
  all_nodes = {}
  exports = {}
  subscriptions = {}
  last_entry_and_events_for_received_mqtt_message = None
  events_listeners = {}
  events_published = {}
  events_published_lock = threading.Lock()
  index_topic_published = {}
  index_topic_subscribed = {}
  topic_cache_reset()
  default_node_name = config['name'] if 'name' in config else 'root'
  
  scripting_js.exports = exports
  
  subscribed_response = []
  subscription_thread = threading.Thread(target = _subscription_timer_thread, daemon = True) # daemon = True allows the main application to exit even though the thread is running. It will also (therefore) make it possible to use ctrl+c to terminate the application
  subscription_thread.start()
  
  notifications.init()
  
  entry_load_definitions(config['entries'], node_name = default_node_name, initial = True, id_from_definition = True)

  if handler_on_initialized:
    for h in handler_on_initialized:
      h(all_entries)

  mqtt.connect(lambda phase: _mqtt_connect_callback(callback, phase))
  
def destroy():
  global destroyed, subscription_thread
  destroyed = True
  notifications.destroy()
  subscription_thread.join()
  _stats_show()
  logging.debug('SYSTEM> Disconnecting from mqtt broker ...')
  mqtt.destroy()
  mqtt.config({ 'subscribe': {} })
  _reset()

def broker():
  return mqtt





###############################################################################################################################################################
#
# PUBLIC
#
###############################################################################################################################################################

# NOTE Use datetime.datetime.now().astimezone() for a "timezoned" datetime

system_time_paused = 0
system_time_offset = 0

def time():
  global system_time_offset, system_time_paused
  return int((core_time.time() if not system_time_paused else system_time_paused) + system_time_offset)

def timems():
  global system_time_offset, system_time_paused
  return int(((core_time.time() if not system_time_paused else system_time_paused) + system_time_offset) * 1000)

def time_offset(v):
  global system_time_offset, system_time_paused
  system_time_offset = system_time_offset + v

def time_set(v):
  global system_time_offset, system_time_paused
  system_time_offset = v - int(core_time.time())
  
def time_pause():
  global system_time_offset, system_time_paused
  if not system_time_paused:
    system_time_paused = int(core_time.time())

def time_resume():
  global system_time_offset, system_time_paused
  if system_time_paused:
    system_time_offset = system_time_offset - (int(core_time.time()) - system_time_paused)
    system_time_paused = 0

def sleep(seconds):
  core_time.sleep(seconds)

_stats = {}
_stats_start_t = 0
_stats_show_t = 0

DEBUG_STATS_INTERVAL = 600 # every X seconds log stats of timings

def _stats_start():
  global _stats, _stats_start_t, _stats_show_t
  t = core_time.time() * 1000
  if not _stats_start_t:
    _stats_start_t = _stats_show_t = t
  if t - _stats_show_t > DEBUG_STATS_INTERVAL * 1000:
    _stats_show_t = t
    _stats_show()
  return t

def _stats_end(key, s):
  global _stats, _stats_start_t
  delta = core_time.time() * 1000 - s
  if not key in _stats:
    _stats[key] = { 'count': 0, 'total': 0, 'avg': 0, 'max': 0 }
  _stats[key]['count'] += 1
  _stats[key]['total'] += delta
  _stats[key]['avg'] = _stats[key]['total'] / _stats[key]['count']
  if delta > _stats[key]['max']:
    _stats[key]['max'] = delta

def _stats_show():
  global _stats, _stats_start_t, index_topic_cache #, _cache_topic_matches, _cache_topic_matches_hits, _cache_topic_matches_miss
  total = core_time.time() * 1000 - _stats_start_t
  ss = _stats
  stats = ''
  for s in sorted(ss):
    perc = round(ss[s]['total'] / total * 1000)
    if perc >= 5:
      stats += '    ' + str(s).ljust(80, " ") + ' (' + str(perc) + '‰): { count: ' + str(ss[s]['count']) + ', avg: ' + str(round(ss[s]['avg'])) + 'ms, max: ' + str(round(ss[s]['max'])) + 'ms }\n';
  logging.debug(('SYSTEM> DEBUG TIMINGS\n  total: {total}min\n' +
    '  mqtt_queue_delay: {delay}ms\n' + 
    '  script_eval_cache: {schits}/{sctotal} hits ({scperc}%), {scsize} size, {scskip} uncacheable, {scdisabled} cache disabled, {scsign} signatures\n' + 
    '  topic cache: {tphits}/{tptotal} ({tpperc}%) hits, {tpsize} size\n' + 
    '  system_stats:\n{stats}').format(
    total = round(total / 60000),
    delay = mqtt.queueDelay(),
    schits = scripting_js.script_eval_cache_hits, sctotal = scripting_js.script_eval_cache_hits + scripting_js.script_eval_cache_miss, scperc = round(scripting_js.script_eval_cache_hits * 100 / (scripting_js.script_eval_cache_hits + scripting_js.script_eval_cache_miss)) if (scripting_js.script_eval_cache_hits + scripting_js.script_eval_cache_miss) > 0 else 0, scsize = len(scripting_js.script_eval_cache), scskip =  scripting_js.script_eval_cache_skipped, scdisabled = scripting_js.script_eval_cache_disabled, scsign = len(scripting_js.script_eval_codecontext_signatures),
    tphits = index_topic_cache['hits'], tptotal = index_topic_cache['hits'] + index_topic_cache['miss'], tpperc = round(index_topic_cache['hits'] * 100 / (index_topic_cache['hits'] + index_topic_cache['miss'])) if (index_topic_cache['hits'] + index_topic_cache['miss']) > 0 else 0, tpsize = sum([len(index_topic_cache['data'][i]) for i in index_topic_cache['data']]),
    stats = stats
  ))
  # '  topic matches cache: {tmhits}/{tmtotal} ({tmperc}%) hits, {tmsize} size\n' + 
  #  tmhits = _cache_topic_matches_hits, tmtotal = _cache_topic_matches_hits + _cache_topic_matches_miss, tmperc = round(_cache_topic_matches_hits * 100 / (_cache_topic_matches_hits + _cache_topic_matches_miss)) if (_cache_topic_matches_hits + _cache_topic_matches_miss) > 0 else 0, tmsize = len(_cache_topic_matches),



###############################################################################################################################################################
#
# ENTRIES MANAGEMENT
#
###############################################################################################################################################################

class Entry(object):
  def __init__(self, entry_id, definition, config):
    global exports, default_node_name
    d = entry_id.find("@")
    if d < 0:
      logging.error("SYSTEM> Invalid entry id: {entry_id}, entry not loaded".format(entry_id = entry_id))
      return None
    
    self.id = entry_id
    self.id_local = entry_id[:d]
    self.node_name = entry_id[d + 1:]
    self.is_local = None
    self.node_config = config
    if 'type' not in definition:
      definition['type'] = 'module' if 'module' in definition else ('device' if 'device' in definition else 'item')
    self.type = definition['type']
    self.definition = definition
    self.caption = self.definition['caption'] if 'caption' in self.definition else self.id_local
    self.created = time()
    self.last_seen = 0
    self.exports = exports
    self.topic_rule_aliases = {}
    self.topic = entry_topic_lambda(self)
    self.publish = entry_publish_lambda(self)

def entry_load(definition, node_name = False, entry_id = False, replace_if_exists = True):
  global config, default_node_name, all_entries, all_nodes, handler_on_entry_load
  if not node_name:
    node_name = default_node_name
  
  if not entry_id:
    definition_id = definition['id'] if 'id' in definition else (definition['module'] if 'module' in definition else (definition['device'] if 'device' in definition else (definition['item'] if 'item' in definition else '')))
    d = definition_id.find("@")
    if d < 0:
      entry_id = re.sub('[^A-Za-z0-9_-]+', '-', definition_id)
      i = 0
      while entry_id + '@' + node_name in all_entries:
        entry_id = re.sub('[^A-Za-z0-9_-]+', '-', definition_id + '_' + str(++ i))
      entry_id = entry_id + '@' + node_name
    else:
      entry_id = definition_id
  if entry_id in all_entries:
    if not replace_if_exists:
      logging.error("SYSTEM> Entry id already exists: {entry_id}, entry not loaded".format(entry_id = entry_id))
      return None
    else:
      logging.debug("SYSTEM> Entry id already exists: {entry_id}, entry not loaded".format(entry_id = entry_id))
      entry_unload(entry_id)
  
  entry = Entry(entry_id, definition, config)
  
  if handler_on_entry_load:
    for h in handler_on_entry_load:
      h(entry)
  
  if not isinstance(entry.definition, dict):
    entry.definition = {}
  entry.config = entry.definition['config'] if 'config' in entry.definition else {}
  
  _entry_definition_normalize(entry, 'load')
  _entry_events_load(entry)
  _entry_actions_load(entry)
  
  all_entries[entry_id] = entry
  if entry.node_name not in all_nodes:
    all_nodes[entry.node_name] = {}

  return entry

def entry_unload(entry_id):
  global all_entries
  if entry_id in all_entries:
    _entry_remove_from_index(all_entries[entry_id])
    del all_entries[entry_id]

def entry_load_definitions(definitions, node_name = False, initial = False, unload_other_from_node = False, id_from_definition = False):
  """
  @param id_from_definition. If True: definitions = [ { ...definition...} ]; if False: definitions = { 'entry_id': { ... definition ... } }
  """
  global all_entries, default_node_name, handler_on_loaded, handler_on_entry_install
  
  if not node_name:
    node_name = default_node_name
  
  loaded = {}
  for definition in definitions:
    if not id_from_definition:
      entry_id = definition
      definition = definitions[entry_id]
    else:
      entry_id = False
    if 'disabled' not in definition or not definition['disabled']:
      entry = entry_load(definition, node_name = node_name, entry_id = entry_id)
      if entry:
        loaded[entry.id] = entry

  if handler_on_loaded:
    for h in handler_on_loaded:
      h(loaded, initial = initial)

  for entry_id, entry in loaded.items():
    _entry_definition_normalize(entry, 'loaded')
    _entry_definition_normalize(entry, 'init')
    _entry_events_install(entry)
    _entry_actions_install(entry)
    
    _entry_add_to_index(entry)
    
    if handler_on_entry_install:
      for h in handler_on_entry_install:
        h(entry)

  if unload_other_from_node:
    todo_unload = []
    for entry_id in all_entries:
      if all_entries[entry_id].node_name == node_name and entry_id not in loaded:
        todo_unload.append(entry_id)
    for entry_id in todo_unload:
      entry_unload(entry_id)

def entry_unload_node_entries(node_name):
  global all_entries
  todo_unload = []
  for entry_id in all_entries:
    if all_entries[entry_id].node_name == node_name:
      todo_unload.append(entry_id)
  for entry_id in todo_unload:
    entry_unload(entry_id)

def entries():
  global all_entries
  return all_entries




###############################################################################################################################################################
#
# GENERIC ENTRIES FUNCTIONS
#
###############################################################################################################################################################

def _entry_definition_normalize(entry, phase):
  if phase == 'load':
    if not 'publish' in entry.definition:
      entry.definition['publish'] = {}
    if not 'subscribe' in entry.definition:
      entry.definition['subscribe'] = {}
    if entry.is_local:
      if not 'entry_topic' in entry.definition:
        entry.definition['entry_topic'] = entry.type + '/' + entry.id_local
      if not 'topic_root' in entry.definition:
        entry.definition['topic_root'] = entry.definition['entry_topic']
    if not 'on' in entry.definition:
      entry.definition['on'] = {}
    if not 'events_listen' in entry.definition:
      entry.definition['events_listen'] = {}

  elif phase == 'loaded':
    for t in entry.definition['subscribe']:
      if 'publish' in entry.definition['subscribe'][t] and 'response' not in entry.definition['subscribe'][t]:
        entry.definition['subscribe'][t]['response'] = entry.definition['subscribe'][t]['publish']

    # events_passthrough translates in event_propagation via on/events definition
    if 'events_passthrough' in entry.definition:
      if isinstance(entry.definition['events_passthrough'], str):
        entry.definition['events_passthrough'] = [ entry.definition['events_passthrough'] ]
      for eventref in entry.definition['events_passthrough']:
        on_event(eventref, _on_events_passthrough_listener_lambda(entry), entry, 'events_passthrough')
  
  elif phase == 'init':
    for k in ['qos', 'retain']:
      if k in entry.definition:
        for topic_rule in entry.definition['publish']:
          if k not in entry.definition['publish'][topic_rule]:
            entry.definition['publish'][topic_rule][k] = entry.definition[k]

    if 'publish' in entry.definition:
      res = {}
      for topic_rule in entry.definition['publish']:
        if 'topic' in entry.definition['publish'][topic_rule]:
          entry.topic_rule_aliases[topic_rule] = entry.topic(entry.definition['publish'][topic_rule]['topic'])
        res[entry.topic(topic_rule)] = entry.definition['publish'][topic_rule]
      entry.definition['publish'] = res

    if 'subscribe' in entry.definition:
      res = {}
      for topic_rule in entry.definition['subscribe']:
        if 'topic' in entry.definition['subscribe'][topic_rule]:
          entry.topic_rule_aliases[topic_rule] = entry.topic(entry.definition['subscribe'][topic_rule]['topic'])
        res[entry.topic(topic_rule)] = entry.definition['subscribe'][topic_rule]
      entry.definition['subscribe'] = res

      for topic_rule in entry.definition['subscribe']:
        if 'response' in entry.definition['subscribe'][topic_rule]:
          res = []
          for t in entry.definition['subscribe'][topic_rule]['response']:
            if not isinstance(t, dict):
              t = { 'topic': t }
            if 'topic' in t and t['topic'] != 'NOTIFY':
              t['topic'] = entry.topic(t['topic'])
            res.append(t)
          entry.definition['subscribe'][topic_rule]['response'] = res
          
    for eventref in entry.definition['events_listen']:
      add_event_listener(eventref, entry, 'events_listen')

    notifications.entry_normalize(entry)

def _on_events_passthrough_listener_lambda(source_entry):
  return lambda entry, eventname, eventdata: _on_events_passthrough_listener(source_entry, entry, eventname, eventdata)
  
def _on_events_passthrough_listener(source_entry, entry, eventname, eventdata):
  _entry_event_publish_and_invoke_listeners(source_entry, eventname, eventdata['params'], eventdata['time'], '#events_passthrough')

"""
Extract the exportable portion [L.0]+[L.0N] of the full entry definition
"""
def _entry_definition_exportable(definition, export_config = None):
  if not export_config:
    export_config = ENTRY_DEFINITION_EXPORTABLE
  ret = {}
  for k in definition:
    if k in export_config and isinstance(definition[k], PRIMITIVE_TYPES):
      if not isinstance(export_config[k], dict):
        ret[k] = definition[k]
      elif isinstance(definition[k], dict):
        ret[k] = {}
        for kk in definition[k]:
          ret[k][kk] = _entry_definition_exportable(definition[k][kk], export_config[k])
  return ret

def _entry_add_to_index(entry):
  """
  Add the initialized entry (local or remote) to various indexes
  """
  entry.definition_exportable = _entry_definition_exportable(entry.definition)
  for topic in entry.definition_exportable['publish']:
    _entry_add_to_index_topic_published(entry, topic, entry.definition_exportable['publish'][topic])
  for topic in entry.definition_exportable['subscribe']:
    _entry_add_to_index_topic_subscribed(entry, topic, entry.definition_exportable['subscribe'][topic])

def _entry_remove_from_index(entry):
  for topic in entry.definition_exportable['publish']:
    _entry_remove_from_index_topic_published(entry, topic)
  for topic in entry.definition_exportable['subscribe']:
    _entry_remove_from_index_topic_subscribed(entry, topic)

def _entry_add_to_index_topic_published(entry, topic, definition):
  global index_topic_published
  if topic not in index_topic_published:
    index_topic_published[topic] = { 'definition': {}, 'entries': []}
  elif topic in index_topic_published[topic]['entries']:
    _entry_remove_from_index_topic_published(entry, topic, rebuild_definition = False)
  index_topic_published[topic]['definition'] = _entry_index_definition_build(index_topic_published[topic]['entries'], 'publish', topic, add_definition = definition)
  index_topic_published[topic]['entries'].append(entry.id)
  topic_cache_reset()
  
def _entry_remove_from_index_topic_published(entry, topic, rebuild_definition = True):
  global index_topic_published
  index_topic_published[topic]['entries'].remove(entry.id)
  if rebuild_definition:
    index_topic_published[topic]['definition'] = _entry_index_definition_build(index_topic_published[topic]['entries'], 'publish', topic)
  topic_cache_reset()

def _entry_add_to_index_topic_subscribed(entry, topic, definition):
  global index_topic_subscribed
  if topic not in index_topic_subscribed:
    index_topic_subscribed[topic] = { 'definition': {}, 'entries': []}
  elif topic in index_topic_subscribed[topic]['entries']:
    _entry_remove_from_index_topic_subscribed(entry, topic, rebuild_definition = False)
  index_topic_subscribed[topic]['definition'] = _entry_index_definition_build(index_topic_subscribed[topic]['entries'], 'subscribe', topic, add_definition = definition)
  index_topic_subscribed[topic]['entries'].append(entry.id)
  topic_cache_reset()
  
def _entry_remove_from_index_topic_subscribed(entry, topic, rebuild_definition = True):
  global index_topic_subscribed
  index_topic_subscribed[topic]['entries'].remove(entry.id)
  if rebuild_definition:
    index_topic_subscribed[topic]['definition'] = _entry_index_definition_build(index_topic_subscribed[topic]['entries'], 'subscribe', topic)
  topic_cache_reset()

def _entry_index_definition_build(entry_ids, mtype, topic, add_definition = None):
  definitions = []
  for entry_id in entry_ids:
    entry = entry_get(entry_id)
    definitions.append(entry.definition_exportable[mtype][topic])
  if add_definition:
    definitions.append(add_definition)
  return _entry_index_definition_build_merge(definitions, ENTRY_DEFINITION_EXPORTABLE[mtype])

def _entry_index_definition_build_merge(definitions, merge_config):
  res = {}
  for definition in definitions:
    for prop in definition:
      if (not prop in merge_config) or merge_config[prop]:
        if prop not in res:
          res[prop] = definition[prop]
        elif isinstance(res[prop], (int, str, bool, float)) and isinstance(definition[prop], (int, str, bool, float)):
          res[prop] = str(res[prop]) + ', ' + str(definition[prop])
        elif isinstance(res[prop], list) and isinstance(definition[prop], list):
          res[prop] = res[prop] + definition[prop]
        else:
          res[prop] = None
  return {k:v for k,v in res.items() if v is not None}

def entry_definition_add_default(entry, default):
  """
  Use this method in "system_loaded" hook to add definitions to an entry, to be intended as base definitions (node config and module definitions and "load" hook will override them)
  Don't use this method AFTER "system_loaded" hook: during "entry_init" phase definitions are processed and normalized, and changing them could result in runtime errors.
  """
  entry.definition = utils.dict_merge(default, entry.definition)

def entry_id_match(entry, reference):
  """
  Return if the id of the entry (reference) matches the one on the entry
  If the reference has no "@", only the local part is matched (so, if there are more entries with the same local part, every one of these is matched)
  """
  #OBSOLETE: return (entry.id == reference) or (reference.find("@") < 0 and entry.is_local and entry.id_local == reference)
  return (entry.id == reference) or (reference.find("@") < 0 and entry.id_local == reference)

def entry_id_find(entry_id):
  #OBSOLETE: global default_node_name
  #OBSOLETE: return entry_id + '@' + default_node_name if entry_id != "*" and entry_id.find("@") < 0 else entry_id
  global all_nodes, all_entries
  if entry_id == "*" or entry_id.find("@") >= 0:
    return entry_id
  for node_name in all_nodes:
    if (entry_id + "@" + node_name) in all_entries:
      return entry_id + "@" + node_name
  return None

def entry_get(entry_id, local = False):
  """
  OBSOLETE (with parameter local = False)
  global all_entries, default_node_name
  d = entry_id.find("@")
  if d < 0:
    return all_entries[entry_id + '@' + default_node_name] if entry_id + '@' + default_node_name in all_entries else None
  return all_entries[entry_id] if entry_id in all_entries and (not local or all_entries[entry_id].is_local) else None
  """
  global all_entries
  eid = entry_id_find(entry_id)
  return all_entries[eid] if eid else None

def entries_definition_exportable():
  global all_entries
  res = {}
  for entry_id in all_entries:
    #if all_entries[entry_id].type != 'module':
    res[entry_id] = all_entries[entry_id].definition_exportable
  return res

def message_payload_serialize(payload):
  return str(payload if not isinstance(payload, dict) else {x:payload[x] for x in sorted(payload)})

_re_topic_matches = re.compile('^(?:(?P<topic_simple>[a-zA-Z0-9#+_-][a-zA-Z0-9#+_/-]*)|/(?P<topic_regex>.*)/)' + '(?:\[(?:js:(?P<js_filter>.*)|/(?P<payload_regex_filter>.*)/|(?P<payload_simple_filter>.*))\])?$')
_re_topic_matches_cache = {} # TODO put a limit on _re_topic_matches_cache (now it can grows forever!)
#_cache_topic_matches = {}
#_cache_topic_matches_hits = 0
#_cache_topic_matches_miss = 0
#_cache_topic_sem = threading.Semaphore()
#CACHE_TOPIC_MATCHES_MAXSIZE=102400 # TODO Very high but should contain AT LEAST # of subscribed+published topic_rules * # of topic published on broker. It should be auto-compiled? Or, if it's too high, the management of _cache_topic_matches should be better
#CACHE_TOPIC_MATCHES_PURGETIME=3600

def topic_matches(rule, topic, payload = None):
  """
  @param rule a/b | a/# | /^(.*)$/ | a/b[payload1|payload2] | a/#[/^(payload.*)$/] | /^(.*)$/[js: payload['a']==1 && matches[1] == 'x']
  @param payload if "None" payload is NOT checked, and result['matched'] is based ONLY on topic rule (extra rules are NOT checked)
  """
  global _re_topic_matches_cache #_cache_topic_matches, _cache_topic_matches_hits, _cache_topic_matches_miss, _cache_topic_sem
  
  """
  key = rule + "|" + topic
  if key in _cache_topic_matches:
    if _cache_topic_matches[key]['use_payload']:
      key = key + "[" + message_payload_serialize(payload) + "]"
      if key in _cache_topic_matches:
        _cache_topic_matches[key]['used'] = time()
        _cache_topic_matches_hits += 1
        return _cache_topic_matches[key]
    else:
      _cache_topic_matches[key]['used'] = time()
      _cache_topic_matches_hits += 1
      return _cache_topic_matches[key]
  _cache_topic_matches_miss += 1
  """

  result = {
    'use_payload': None, # True if payload is needed for match
    'topic_matches': None, # If topic part of the rule matches, this is the list of regexp groups (or [True] for no-regexp matchs). WARN: This is filled even if the whole rule is not matched (if topic part matches but payload part unmatch)
    'matched': False, # Full match result
    'used': time()
  }
  m = _re_topic_matches.match(rule)
  m = m.groupdict() if m else None
  if m:
    if m['topic_simple']:
      result['topic_matches'] = [True] if mqtt.topicMatchesMQTT(m['topic_simple'], topic) else []
    elif m['topic_regex']:
      try:
        # TODO put a limit on _re_topic_matches_cache
        if not m['topic_regex'] in _re_topic_matches_cache:
          _re_topic_matches_cache[m['topic_regex']] = re.compile(m['topic_regex'])
        mm = _re_topic_matches_cache[m['topic_regex']].match(topic)
        result['topic_matches'] = ([mm.group(0)] + list(mm.groups())) if mm else []
      except:
        logging.exception("SYSTEM> Regexp error in message rule: {rule}".format(rule = rule))
    if result['topic_matches']:
      result['use_payload'] = False
      result['matched'] = True
      if payload is not None:
        if m['payload_simple_filter']:
          result['use_payload'] = True
          result['matched'] = payload in m['payload_simple_filter'].split("|")
        elif m['payload_regex_filter']:
          result['use_payload'] = True
          try:
            mm = re.search(m['payload_regex_filter'], payload) # TODO Dovrebbe essere fatto sul payload originale prima del decode JSON - quando passero mqttMessage si potrà fare
            result['matched'] = True if mm else False
          except:
            logging.exception("SYSTEM> Regexp error in message rule (payload part): {rule}".format(rule = rule))
        elif m['js_filter']:
          if m['js_filter'].find("payload") >= 0:
            result['use_payload'] = True
          ret = scripting_js.script_eval(m['js_filter'], {"topic": topic, "payload": payload, "matches": result['topic_matches']}, to_dict = True, cache = True)
          result['matched'] = True if ret else False
  else:
    logging.error("SYSTEM> Invalid message rule: {rule}".format(rule = rule))
  
  """
  key = rule + "|" + topic
  if result['use_payload']:
    if not key in _cache_topic_matches:
      _cache_topic_matches[key] = { 'use_payload': True, 'used': time() }
    key = key + "[" + message_payload_serialize(payload) + "]"

  _cache_topic_sem.acquire()
  _cache_topic_matches[key] = result
  _cache_topic_matches[key]['used'] = time()
    
  if len(_cache_topic_matches) > CACHE_TOPIC_MATCHES_MAXSIZE:
    t = CACHE_TOPIC_MATCHES_PURGETIME
    while len(_cache_topic_matches) > CACHE_TOPIC_MATCHES_MAXSIZE:
      logging.debug("SYSTEM> Reducing cache_topic_matches ({len}) ...".format(len = len(_cache_topic_matches)))
      _cache_topic_matches = {x:_cache_topic_matches[x] for x in _cache_topic_matches if _cache_topic_matches[x]['used'] > time() - t}
      t = t / 2 if t > 1 else -1

  _cache_topic_sem.release()
  """
  
  return result

_re_topic_match_priority = re.compile('^(topic|notify.*|description)$')

def topic_match_priority(definition):
  global _re_topic_match_priority
  if 'topic_match_priority' in definition:
    return definition['topic_match_priority']
  for k in definition:
    if not _re_topic_match_priority.match(k):
      return 1
  return 0

def topic_cache_reset():
  global index_topic_cache
  index_topic_cache = { 'hits': 0, 'miss': 0, 'data': { } }

def topic_cache_find(index, cache_key, topic, payload = None):
  """
  @param index is { topic:  { 'definition': { ... merged definition from all TOPIC published by entries ... }, 'entries': [ ... entry ids ... ]};
  @return (0: topic rule found, 1: topic metadata { 'definition': { ... merged definition from all topic published by entries ... }, 'entries': [ ... entry ids ... ]}, 2: matches)
  """
  global index_topic_cache
  
  if cache_key not in index_topic_cache['data']:
    index_topic_cache['data'][cache_key] = {}

  if len(index_topic_cache['data'][cache_key]) > INDEX_TOPIC_CACHE_MAXSIZE:
    t = INDEX_TOPIC_CACHE_PURGETIME
    while len(index_topic_cache['data'][cache_key]) > INDEX_TOPIC_CACHE_MAXSIZE:
      index_topic_cache['data'][cache_key] = {x:index_topic_cache['data'][cache_key][x] for x in index_topic_cache['data'][cache_key] if index_topic_cache['data'][cache_key][x]['used'] > time() - t}
      t = t / 2 if t > 1 else -1

  topic_and_payload = None
  if topic in index_topic_cache['data'][cache_key]:
    if not 'use_payload' in index_topic_cache['data'][cache_key][topic]:
      index_topic_cache['data'][cache_key][topic]['used'] = time()
      index_topic_cache['hits'] += 1
      return index_topic_cache['data'][cache_key][topic]['result']
    else:
      topic_and_payload = topic + "[" + message_payload_serialize(payload) + "]"
      if topic_and_payload in index_topic_cache['data'][cache_key]:
        index_topic_cache['data'][cache_key][topic]['used'] = time()
        index_topic_cache['data'][cache_key][topic_and_payload]['used'] = time()
        index_topic_cache['hits'] += 1
        return index_topic_cache['data'][cache_key][topic_and_payload]['result']
  
  index_topic_cache['miss'] += 1
  res = { 'used': time(), 'result': [] }
  use_payload = False
  for itopic in index:
    m = topic_matches(itopic, topic, payload)
    if m['matched']:
      res['result'].append((itopic, index[itopic], m['topic_matches']))
    if m['use_payload']:
      use_payload = True
  if not use_payload:
    index_topic_cache['data'][cache_key][topic] = res
  else:
    index_topic_cache['data'][cache_key][topic] = { 'use_payload': True, 'used': time() }
    if topic_and_payload is None:
      topic_and_payload = topic + "[" + message_payload_serialize(payload) + "]"
    index_topic_cache['data'][cache_key][topic_and_payload] = res

  return res['result']

def topic_published_definition_is_internal(definition):
  return not (definition and ('description' in definition or 'type' in definition))

def topic_published_definition(topic, payload = None, strict_match = False):
  """
  Return published definition, if present, of a published topic
  If multiple publish are found, first not internal is returned, if present (use "entries_publishers_of" if you want them all)
  """
  global index_topic_published
  if strict_match:
    if topic in index_topic_published:
      return index_topic_published[topic]['definition']
  else:
    ret = topic_cache_find(index_topic_published, 'published', topic, payload)
    if ret:
      for t in ret:
        if not topic_published_definition_is_internal(t[1]['definition']):
          return t[1]['definition']
      return ret[0][1]['definition']
  return None

def topic_subscription_list():
  global index_topic_subscribed
  return list(index_topic_subscribed.keys())

def topic_subscription_definition_is_internal(definition):
  return not (definition and ('description' in definition or 'response' in definition))

def topic_subscription_is_internal(topic, payload = None, strict_match = False):
  definition = topic_subscription_definition(topic, payload, strict_match)
  return topic_subscription_definition_is_internal(definition)

def topic_subscription_definition(topic, payload = None, strict_match = False):
  """
  Return subscription definition, if present, of a published topic
  If multiple subscriptions are found, first not internal is returned, if present (use "entries_subscribed_to" if you want them all)
  """
  global index_topic_subscribed
  if strict_match:
    if topic in index_topic_subscribed:
      return index_topic_subscribed[topic]['definition']
  else:
    ret = topic_cache_find(index_topic_subscribed, 'subscribed', topic, payload)
    if ret:
      for t in ret:
        if not topic_subscription_definition_is_internal(t[1]['definition']):
          return t[1]['definition']
      return ret[0][1]['definition']
  return None

def entries_publishers_of(topic, payload = None, strict_match = False):
  """
  Search for all entries that can publish the topic passed, and return all topic metadatas (and matches, if subscribed with a regex pattern)
  return {
    'ENTRY_ID': {
      'ENTRY_TOPIC': {
        'entry': [object],
        'definition': { ... },
        'matches': []
      }
    }
  }
  """
  global index_topic_published
  res = {}
  
  if strict_match:
    if topic in index_topic_published:
      for entry_id in index_topic_published[topic]['entries']:
        entry = entry_get(entry_id)
        if entry:
          res[entry_id] = {
            'entry': entry,
            'definition': entry.definition['publish'][topic],
            'topic': topic,
            'matches': [],
          }
        else:
          logging.error("SYSTEM> Internal error, entry references in index_topic_published not found: {entry_id}".format(entry_id = entry_id))

  else:
    ret = topic_cache_find(index_topic_published, 'published', topic, payload)
    for t in ret:
      for entry_id in t[1]['entries']:
        entry = entry_get(entry_id)
        if entry:
          if entry_id in res and topic_match_priority(entry.definition['publish'][t[0]]) > topic_match_priority(res[entry_id]['definition']):
            del res[entry_id]
          if not entry_id in res:
            res[entry_id] = {
              'entry': entry,
              'definition': entry.definition['publish'][t[0]],
              'topic': t[0],
              'matches': t[2],
            }
        else:
          logging.error("SYSTEM> Internal error, entry references in index_topic_published not found: {entry_id}".format(entry_id = entry_id))
          
  return res

def entries_subscribed_to(topic, payload = None, strict_match = False):
  """
  Search for all entries subscribed to that topic, and return all topic metadatas (and matches, if subscribed with a regex pattern)
  """
  
  global index_topic_subscribed
  res = {}
  
  if strict_match:
    if topic in index_topic_subscribed:
      for entry_id in index_topic_subscribed[topic]['entries']:
        entry = entry_get(entry_id)
        if entry:
          res[entry_id] = {
            'entry': entry,
            'definition': entry.definition['subscribe'][topic],
            'topic': topic,
            'matches': [],
          }
        else:
          logging.error("SYSTEM> Internal error, entry references in index_topic_subscribed not found: {entry_id}".format(entry_id = entry_id))
  else:
    ret = topic_cache_find(index_topic_subscribed, 'subscribed', topic, payload)
    for t in ret:
      for entry_id in t[1]['entries']:
        entry = entry_get(entry_id)
        if entry:
          if entry_id in res and topic_match_priority(entry.definition['subscribe'][t[0]]) > topic_match_priority(res[entry_id]['definition']):
            del res[entry_id]
          if not entry_id in res:
            res[entry_id] = {
              'entry': entry,
              'definition': entry.definition['subscribe'][t[0]],
              'topic': t[0],
              'matches': t[2],
            }
        else:
          logging.error("SYSTEM> Internal error, entry references in index_topic_subscribed not found: {entry_id}".format(entry_id = entry_id))
  return res






###############################################################################################################################################################
#
# MANAGE MESSAGES PUBLISHED ON BROKER
#
###############################################################################################################################################################

class Message(object):
  def __init__(self, topic, payload, qos = None, retain = None, payload_source = None, received = 0):
    self.topic = topic
    self.payload = payload
    self.payload_source = payload_source
    self.qos = qos
    self.retain = retain
    self.received = received # timestamp in ms. If 0, it's a message created by code, but not really received
    self._publishedMessages = None
    self._firstPublishedMessage = None
    self._subscribedMessages = None
    self._events = None
  
  def publishedMessages(self):
    if self._publishedMessages is None:
      self._publishedMessages = []
      _s = _stats_start()
      entries = entries_publishers_of(self.topic, self.payload)
      _stats_end('Message.publishedMessages().find', _s)
      _stats_end('Message(' + self.topic + ').publishedMessages().find', _s)
      
      _s = _stats_start()
      self._publishedMessages = []
      for entry_id in entries:
        self._publishedMessages.append(PublishedMessage(self, entries[entry_id]['entry'], entries[entry_id]['topic'], entries[entry_id]['definition'], entries[entry_id]['matches'] if entries[entry_id]['matches'] != [True] else []))
      _stats_end('Message.publishedMessages().create', _s)
      _stats_end('Message(' + self.topic + ').publishedMessages().create', _s)

    return self._publishedMessages

  def firstPublishedMessage(self):
    """
    Return first publishedMessaged NOT internal (if present), or internal (if no NOT internal is found)
    """
    if self._publishedMessages and self._firstPublishedMessage is None:
      for pm in self.publishedMessages():
        if not pm.internal:
          self._firstPublishedMessage = pm
          break
      if self._firstPublishedMessage is None:
        for pm in self.publishedMessages():
          self._firstPublishedMessage = pm
    return self._firstPublishedMessage;
    
  def subscribedMessages(self):
    if self._subscribedMessages is None:
      self._subscribedMessages = []
      _s = _stats_start()
      entries = entries_subscribed_to(self.topic, self.payload)
      _stats_end('Message.subscribedMessages().find', _s)
      _stats_end('Message(' + self.topic + ').subscribedMessages().find', _s)
      
      _s = _stats_start()
      self._subscribedMessages = []
      for entry_id in entries:
        self._subscribedMessages.append(SubscribedMessage(self, entries[entry_id]['entry'], entries[entry_id]['topic'], entries[entry_id]['definition'], entries[entry_id]['matches'] if entries[entry_id]['matches'] != [True] else []))
      _stats_end('Message.subscribedMessages().create', _s)
      _stats_end('Message(' + self.topic + ').subscribedMessages().create', _s)

    return self._subscribedMessages
  
  def events(self):
    if self._events is None:
      self._events = []
      for pm in self.publishedMessages():
        self._events += pm.events()
    return self._events
  
  def copy(self):
    m = Message(self.topic, copy.deepcopy(self.payload), self.qos, self.retain, self.payload_source, self.received)
    m._publishedMessages = self._publishedMessages
    m._firstPublishedMessage = self._firstPublishedMessage
    m._subscribedMessages = self._subscribedMessages
    m._events = self._events
    return m

class PublishedMessage(object):
  def __init__(self, message, entry, topic_rule, definition, matches, do_copy = False):
    self.message = message
    self.entry = entry
    self.topic_rule = topic_rule
    self.definition = definition
    self.topic = message.topic if not do_copy else copy.deepcopy(message.topic)
    # NOTE: payload in PublishedMessage could be different from payload in message (if 'payload_transform' is in definition)
    _s = _stats_start()
    self.payload = (message.payload if not do_copy else copy.deepcopy(message.payload)) if 'payload_transform' not in definition else _entry_transform_payload(entry, message.topic, message.payload, definition['payload_transform'])
    _stats_end('PublishedMessages.payload_transformed', _s)
    self.matches = matches if not do_copy else copy.deepcopy(matches)
    self.internal = topic_published_definition_is_internal(self.definition)
    self._events = None
    self._notification = None
    
  def events(self):
    if self._events is None:
      global events_listeners, config
      self._events = []
      
      if 'events' in self.definition:
        for eventname in self.definition['events']:
          if ":" not in eventname:
            eventdefs = self.definition['events'][eventname] if isinstance(self.definition['events'][eventname], list) else [ self.definition['events'][eventname] ]
            for eventdef in eventdefs:
              if ('listen_all_events' in config and config['listen_all_events']) or (eventname in events_listeners and ("*" in events_listeners[eventname] or self.entry.id in events_listeners[eventname] or self.entry.id_local in events_listeners[eventname])):
                _s = _stats_start()
                event = _entry_event_process(self.entry, eventname, eventdef, self)
                _stats_end('PublishedMessages.event_process', _s)
                if event:
                  _s = _stats_start()
                  eventdata = _entry_event_publish(self.entry, event['name'], event['params'], time() if not self.message.retain else 0)
                  _stats_end('PublishedMessages.event_publish', _s)
                  self._events.append(eventdata)
    return self._events
  
  def _notificationBuild(self):
    self._notification = notifications.notification_build(self)
  
  def notificationString(self):
    if self._notification is None:
      self._notificationBuild()
    return self._notification['notification_string']
  
  def notificationLevel(self):
    if self._notification is None:
      self._notificationBuild()
    return self._notification['notification_level']
    
  def notificationLevelString(self):
    if self._notification is None:
      self._notificationBuild()
    return self._notification['notification_slevel']

class SubscribedMessage(object):
  def __init__(self, message, entry, topic_rule, definition, matches, do_copy = False):
    self.message = message
    self.entry = entry
    self.topic_rule = topic_rule
    self.definition = definition
    self.topic = message.topic if not do_copy else copy.deepcopy(message.topic)
    self.payload = message.payload if not do_copy else copy.deepcopy(message.payload)
    self.matches = matches if not do_copy else copy.deepcopy(matches)
    self.internal = topic_subscription_definition_is_internal(self.definition)

  def copy(self):
    m = SubscribedMessage(self.message, self.entry, self.topic_rule, self.definition, self.matches, do_copy = True)
    return m

_current_received_message = None

def _on_mqtt_message(topic, payload_source, payload, qos, retain, matches, timems):
  """
  This handler is called for ALL messages received by broker. It finds if they are related to a topic published by an entry and manages it.
  This is called AFTER mqtt_on_subscribed_message (called only for subscribed topic)
  """
  m = Message(topic, payload, qos, retain, payload_source, received = timems)
  global _current_received_message
  _current_received_message = threading.local()
  _current_received_message.message = m
  
  # invoke events listeners
  _s = _stats_start()
  for pm in m.publishedMessages():
    pm.entry.last_seen = int(timems / 1000)
    for eventdata in pm.events():
      _entry_event_invoke_listeners(pm.entry, eventdata, 'message', pm)
  _stats_end('on_mqtt_message.invoke_listeners', _s)
  _stats_end('on_mqtt_message(' + str(topic) + ').invoke_listeners', _s)
  
  # manage responses callbacks
  _s = _stats_start()
  _subscribed_response_on_message(m)
  _stats_end('on_mqtt_message.subscribed_response', _s)
  _stats_end('on_mqtt_message(' + str(topic) + ').subscribed_response', _s)

  # call external handlers
  _s = _stats_start()
  global handler_on_message
  if handler_on_message:
    for h in handler_on_message:
      h(m.copy())
  _stats_end('on_mqtt_message.handlers', _s)
  _stats_end('on_mqtt_message(' + str(topic) + ').handlers', _s)

def current_received_message():
  global _current_received_message
  return _current_received_message.message if _current_received_message is not None and hasattr(_current_received_message, 'message') else None

def _entry_event_process(entry, eventname, eventdef, published_message):
  ret = scripting_js.script_eval(eventdef, {"topic": published_message.topic, "payload": published_message.payload, "matches": published_message.matches}, to_dict = True, cache = True)
  if ret == True:
    ret = {}
  return { 'name': eventname, 'params': ret } if ret != None and ret != False else None

def _entry_transform_payload(entry, topic, payload, transformdef):
  return scripting_js.script_eval(transformdef, {"topic": topic, "payload": payload}, to_dict = True, cache = True)

def entry_event_keys(entry, eventname):
  return entry.events_keys[eventname] if eventname in entry.events_keys else (entry.definition['event_keys'] if 'event_keys' in entry.definition else EVENT_KEYS);

def entry_event_keys_index(key_values, temporary = False):
  return ("T:" if temporary else "") + utils.json_sorted_encode(key_values)
  
def entry_event_keys_index_is_temporary(keys_index):
  return keys_index.startswith("T:")

def _entry_event_publish(entry, eventname, params, time):
  """
  Given an event generated (by a published messaged, or by an event passthrough), process it's params to generate event data and store it's content in events_published history var
  Note: if an event has no "event_keys", it's data will be setted to all other stored data (of events with keys_index). If a new keys_index occours, the data will be merged with "no params-key" data.
  @param time timestamp event has been published, 0 if from a retained message, -1 if from an event data initialization
  """
  global events_published, events_published_lock
  #logging.debug("SYSTEM> Published event " + entry.id + "." + eventname + " = " + str(params))
  
  data = { 'name': eventname, 'time': time, 'params': params, 'changed_params': {}, 'keys': {} }
  event_keys = entry_event_keys(entry, eventname)
  data['keys'] = {k:params[k] for k in event_keys if params and k in params}
  keys_index = entry_event_keys_index(data['keys'])

  # If this is a new keys_index, i must merge data with empty keys_index (if present)
  if keys_index != '{}' and eventname in events_published and entry.id in events_published[eventname] and '{}' in events_published[eventname][entry.id] and keys_index not in events_published[eventname][entry.id]:
    for k in events_published[eventname][entry.id]['{}']['params']:
      if k not in params:
        params[k] = events_published[eventname][entry.id]['{}']['params'][k]
  
  __entry_event_publish_store(entry, eventname, keys_index, data, time, event_keys)
  
  # If this is an empty keys_index, i must pass data to other stored data with keys_index (i can ignore temporary data)
  if keys_index == '{}' and eventname in events_published and entry.id in events_published[eventname]:
    p = [p for p in events_published[eventname][entry.id] if not entry_event_keys_index_is_temporary(p)]
    for keys_index2 in p:
      if keys_index2 != '{}':
        data2 = { 'name': eventname, 'time': time, 'params': copy.deepcopy(params), 'changed_params': {}, 'keys': events_published[eventname][entry.id][keys_index2]['keys'] }
        __entry_event_publish_store(entry, eventname, keys_index2, data2, time, event_keys)
  
  return data

def __entry_event_publish_store(entry, eventname, keys_index, data, time, event_keys):
  global events_published, events_published_lock
  with events_published_lock:
    # Extract changed params (from previous event detected)
    if eventname in events_published and entry.id in events_published[eventname] and keys_index in events_published[eventname][entry.id]:
      for k in data['params']:
        if k not in event_keys and (k not in events_published[eventname][entry.id][keys_index]['params'] or data['params'][k] != events_published[eventname][entry.id][keys_index]['params'][k]):
          data['changed_params'][k] = data['params'][k]
      for k in events_published[eventname][entry.id][keys_index]['params']:
        if k not in data['params']:
          data['params'][k] = events_published[eventname][entry.id][keys_index]['params'][k]
    else:
      for k in data['params']:
        if k not in event_keys:
          data['changed_params'][k] = data['params'][k]
  
    if not eventname in events_published:
      events_published[eventname] = {}
    if not entry.id in events_published[eventname]:
      events_published[eventname][entry.id] = {}
    if (time < 0) or (not isinstance(data['params'], dict)) or ('temporary' not in data['params']) or (not data['params']['temporary']):
      events_published[eventname][entry.id][keys_index] = data
    elif 'temporary' in data['params'] and data['params']['temporary']:
      events_published[eventname][entry.id][entry_event_keys_index(data['keys'], True)] = data
  
  return data

def event_get_invalidate_on_action(entry, action, full_params, if_event_not_match_decoded = None):
  # Devo invalidare dalla cache di event_get tutti gli eventi che potrebbero essere interessati da questo action
  # 1. Se ho if_event_not_match mi baso sul "condition" impostato li per il reset. Se condition non c'è, deve resettare tutte le cache dell'evento
  # 2. Altrimenti prendo i params della action prima di trasformarli nel payload (quindi dopo aver applicat init e actiondef['init']), prendo solo gli "EVENT_KEYS" e li trasformo in una condition. Anche qui, se non ci sono dati utili la condition è vuota e resetta tutto.
  # ATTENZIONE: Se la gestione di 'port' o 'channel' (o altri event_keys) avviene direttamente nella definizione della action (quindi non dentro degli init, o nei parametri passati alla action, ma nel codice js che trasforma parametri in payload) non posso rilevarli e quindi la cache invalida tutto (e non solo i parametri interessati)
  
  global events_published, events_published
  with events_published_lock:
    eventname = transform_action_name_to_event_name(action)
    if eventname in events_published and entry.id in events_published[eventname]:
      
      if if_event_not_match_decoded:
        condition = if_event_not_match_decoded['condition']
      else:
        event_keys = entry_event_keys(entry, eventname)
        condition = " && ".join([ "params['" + k + "'] == " + json.dumps(full_params[k]) for k in event_keys if full_params and k in full_params])

      to_delete = []
      for keys_index in events_published[eventname][entry.id]:
        if not condition or _entry_event_params_match_condition(events_published[eventname][entry.id][keys_index], condition):
          to_delete.append(keys_index)
      for i in to_delete:
        del events_published[eventname][entry.id][i]

def _entry_event_invoke_listeners(entry, eventdata, caller, published_message = None):
  """
  Call this method when an entry should emit an event
  This invokes the event listeners of the entry
  @params eventdata contains { "params": ..., "changed_params": ...}
  """
  global events_listeners, handler_on_all_events
  
  #logging.debug("_entry_event_invoke_listeners " + str(eventdata) + " | " + str(events_listeners))
  eventname = eventdata['name']
  if eventname in events_listeners:
    for entry_ref in events_listeners[eventname]:
      if entry_ref == '*' or entry_id_match(entry, entry_ref):
        #logging.debug("_entry_event_invoke_listeners_match" + str(events_listeners[eventname][entry_ref]))
        for listener, condition in events_listeners[eventname][entry_ref]:
          _s = _stats_start()
          if condition is None or _entry_event_params_match_condition(eventdata, condition):
            #logging.debug("_entry_event_invoke_listeners_GO")
            listener(entry, eventname, eventdata)
          _stats_end('event_listener(' + str(listener) + '|' + str(condition) + ')', _s)
            
  if handler_on_all_events:
    for h in handler_on_all_events:
      h(entry, eventname, eventdata, caller, published_message)

def _entry_event_params_match_condition(eventdata, condition):
  """
  @params eventdata { 'params' : ..., ... }
  """
  return scripting_js.script_eval(condition, {'params': eventdata['params'], 'changed_params': eventdata['changed_params'], 'keys': eventdata['keys']}, cache = True)

def _entry_event_publish_and_invoke_listeners(entry, eventname, params, time, caller):
  # @param caller is "#events_passthrough" in case of events_passthrough
  eventdata = _entry_event_publish(entry, eventname, params, time)
  _entry_event_invoke_listeners(entry, eventdata, caller)




def entry_topic_lambda(entry):
  return lambda topic: entry_topic(entry, topic)

def entry_topic(entry, topic):
  result = __entry_topic(entry, topic)
  if result == 'health':
    logging.error("DEBUG> entry_topic HEALTH ERROR, entry = {entry}, is_local = {is_local}, aliases = {aliases}, definition = {definition})".format(entry = entry.id, is_local = entry.is_local, aliases = entry.topic_rule_aliases, definition = entry.definition))
  return result

def __entry_topic(entry, topic):
  if not entry.is_local:
    return topic

  # OBSOLETE (/topic should not be used)
  #if topic.startswith('/'):
  #  return topic[1:]
  if topic.startswith('./'):
    return ((entry.definition['topic_root'] + '/') if 'topic_root' in entry.definition else '') + topic[2:]
  if topic == '.':
    return entry.definition['topic_root'] if 'topic_root' in entry.definition else ''
  if topic.startswith('@/'):
    return ((entry.definition['entry_topic'] + '/') if 'entry_topic' in entry.definition else '') + topic[2:]
  if topic == '@':
    return entry.definition['entry_topic'] if 'entry_topic' in entry.definition else ''
  if topic in entry.topic_rule_aliases:
    return entry.topic_rule_aliases[topic]
  return topic

def entry_publish_lambda(entry):
  return lambda topic, payload = None, qos = None, retain = None, response_callback = None, no_response_callback = None, response_id = None: entry_publish(entry, topic, payload, qos, retain, response_callback, no_response_callback, response_id)

def entry_publish(entry, topic, payload = None, qos = None, retain = None, response_callback = None, no_response_callback = None, response_id = None):
  """
  Installed as entry.publish, publish a topic on mqtt
  """
  if topic == '' and entry_publish_current_default_topic():
    topic = entry_publish_current_default_topic()

  topic = entry_topic(entry, topic)

  if qos is None:
    qos = entry.definition['publish'][topic]['qos'] if topic in entry.definition['publish'] and 'qos' in entry.definition['publish'][topic] else 0
  if retain is None:
    retain = entry.definition['publish'][topic]['retain'] if topic in entry.definition['publish'] and 'retain' in entry.definition['publish'][topic] else False
  message = Message(topic, payload, qos, retain)
  
  if response_callback or no_response_callback:
    subscribe_response(entry, message, response_callback, no_response_callback, response_id)
  
  broker().publish(topic, payload, qos, retain)

entry_publish_current_default_topic_var = None

def entry_publish_current_default_topic(set_topic = None):
  global entry_publish_current_default_topic_var
  if entry_publish_current_default_topic_var is None:
    entry_publish_current_default_topic_var = threading.local()
  if set_topic:
    entry_publish_current_default_topic_var.topic = set_topic
  return entry_publish_current_default_topic_var.topic if hasattr(entry_publish_current_default_topic_var, 'topic') else None

def subscribe_response(entry, message, callback = False, no_response_callback = False, id = False, default_count = 1, default_duration = 5):
  """
  Temporarily subscribe to responses of a published message (other message emitted as described in topic metadata 'response' field)
  @param callback (entry, id, message, matches, final, response_to_message)
  @param no_response_callback (entry, id, response_to_message)
  @param id if you specify this id with a string, and there is already a subscription done with this id, no new subscription will be generated. The id is also passed to callbacks. Usually you should use it with count > 1.
  @param default_count Number of responses it should detect (and call the callback for) before deleting the subscription.
  @param default_duration the system will wait for answer for this amount of seconds, and after that it will delete the subscription. If no answer arrive, no_response_callback is called.
  
  @return True if subscribed (so callback or no_response_callback will be called), False if not (id already subscribed, or no 'response' declared - in this case NO callback will be called)
  """
  global subscribed_response
  
  if id:
    for x in subscribed_response:
      if x['id'] and x['id'] == id:
        return False
      
  
  s = { 'message': message, 'callback': callback, 'no_response_callback': no_response_callback, 'entry': entry, 'id': id, 'listeners': [] }

  # Add specific listeners, as described in metadata 'response'
  subs = entries_subscribed_to(message.topic, message.payload)
  for entry_id  in subs:
    r = subs[entry_id]
    if r['definition'] and 'response' in r['definition']:
      for t in r['definition']['response']:
        if 'topic' in t:
          rtopic_rule = t['topic']
          if "{" in rtopic_rule:
            for i in range(0, len(r['matches'])):
              rtopic_rule = rtopic_rule.replace("{matches[" + str(i) + "]}", str(r['matches'][i]))
          s['listeners'].append({ 'topic_rule': rtopic_rule, 'expiry': timems() + (utils.read_duration(t['duration']) if 'duration' in t else default_duration) * 1000, 'count': t['count'] if 'count' in t else default_count })
  
  if not s['listeners']:
    return False
  
  subscribed_response.append(s)
  
  return True

def _subscribed_response_on_message(message):
  """
  Listen for all mqtt messages to find response topics
  """

  #
  # Manages "subscribe_response"
  #
  global subscribed_response
  now = timems()
  delay = round(mqtt.queueDelay() / 1000 + 0.49) * 1000
  # If there is a lot of delay in mqtt queue, we can assume there are probably a lot of messages managed by mqtt broker, so it could be normal a slowly processing of messages. So we add some more delay (20%).
  delay = delay * 1.2
  
  for x in subscribed_response:
    for l in x['listeners']:
      if l['expiry'] + delay > now and l['count'] > 0:
        #TODO Gestire una cache di qualche tipo (qui ho bisogno solo di sapere che c'è il match, quindi basterebbe una cache di topic_matches)
        matches = topic_matches(l['topic_rule'], message.topic, message.payload)
        if matches['matched']:
          l['count'] = l['count'] - 1
          final = True if [m['count'] for m in x['listeners'] if m['count'] > 0] else False
          if x['callback']:
            x['callback'](x['entry'], x['id'], message, final, x['message'])
          x['called'] = True
  for x in subscribed_response:
    if [l for l in x['listeners'] if l['expiry'] + delay <= now or l['count'] <= 0]:
      x['listeners'] = [l for l in x['listeners'] if l['expiry'] + delay > now and l['count'] > 0]

def _subscription_timer_thread():
  """
  An internal thread, initialied by init(), that scans for expired subscribed response topics (@see subscribe_response)
  """
  global subscribed_response, destroyed
  while not destroyed:
    now = timems()
    delay = round(mqtt.queueDelay() / 1000 + 0.49) * 1000
    # If there is a lot of delay in mqtt queue, we can assume there are probably a lot of messages managed by mqtt broker, so it could be normal a slowly processing of messages. So we add some more delay (20%).
    delay = delay * 1.2
    
    for x in subscribed_response:
      if [l for l in x['listeners'] if l['expiry'] + delay <= now]:
        # TODO DEBUG REMOVE
        for l in x['listeners']:
          if l['expiry'] + delay <= now:
            logging.debug('SYSTEM> Detected timeout in response checkout for entry #{id}, message {topic}={payload}, check: expiry {expiry} + delay {delay} <= now {now}'.format(id = x['entry'].id, topic = x['message'].topic, payload = x['message'].payload, expiry = l['expiry'], delay = delay, now = now))
        x['listeners'] = [l for l in x['listeners'] if l['expiry'] + delay > now]

    expired = [x for x in subscribed_response if not x['listeners']]
    if expired:
      for x in expired:
        if x['no_response_callback'] and 'called' not in x:
          x['no_response_callback'](x['entry'], x['id'], x['message'])
      subscribed_response = [x for x in subscribed_response if x['listeners']]

    sleep(.5)





###############################################################################################################################################################
#
# EVENTS & ACTIONS
#
###############################################################################################################################################################

def _entry_events_load(entry):
  entry.on = entry_on_event_lambda(entry)
  
def _entry_events_install(entry):
  """
  Initializes events for entry entry
  """
  entry.events = {}
  entry.events_keys = {}
  for topic in entry.definition['publish']:
    if 'events' in entry.definition['publish'][topic]:
      for eventname in entry.definition['publish'][topic]['events']:
        if ":" not in eventname:
          if not eventname in entry.events:
            entry.events[eventname] = []
          entry.events[eventname].append(topic)
        elif eventname.endswith(':keys'):
          entry.events_keys[eventname[0:-5]] = entry.definition['publish'][topic]['events'][eventname]
        elif eventname.endswith(':init'):
          data = entry.definition['publish'][topic]['events'][eventname] if isinstance(entry.definition['publish'][topic]['events'][eventname], list) else [ entry.definition['publish'][topic]['events'][eventname] ]
          for eventparams in data:
            _entry_event_publish(entry, eventname[0:-5], eventparams, -1)

def _entry_actions_load(entry):
  entry.do = entry_do_action_lambda(entry)

def _entry_actions_install(entry):
  """
  Initializes actions for entry entry
  """
  entry.actions = {}
  for topic in entry.definition['subscribe']:
    if 'actions' in entry.definition['subscribe'][topic]:
      for actionname in entry.definition['subscribe'][topic]['actions']:
        if ":" not in actionname:
          if not actionname in entry.actions:
            entry.actions[actionname] = []
          entry.actions[actionname].append(topic)
        elif actionname.endswith(':init'):
          data = entry.definition['subscribe'][topic]['actions'][actionname] if isinstance(entry.definition['subscribe'][topic]['actions'][actionname], list) else [ entry.definition['subscribe'][topic]['actions'][actionname] ]
          for eventparams in data:
            _entry_event_publish(entry, 'action/' + actionname[0:-5], eventparams, -1)

def entry_support_event(entry, eventname):
  if hasattr(entry, 'events'):
    return eventname in entry.events
  # If called during "system_loaded" phase, i must cycle through published topics
  if 'publish' in entry.definition:
    for topic in entry.definition['publish']:
      if 'events' in entry.definition['publish'][topic]:
        if eventname in entry.definition['publish'][topic]['events'] and (":" not in eventname):
          return True
  return False

def entry_events_supported(entry):
  return list(entry.events.keys)

def on_event(eventref, listener = None, reference_entry = None, reference_tag = None):
  """
  Adds an event listener based on the event reference string "entry.event(condition)"
  @param eventref 
  @param listener a callback, defined as listener(entry, eventname, eventdata)
  @param reference_entry The entry with the event_reference. Used for implicit references (if eventref dont contains entry id).
  @param reference_tag Just for logging purpose, the context of the entry defining the the event_reference (so who reads the log could locate where the event_reference is defined)
  """
  global events_listeners
  d = decode_event_reference(eventref, default_entry_id = reference_entry.id if reference_entry else None) if not isinstance(eventref, dict) else eventref
  if not d:
    logging.error("#{entry}> Invalid '{type}' definition{tag}: {defn}".format(entry = reference_entry.id if reference_entry else '?', type = 'on event' if listener else 'events_listen', tag = (' in ' + reference_tag) if reference_tag else '', defn = eventref))
  else:
    if not d['event'] in events_listeners:
      events_listeners[d['event']] = {}
    #OBSOLETE: d['entry'] = entry_id_expand(d['entry'])
    if not d['entry'] in events_listeners[d['event']]:
      events_listeners[d['event']][d['entry']] = []
    if listener:
      events_listeners[d['event']][d['entry']].append([listener, d['condition']])

def add_event_listener(eventref, reference_entry = None, reference_tag = None):
  """
  Add a generic listener for event (so the events are stored on "listened_events" parameter on handlers, and they can be fetched by event_get())
  """
  on_event(eventref, None, reference_entry, reference_tag)

def entry_on_event_lambda(entry):
  return lambda event, listener, condition = None: entry_on_event(entry, event, listener, condition)

def entry_on_event(entry, event, listener, condition = None):
  """
  Adds an event listener on the specified entry.event(condition)
  @param event name of event matched
  @param listener a callback, defined as listener(entry, eventname, eventdata)
  @param condition javascript condition to match event. Example: "port = 1 && value < 10"
  """
  on_event({'entry': entry.id, 'event': event, 'condition': condition}, listener)

def entry_support_action(entry, actionname):
  if hasattr(entry, 'actions'):
    return actionname in entry.actions
  # If called during "system_loaded" phase, i must cycle through published topics
  if 'subscribe' in entry.definition:
    for topic in entry.definition['subscribe']:
      if 'actions' in entry.definition['subscribe'][topic]:
        if actionname in entry.definition['subscribe'][topic]['actions'] and (":" not in actionname):
          return True
  return False

def entry_actions_supported(entry):
  return list(entry.actions.keys)

def do_action(actionref, params, reference_entry_id = None, if_event_not_match = False, if_event_not_match_keys = False, if_event_not_match_timeout = None):
  d = decode_action_reference(actionref, default_entry_id = reference_entry_id)
  return entry_do_action(d['entry'], d['action'], params, d['init'], if_event_not_match = if_event_not_match, if_event_not_match_keys = if_event_not_match_keys, if_event_not_match_timeout = if_event_not_match_timeout) if d else None

def entry_do_action_lambda(entry):
  return lambda action, params = {}, init = None, if_event_not_match = False, if_event_not_match_keys = False, if_event_not_match_timeout = None: entry_do_action(entry, action, params = params, init = init, if_event_not_match = if_event_not_match, if_event_not_match_keys = if_event_not_match_keys, if_event_not_match_timeout = if_event_not_match_timeout)

def entry_do_action(entry_or_id, action, params = {}, init = None, if_event_not_match = False, if_event_not_match_keys = False, if_event_not_match_timeout = None):
  entry = entry_or_id if not isinstance(entry_or_id, str) else entry_get(entry_or_id)
  #logging.debug("entry_do_action " + str(entry_or_id) + " | " + str(entry) + " | " + str(action) + " | " + str(params))
  
  if entry and action in entry.actions:
    if if_event_not_match and if_event_not_match == True:
      if_event_not_match = transform_action_reference_to_event_reference({'entry': entry.id, 'action': action, 'init': init}, return_decoded = True)
    if if_event_not_match and isinstance(if_event_not_match, str):
      if_event_not_match = decode_event_reference(if_event_not_match)
    if if_event_not_match:
      event = entry_event_get(if_event_not_match['entry'], if_event_not_match['event'], if_event_not_match['condition'], timeout = if_event_not_match_timeout)
      #logging.debug("MATCH " + str(event))
      if event:
        match = True
        for k in (if_event_not_match_keys if if_event_not_match_keys else params.keys()):
          match = match and (k in params and k in event and params[k] == event[k])
        if match:
          #logging.debug("MATCHED")
          return True
  
    publish = None
    action_full_params = None
    for topic in entry.actions[action]:
      actiondef = entry.definition['subscribe'][topic]['actions'][action]
      if isinstance(actiondef, str):
        actiondef = { 'payload': actiondef }
      if actiondef['payload']:
        context = scripting_js.script_context({ 'params': params })
        if 'init' in actiondef and actiondef['init']:
          scripting_js.script_exec(actiondef['init'], context)
        if init:
          scripting_js.script_exec(init, context)
        action_full_params = context.params
        payload = scripting_js.script_eval(actiondef['payload'], context, to_dict = True)
        if payload != None:
          if 'topic' in actiondef and actiondef['topic']:
            topic = scripting_js.script_eval(actiondef['topic'], context, to_dict = True)
          publish = [topic, payload]
          break
      else:
        publish = [topic, None]
        break

    if publish:
      entry.publish(publish[0], publish[1])
      event_get_invalidate_on_action(entry, action, action_full_params, if_event_not_match)
      return True

  return False

def event_get(eventref, timeout = None, keys = None, temporary = False):
  d = decode_event_reference(eventref)
  if d:
    return entry_event_get(d['entry'], d['event'], condition = d['condition'], timeout = timeout, keys = keys, temporary = temporary)
  else:
    logging.error("#SYSTEM> Invalid event reference {eventref}".format(eventref = eventref))

def event_get_time(eventref, timeout = None, temporary = False):
  return event_get(eventref, timeout = timeout, keys = ['_time'], temporary = temporary)

def entry_event_get(entry_or_id, eventname, condition = None, keys = None, timeout = None, temporary = False):
  """
  WARN: You can do an "entry_event_get" only of listened events (referenced in "on" or "events_listen" entry definitions)

  @param keys List of event params names to get. Use "_time" as param name to get event time
  @param timeout None or a duration
  """
  global events_published, events_published_lock
  entry_id = entry_id_find(entry_or_id) if isinstance(entry_or_id, str) else entry_or_id.id
  
  with events_published_lock:
    match = None
    if eventname in events_published and entry_id in events_published[eventname]:
      for keys_index in events_published[eventname][entry_id]:
        t = entry_event_keys_index_is_temporary(keys_index)
        if (t and temporary) or (not t and not temporary):
          e = events_published[eventname][entry_id][keys_index]
          if (timeout is None or time() - e['time'] <= utils.read_duration(timeout)) and (condition is None or _entry_event_params_match_condition(e, condition)) and (not match or e['time'] > match['time']):
            match = e;
  if match:
    ret = (match['params'], ) if keys is None else tuple(match['time'] if k == '_time' else (match['params'][k] if k in match['params'] else None) for k in keys)
    return ret[0] if len(ret) <= 1 else ret

  return None if keys is None or len(keys) == 1 else tuple(None for k in keys)

def entry_event_get_time(entry_or_id, eventname, timeout = None, temporary = False):
  entry_event_get(entry_or_id, eventname, ['_time'], timeout, temporary)

def entry_events_published(entry_or_id):
  """
  WARN: You can do an "entry_events_published" only of listened events (referenced in "on" or "events_listen" entry definitions)
  """
  global events_published, events_published_lock
  res = {}
  entry = entry_or_id if not isinstance(entry_or_id, str) else entry_get(entry_or_id)
  with events_published_lock:
    for eventname in entry.events:
      res[eventname] = {}
      if eventname in events_published and entry.id in events_published[eventname]:
        for keys_index in events_published[eventname][entry.id]:
          res[eventname][keys_index] = events_published[eventname][entry.id][keys_index]
  return res;

def events_import(data, mode = 0):
  global events_published, events_published_lock
  
  """
  @param mode = 0 only import events not present, or with time = -1|0
  @param mode = 1 ... also events with time >= than the one in memory
  @param mode = 2 ... also all events with time > 0
  @param mode = 3 import all events (even if time = 0)
  """
  for entry_id in data:
    for eventname in data[entry_id]:
      for eventdata in data[entry_id][eventname]:
        temporary = "temporary" in eventdata['params'] and eventdata['params']['temporary']
        keys_index = entry_event_keys_index(eventdata['keys'] if 'keys' in eventdata else {}, temporary)
        with events_published_lock:
          if not eventname in events_published:
            events_published[eventname] = {}
          if not entry_id in events_published[eventname]:
            events_published[eventname][entry_id] = {}
          prevdata = events_published[eventname][entry_id][keys_index] if keys_index in this.events_published[eventname][entry_id] else None
          go = mode == 3 or (not prevdata) or (prevdata['time'] <= 0 and eventdata['time'] > 0)
          if (not go) and mode == 1:
            go = eventdata['time'] >= prevdata['time']
          if (not go) and mode == 2:
            go = eventdata['time'] > 0
          if go:
            events_published[eventname][entry.id][keys_index] = eventdata
            if eventdata['time'] >= 0 and not temporary:
              _entry_event_invoke_listeners(entry_get(entry_id), eventdata, 'import');

def events_export():
  global all_entries
  all_events = {}
  for entry_id in all_entries:
    all_events[entry_id] = entry_events_published(entry_id)
  return all_events

_re_decode_event_reference = re.compile('^(?P<entry>[A-Za-z0-9@*_-]+)?(?:\.(?P<event>[A-Za-z0-9_-]+))?(?:\((?P<condition>.*)\))?$')

def decode_event_reference(s, default_entry_id = None, default_event = None, no_event = False):
  """
  Decodes a string reference to an event, like "entry_id.event" or "*.event(port == 1)"
  @params no_event: True if is possibile to NOT specify an event in the string, ex: "entry(condition)"
  @return { "entry": "...", "event": "...", "condition": "..." }
  """
  m = _re_decode_event_reference.match(s)
  m = m.groupdict() if m else None
  if m and default_entry_id and not m['entry']:
    m['entry'] = default_entry_id
  if m and default_event and not m['event']:
    m['event'] = default_event
  return m if m and m['entry'] and (m['event'] or no_event) else None

_re_decode_action_reference = re.compile('^(?P<entry>[A-Za-z0-9@*_-]+)?(?:\.(?P<action>[A-Za-z0-9_-]+))?(?:\((?P<init>.*)\))?$')

def decode_action_reference(s, default_entry_id = None, default_action = None, no_action = False):
  """
  Decodes a string reference to an action, like "entry_id.action" or "entry_id.action(init_code)"
  @params no_action: True if is possibile to NOT specify an action in the string, ex: "entry(init)"
  @return { "entry": "...", "action": "...", "init": "..." }
  """
  m = _re_decode_action_reference.match(s)
  m = m.groupdict() if m else None
  if m and default_entry_id and not m['entry']:
    m['entry'] = default_entry_id
  if m and default_action and not m['action']:
    m['action'] = default_action
  return m if m and (m['action'] or no_action) else None

def generate_event_reference(entry_id, eventname, eventdata):
  condition = ''
  if 'keys' in eventdata and eventdata['keys']:
    for k in eventdata['keys']:
      condition = condition + (' && ' if condition else '') + 'params["' + k + '"] == ' + json.dumps(eventdata['keys'][k])
    condition = '(js: ' + condition + ')'
  return entry_id + '.' + eventname + condition

def transform_action_reference_to_event_reference(actionref, return_decoded = False):
  """
  Transform an action reference to a valid event reference.
  Delete the "-set" postfix to action name and replace "=" with "==" and ";" with "&&" in init code
  Example: "js: action-set(js: params['x'] = 1; params['y'] = 2;" > "js: action(js: params['x'] == 1 && params['y'] == 2)"
  """
  d = decode_action_reference(actionref, no_action = True) if not isinstance(actionref, dict) else actionref
  if not d:
    return None
  r = {'entry': d['entry'], 'event': transform_action_name_to_event_name(d['action']) if d['action'] else None, 'condition': d['init'].strip('; ').replace('=', '==').replace(';', '&&') if d['init'] else None }
  return r if return_decoded else r['entry'] + (('.' + r['event']) if r['event'] else '') + (('(' + r['condition'] + ')') if r['condition'] else '')

def transform_action_name_to_event_name(actionname):
  return actionname.replace('-set', '')

def transform_event_reference_to_action_reference(eventref, return_decoded = False):
  """
  Transform an event reference to a valid action reference.
  Add the "-set" postfix to event name and replace "==" with "=" and "&&" with ";" in condition
  Example: "js: event(js: params['x'] == 1 && params['y'] == 2)" > "js: event-set(js: params['x'] = 1; params['y'] = 2;"
  """
  d = decode_event_reference(eventref, no_event = True) if not isinstance(eventref, dict) else eventref
  if not d:
    return None
  r = {'entry': d['entry'], 'action': (d['event'] + '-set') if d['event'] else None, 'init': d['condition'].replace('==', '=').replace('&&', ';') if d['condition'] else None }
  return r if return_decoded else r['entry'] + (('.' + r['action']) if r['action'] else '') + (('(' + r['init'] + ')') if r['init'] else '')

