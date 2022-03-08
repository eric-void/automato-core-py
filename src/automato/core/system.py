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
  'events': True,
  'actions': True,
  'events_passthrough': True,
}
PRIMITIVE_TYPES = (int, str, bool, float, dict, list)
EVENT_KEYS = ['port']

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

index_topic_cache = { 'hits': 0, 'miss': 0, 'data': {} }
index_topic_cache_lock = threading.Lock()
INDEX_TOPIC_CACHE_MAXSIZE = 1024
INDEX_TOPIC_CACHE_PURGETIME = 3600

destroyed = False
test_mode = False

def set_config(_config):
  global config
  config = _config

def boot():
  global config, destroyed
  
  destroyed = False
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
  global all_entries, all_entries_signatures, all_nodes, exports, subscriptions, last_entry_and_events_for_received_mqtt_message, events_listeners, events_published, events_published_lock, events_groups, events_groups_lock, index_topic_published, index_topic_subscribed
  all_entries = {}
  all_entries_signatures = {}
  all_nodes = {}
  exports = {}
  subscriptions = {}
  last_entry_and_events_for_received_mqtt_message = None
  events_listeners = {}
  events_published = {}
  events_published_lock = threading.Lock()
  events_groups = {}
  events_groups_lock = threading.Lock()
  index_topic_published = {}
  index_topic_subscribed = {}
  scripting_js.exports = exports
  
  global handler_on_entry_load, handler_on_entry_load_batch, handler_on_entry_unload, handler_on_entry_init, handler_on_entry_init_batch, handler_on_entries_change, handler_on_initialized, handler_on_message, handler_on_all_events
  handler_on_entry_load = []
  handler_on_entry_load_batch = []
  handler_on_entry_unload = []
  handler_on_entry_init = []
  handler_on_entry_init_batch = []
  handler_on_entries_change = []
  handler_on_initialized = []
  handler_on_message = []
  handler_on_all_events = []

def on_entry_load(handler):
  """
  Called during entry load phase, before on_entry_load_batch/on_entry_init/on_entry_init_batch, for every entry in loading phase
  @param handler(entry)
  """
  global handler_on_entry_load
  handler_on_entry_load.append(handler)

def on_entry_load_batch(handler):
  """
  Called after on_entry_load, and before on_entry_init/on_entry_init_batch, with current batch of loading entries. 
  @param handler(loading_defs: {entry_id: entry}): This callback can be called several time for a single batch of entries loading. If a call invalid previously loaded (and initialized entries), they will be passed in a new callback.
    You can classify entry loading phases with these references:
    - loading_defs {entry_id: entry}: loading entries for this specific callback call, that must be processed
    - system.entries(): ALL entries managed by the system, this contains already loaded and initialized, already loaded (by the current loading request) but NOT inizialized (passed in previous callback calls and processed), loading now (NOT initialized and, obviously, NOT processed by this call)
    - for (entry in system.entries().values()) if entry.loaded: entries already loaded and initialized. Only these entries can be returned in this callback (to flag them as "must_reload")
    - for (entry in system.entries().values()) if entry not in loading_defs: entries already loaded and initialized + entries already loaded (by the current loading request) but NOT inizialized. These have been passed in previous callback calls and processed by it.
    - for (entry in system.entries().values()) if not entry.loaded and entry not in loading_defs: only entries already loaded (by the current loading request) but NOT inizialized. These have been passed in previous callback calls and processed by it.
    handler can return a list of ids of entries (already loaded and initialized) that must be unloaded and reloaded
  """
  global handler_on_entry_load_batch
  handler_on_entry_load_batch.append(handler)

def on_entry_unload(handler):
  global handler_on_entry_unload
  handler_on_entry_unload.append(handler)

def on_entry_init(handler):
  """
  Called after on_entry_load/on_entry_load, and before on_entry_init_batch, for every entry loaded and initialized by the core system
  @param handler(entry)
  """
  global handler_on_entry_init
  handler_on_entry_init.append(handler)

def on_entry_init_batch(handler):
  """
  Called after on_entry_init, with current batch of initialized entries
  @param handler(entries: {entry_id: entry})
  """
  global handler_on_entry_init_batch
  handler_on_entry_init_batch.append(handler)

"""
@param handler (entry_ids_loaded, entry_ids_unloaded)
"""
def on_entries_change(handler):
  global handler_on_entries_change
  handler_on_entries_change.append(handler)

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
  global config, all_entries, default_node_name, subscribed_response, subscription_thread
  topic_cache_reset()
  default_node_name = config['name'] if 'name' in config else 'root'
  
  subscribed_response = []
  subscription_thread = threading.Thread(target = _subscription_timer_thread, daemon = True) # daemon = True allows the main application to exit even though the thread is running. It will also (therefore) make it possible to use ctrl+c to terminate the application
  subscription_thread.start()
  
  notifications.init()
  
  entry_load(config['entries'], node_name = default_node_name, id_from_definition = True, generate_new_entry_id_on_conflict = True)

  if handler_on_initialized:
    for h in handler_on_initialized:
      h(all_entries)

  mqtt.connect(lambda phase: _mqtt_connect_callback(callback, phase))
  
def destroy():
  global all_entries, destroyed, subscription_thread
  while len(all_entries.keys()):
    entry_unload(list(all_entries.keys()))
  
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
    '  mqtt_queue_delay: {cdelay}ms (recent/load: {delay}ms)\n' + 
    #'  script_eval_cache: {schits}/{sctotal} hits ({scperc}%), {scsize} size, {scskip} uncacheable, {scdisabled} cache disabled, {scsign} signatures\n' + 
    '  script_js_compiled: {sjhits}/{sjtotal} hits ({sjperc}%), {sjsize} size\n' + 
    '  topic cache: {tphits}/{tptotal} ({tpperc}%) hits, {tpsize} size\n' + 
    '  system_stats:\n{stats}').format(
    total = round(total / 60000),
    delay = mqtt.queueDelay(), cdelay = mqtt.queueDelayCurrent(),
    #schits = scripting_js.script_eval_cache_hits, sctotal = scripting_js.script_eval_cache_hits + scripting_js.script_eval_cache_miss, scperc = round(scripting_js.script_eval_cache_hits * 100 / (scripting_js.script_eval_cache_hits + scripting_js.script_eval_cache_miss)) if (scripting_js.script_eval_cache_hits + scripting_js.script_eval_cache_miss) > 0 else 0, scsize = len(scripting_js.script_eval_cache), scskip =  scripting_js.script_eval_cache_skipped, scdisabled = scripting_js.script_eval_cache_disabled, scsign = len(scripting_js.script_eval_codecontext_signatures),
    sjhits = scripting_js.script_js_compiled_hits, sjtotal = scripting_js.script_js_compiled_hits + scripting_js.script_js_compiled_miss, sjperc = round(scripting_js.script_js_compiled_hits * 100 / (scripting_js.script_js_compiled_hits + scripting_js.script_js_compiled_miss)) if (scripting_js.script_js_compiled_hits + scripting_js.script_js_compiled_miss) > 0 else 0, sjsize = len(scripting_js.script_js_compiled), 
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

    self.definition = copy.deepcopy(definition) # WARN! Do not modify definition in code below, only self.definition
    self.definition_loaded = copy.deepcopy(definition)
    
    self.id = entry_id
    self.id_local = entry_id[:d]
    self.node_name = entry_id[d + 1:]
    self.is_local = None
    self.node_config = config
    if 'type' not in self.definition:
      self.definition['type'] = 'module' if 'module' in definition else ('device' if 'device' in self.definition else 'item')
    self.type = self.definition['type']
    self.created = time()
    self.last_seen = 0
    self.publish_last_seen = {}
    self.exports = exports
    self.topic_rule_aliases = {}
    self.topic = entry_topic_lambda(self)
    self.publish = entry_publish_lambda(self)
    self._refresh_definition_based_properties()
  
  # Call this when entry.definition changes (should happen only during entry_load phase)
  def _refresh_definition_based_properties(self):
    self.config = self.definition['config'] if 'config' in self.definition else {}
    self.caption = self.definition['caption'] if 'caption' in self.definition else self.id_local
    if not 'publish' in self.definition:
      self.definition['publish'] = {}
    if not 'subscribe' in self.definition:
      self.definition['subscribe'] = {}
    if not 'on' in self.definition:
      self.definition['on'] = {}
    if not 'events_listen' in self.definition:
      self.definition['events_listen'] = []

def entry_load(definitions, node_name = False, unload_other_from_node = False, id_from_definition = False, generate_new_entry_id_on_conflict = False):
  """
  Load a batch of definitions to instantiate entries.
  There are 2 phases for this process:
  - in first one (_entry_load_definition) we load and process each entry definition. During this phase on_entry_load callback is called and can invalidate previously loaded (and initialized) entries, that must be unloaded and reloaded and injected in this phase.
    in this phase a call to system.entries() will returns ALL entries, event the one loading right now (we can detect the state with entry.loaded flag)
    load, entry_load, entry_install handlers are called in this phase
  - in a second phase (_entry_load_init) all defined entries will be initialized
    init handler is called in this phase
  
  @param id_from_definition. If True: definitions = [ { ...definition...} ]; if False: definitions = { 'entry_id': { ... definition ... } }
  """
  global all_entries, default_node_name, handler_on_entry_load, handler_on_entry_load_batch, handler_on_entry_init, handler_on_entry_init_batch, handler_on_entries_change
  
  if not node_name:
    node_name = default_node_name
  
  loaded_defs = {}
  loading_defs = {}
  reload_definitions = {}
  skipped_ids = []
  unloaded = []
  while definitions:
    for definition in definitions:
      if not id_from_definition:
        entry_id = definition
        definition = definitions[entry_id]
      else:
        entry_id = False
      if 'disabled' not in definition or not definition['disabled']:
        result = _entry_load_definition(definition, from_node_name = node_name, entry_id = entry_id, generate_new_entry_id_on_conflict = generate_new_entry_id_on_conflict, extended_result = True)
        if result and result['entry']:
          if handler_on_entry_load:
            for h in handler_on_entry_load:
              h(result['entry'])
          loading_defs[result['entry'].id] = result['entry']
        elif result['message'] == 'no_changes':
          skipped_ids.append(result['id'])
    if loading_defs:
      logging.debug("SYSTEM> Loading entries, loaded definitions for {entries} ...".format(entries = list(loading_defs.keys()) ))
      if handler_on_entry_load_batch:
        for h in handler_on_entry_load_batch:
          reload_entries = h(loading_defs)
          if reload_entries:
            logging.debug("SYSTEM> Loading entries, need to reload other entries {entries} ...".format(entries = reload_entries))
            for rentry_id in reload_entries:
              if rentry_id not in reload_definitions and rentry_id in all_entries:
                reload_definitions[rentry_id] = all_entries[rentry_id].definition_loaded
                entry_unload(rentry_id, call_on_entries_change = False)
                unloaded.append(rentry_id)
    if reload_definitions:
      id_from_definition = False
      definitions = reload_definitions
      reload_definitions = {}
    else:
      definitions = False
    for entry_id in loading_defs:
      loaded_defs[entry_id] = loading_defs[entry_id]
    loading_defs = {}
  
  if unload_other_from_node:
    todo_unload = []
    for entry_id in all_entries:
      if all_entries[entry_id].node_name == node_name and entry_id not in loaded_defs and entry_id not in skipped_ids:
        todo_unload.append(entry_id)
    for entry_id in todo_unload:
      entry_unload(entry_id, call_on_entries_change = False)
      unloaded.append(entry_id)

  if loaded_defs:
    # Final loading phase, install final features. This must be done BEFORE init hooks (they must receive a fully loaded entry)
    for entry_id in loaded_defs:
      _entry_definition_normalize_after_load(loaded_defs[entry_id])
      _entry_events_install(loaded_defs[entry_id])
      _entry_actions_install(loaded_defs[entry_id])
      _entry_add_to_index(loaded_defs[entry_id])
      loaded_defs[entry_id].loaded = True

    logging.debug("SYSTEM> Loading entries, initializing {entries} ...".format(entries = list(loaded_defs.keys())))
    # Calls handler_on_entry_init
    for entry_id in loaded_defs:
      if handler_on_entry_init:
        for h in handler_on_entry_init:
          h(loaded_defs[entry_id])
    # Calls handler_on_entry_init_batch
    if handler_on_entry_init_batch:
      for h in handler_on_entry_init_batch:
        h(loaded_defs)

    logging.debug("SYSTEM> Loaded entries {entries}.".format(entries = list(loaded_defs.keys())))

  # Calls handler_on_entries_change
  if handler_on_entries_change:
    for h in handler_on_entries_change:
      h(list(loaded_defs.keys()), unloaded)

def _entry_load_definition(definition, from_node_name = False, entry_id = False, generate_new_entry_id_on_conflict = False, extended_result = False):
  global config, default_node_name, all_entries, all_entries_signatures, all_nodes
  if not isinstance(definition, dict):
    return None if not extended_result else { "entry": None, "id": entry_id, "message": "error_invalid_definition" }

  if not from_node_name:
    d = entry_id.find("@") if entry_id else -1
    from_node_name = entry_id[d + 1:] if d >= 0 else default_node_name
  
  if not entry_id:
    entry_id = definition['id'] if 'id' in definition else (definition['module'] if 'module' in definition else (definition['device'] if 'device' in definition else (definition['item'] if 'item' in definition else '')))
  if not entry_id:
    return None if not extended_result else { "entry": None, "id": entry_id, "message": "error_no_id" }
  entry_id = re.sub('[^A-Za-z0-9@_-]+', '-', entry_id)
  d = entry_id.find("@")
  if d < 0:
    entry_id = entry_id + '@' + from_node_name
    
  if generate_new_entry_id_on_conflict and entry_id in all_entries:
    d = entry_id.find("@")
    entry_local_id = entry_id[:d]
    entry_node_name = entry_id[d + 1:]
    entry_id = entry_local_id
    i = 0
    while entry_id + '@' + entry_node_name in all_entries:
      i += 1
      entry_id = entry_local_id + '_' + str(i)
    entry_id = entry_id + '@' + entry_node_name

  new_signature = utils.data_signature(definition)
  if entry_id in all_entries:
    if new_signature and all_entries_signatures[entry_id] == new_signature:
      return None if not extended_result else { "entry": None, "id": entry_id, "message": "no_changes" }
    logging.debug("SYSTEM> Entry definition for {entry} changed signature, reloading it...".format(entry = entry_id))
    entry_unload(entry_id)
  
  entry = Entry(entry_id, definition, config)
  entry.is_local = entry.node_name == default_node_name
  
  _entry_events_load(entry)
  _entry_actions_load(entry)
  entry._refresh_definition_based_properties()
  
  entry.loaded = False
  all_entries[entry.id] = entry
  all_entries_signatures[entry.id] = new_signature
  if entry.node_name not in all_nodes:
    all_nodes[entry.node_name] = {}

  return entry if not extended_result else { "entry": entry, "id": entry_id, "message": None }
  
def entry_unload(entry_ids, call_on_entries_change = True):
  global all_entries, all_entries_signatures, handler_on_entries_change
  if isinstance(entry_ids, str):
    entry_ids = [ entry_ids ]
  for entry_id in entry_ids:
    if entry_id in all_entries:
      logging.debug("SYSTEM> Unloading entry {id} ...".format(id = entry_id))
      if handler_on_entry_unload:
        for h in handler_on_entry_unload:
          h(all_entries[entry_id])

      remove_event_listener_by_reference_entry(entry_id)
      _entry_remove_from_index(all_entries[entry_id])
      
      del all_entries[entry_id]
      del all_entries_signatures[entry_id]
      
      logging.debug("SYSTEM> Unloaded entry {id}.".format(id = entry_id))
  
  # TODO I need to reset topic_cache. I can improve this by resetting only topic matching entry unloading ones
  topic_cache_reset()
  
  if call_on_entries_change and handler_on_entries_change:
    for h in handler_on_entries_change:
      h([], entry_ids)

def entry_reload(entry_ids, call_on_entries_change = True):
  global all_entries, handler_on_entries_change
  if isinstance(entry_ids, str):
    entry_ids = [ entry_ids ]
  for entry_id in entry_ids:
    if entry_id in all_entries:
      definition = all_entries[entry_id].definition_loaded
      entry_unload(entry_id, call_on_entries_change = False)
      entry_load(definition, None, entry_id, generate_new_entry_id_on_conflict = False, call_on_entries_change = False)

  if call_on_entries_change and handler_on_entries_change:
    for h in handler_on_entries_change:
      h(entry_ids, entry_ids)

def entry_unload_node_entries(node_name):
  global all_entries, handler_on_entries_change
  todo_unload = []
  for entry_id in all_entries:
    if all_entries[entry_id].node_name == node_name:
      todo_unload.append(entry_id)
  if todo_unload:
    for entry_id in todo_unload:
      entry_unload(entry_id, call_on_entries_change = False)
      
  if handler_on_entries_change:
    for h in handler_on_entries_change:
      h([], todo_unload)

def entries():
  """
  Returns ALL entries loaded, or in loading phase, by the system. To get only fully loaded (and initialized) entries look at entry.loaded flag
  """
  global all_entries
  return all_entries







###############################################################################################################################################################
#
# GENERIC ENTRIES FUNCTIONS
#
###############################################################################################################################################################

def _entry_definition_normalize_after_load(entry):
  if entry.is_local:
    if not 'entry_topic' in entry.definition:
      entry.definition['entry_topic'] = entry.type + '/' + entry.id_local
    if not 'topic_root' in entry.definition:
      entry.definition['topic_root'] = entry.definition['entry_topic']

  for t in entry.definition['subscribe']:
    if 'publish' in entry.definition['subscribe'][t] and 'response' not in entry.definition['subscribe'][t]:
      entry.definition['subscribe'][t]['response'] = entry.definition['subscribe'][t]['publish']

  for k in ['qos', 'retain']:
    if k in entry.definition:
      for topic_rule in entry.definition['publish']:
        if k not in entry.definition['publish'][topic_rule]:
          entry.definition['publish'][topic_rule][k] = entry.definition[k]

  if 'ignore_interval' in entry.definition:
    entry.definition['ignore_interval'] = utils.read_duration(entry.definition['ignore_interval'])

  if 'publish' in entry.definition:
    res = {}
    for topic_rule in entry.definition['publish']:
      if 'topic' in entry.definition['publish'][topic_rule]:
        entry.topic_rule_aliases[topic_rule] = entry.topic(entry.definition['publish'][topic_rule]['topic'])
      if 'ignore_interval' in entry.definition['publish'][topic_rule]:
        entry.definition['publish'][topic_rule]['ignore_interval'] = utils.read_duration(entry.definition['publish'][topic_rule]['ignore_interval'])
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

def _on_events_passthrough_listener_lambda(dest_entry, passthrough_conf):
  return lambda entry, eventname, eventdata, caller, published_message: _on_events_passthrough_listener(entry, eventname, eventdata, caller, published_message, dest_entry, passthrough_conf)
  
def _on_events_passthrough_listener(source_entry, eventname, eventdata, caller, published_message, dest_entry, passthrough_conf):
  params = copy.deepcopy(eventdata['params'])
  if passthrough_conf['remove_keys'] and ('keys' in eventdata):
    for k in eventdata['keys']:
      del params[k]
  exec_context = None
  if 'init' in passthrough_conf:
    exec_context = scripting_js.script_exec(passthrough_conf['init'], { 'params': params })
  _entry_event_publish_and_invoke_listeners(dest_entry, passthrough_conf["rename"] if "rename" in passthrough_conf and passthrough_conf["rename"] else eventname, exec_context['params'] if 'init' in passthrough_conf else params, eventdata['time'], 'events_passthrough', published_message)

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
  NOTE: List settings in first-level (for example: required, events_listen, ...) are joined together. List settings in other levels are NOT joined (like all other primitive types).
  """
  entry.definition = utils.dict_merge(default, entry.definition, join_lists_depth = 2) # 2 = lists in first level are joined

def entry_id_match(entry, reference):
  """
  Return if the id of the entry (reference) matches the one on the entry
  If the reference has no "@", only the local part is matched (so, if there are more entries with the same local part, every one of these is matched)
  """
  if isinstance(entry, str):
    if entry == reference:
      return True
    d1 = entry.find("@")
    d2 = reference.find("@")
    return (d1 < 0 and d2 >= 0 and entry == reference[:d2]) or (d1 >= 0 and d2 < 0 and entry[:d1] == reference)
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
  return all_entries[eid] if eid and (eid in all_entries) else None

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
  global index_topic_cache, index_topic_cache_lock
  with index_topic_cache_lock:
    index_topic_cache = { 'hits': 0, 'miss': 0, 'data': { } }

def topic_cache_find(index, cache_key, topic, payload = None):
  """
  @param index is { topic:  { 'definition': { ... merged definition from all TOPIC published by entries ... }, 'entries': [ ... entry ids ... ]};
  @return (0: topic rule found, 1: topic metadata { 'definition': { ... merged definition from all topic published by entries ... }, 'entries': [ ... entry ids ... ]}, 2: matches)
  """
  global index_topic_cache, index_topic_cache_lock
  
  with index_topic_cache_lock:
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
  Return subscription definition, if present, of a subscribed topic
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
  WARN: if a topic_rule has a topic_match_priority = 0 (usually because its defined as a "catchall" topic rule, like ['base/#': {}]), and the same topic matches a subscribed topic of the same entry, that entry will NOT be listed (the topic will be considered only as a subscribed one, and not a published one)
  
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
          tmp = topic_match_priority(entry.definition['publish'][t[0]])
          if tmp > 0 or not entry_is_subscribed_to(entry, topic, payload):
            if entry_id in res and tmp > topic_match_priority(res[entry_id]['definition']):
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

def entry_topic_published_definition(entry, topic, payload = None, strict_match = False, skip_topic_match_priority = False):
  if topic in entry.definition['publish']:
    return entry.definition['publish']
  res = None
  if not strict_match:
    for topic_rule in entry.definition['publish']:
      m = topic_matches(topic_rule, topic)
      if m['matched']:
        if skip_topic_match_priority:
          return entry.definition['publish'][topic_rule]
        if (not res) or topic_match_priority(entry.definition['publish'][topic_rule]) > topic_match_priority(res):
          res = entry.definition['publish'][topic_rule]
  return res

def entry_is_publisher_of(entry, topic, payload = None):
  return True if entry_topic_published_definition(entry, topic, strict_match = False, skip_topic_match_priority = True) else False

def entry_topic_subscription_definition(entry, topic, payload = None, strict_match = False, skip_topic_match_priority = False):
  if topic in entry.definition['subscribe']:
    return entry.definition['subscribe']
  res = None
  if not strict_match:
    for topic_rule in entry.definition['subscribe']:
      m = topic_matches(topic_rule, topic)
      if m['matched']:
        if skip_topic_match_priority:
          return entry.definition['subscribe'][topic_rule]
        if (not res) or topic_match_priority(entry.definition['subscribe'][topic_rule]) > topic_match_priority(res):
          res = entry.definition['subscribe'][topic_rule]
  return res

def entry_is_subscribed_to(entry, topic, payload = None):
  return True if entry_topic_subscription_definition(entry, topic, strict_match = False, skip_topic_match_priority = True) else False


###############################################################################################################################################################
#
# MANAGE MESSAGES PUBLISHED ON BROKER
#
###############################################################################################################################################################

message_counter = 0
message_counter_lock = threading.Lock()

class Message(object):
  def __init__(self, topic, payload, qos = None, retain = None, payload_source = None, received = 0, copy_id = -1):
    global message_counter, message_counter_lock
    if copy_id == -1:
      with message_counter_lock:
        self.id = message_counter
        message_counter += 1
    else:
      self.id = copy_id
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
        ignored = False
        if "ignore" in entries[entry_id]['entry'].definition:
          ignored = entries[entry_id]['entry'].definition["ignore"]
        if not ignored and "ignore" in entries[entry_id]['definition']:
          ignored = entries[entry_id]['definition']["ignore"]
        if not ignored:
          ignore_interval = 0
          if "ignore_interval" in entries[entry_id]['entry'].definition:
            ignore_interval = entries[entry_id]['entry'].definition["ignore_interval"]
          if "ignore_interval" in entries[entry_id]['definition']:
            ignore_interval = entries[entry_id]['definition']["ignore_interval"]
          if ignore_interval > 0 and self.topic in entries[entry_id]['entry'].publish_last_seen and (timems() / 1000) - entries[entry_id]['entry'].publish_last_seen[self.topic] < ignore_interval - 1:
            ignored = True
        if not ignored:
          self._publishedMessages.append(PublishedMessage(self, entries[entry_id]['entry'], entries[entry_id]['topic'], entries[entry_id]['definition'], entries[entry_id]['matches'] if entries[entry_id]['matches'] != [True] else []))
        #else:
        #  logging.debug("SYSTEM> Ignored publishedmessage {entry_id}.{topic_rule} ({topic})".format(entry_id = entry_id, topic_rule = entries[entry_id]['topic'], topic = self.topic))
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
    m = Message(self.topic, copy.deepcopy(self.payload), self.qos, self.retain, self.payload_source, self.received, self.id)
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
              if ('listen_all_events' in config and config['listen_all_events']) or (eventname in events_listeners and ("*" in events_listeners[eventname] or self.entry.id in events_listeners[eventname] or self.entry.id_local in events_listeners[eventname])) or ('events_debug' in self.definition and self.definition['events_debug']):
                _s = _stats_start()
                event = _entry_event_process(self.entry, eventname, eventdef, self)
                _stats_end('PublishedMessages.event_process', _s)
                if 'events_debug' in self.definition and self.definition['events_debug']:
                  logging.debug("{id}> EVENT_DEBUG> {topic}={payload} -> {eventname}:{event}".format(id = self.entry.id, topic = self.topic, payload = self.payload, eventname = eventname, event = event))
                  if self.definition['events_debug'] >= 2:
                    event = False
                if event:
                  _s = _stats_start()
                  eventdata = _entry_event_publish(self.entry, event['name'], event['params'], time() if not self.message.retain else 0)
                  _stats_end('PublishedMessages.event_publish', _s)
                  if eventdata:
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
  for pm in m.publishedMessages():
    pm.entry.last_seen = int(timems / 1000)
    pm.entry.publish_last_seen[topic] = int(timems / 1000)
    _s = _stats_start()
    events = pm.events()
    _stats_end('on_mqtt_message.generate_events', _s)
    _stats_end('on_mqtt_message(' + str(topic) + ').generate_events', _s)
    _s = _stats_start()
    for eventdata in events:
      _entry_event_invoke_listeners(pm.entry, eventdata, 'message', pm)
    _stats_end('on_mqtt_message.invoke_elisteners', _s)
    _stats_end('on_mqtt_message(' + str(topic) + ').invoke_elisteners', _s)
  
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
  @param time timestamp event has been published, 0 if from a retained message, -1 if from an event data initialization, -X if from a stored event data (X is the original time)
  """
  global events_groups
  #logging.debug("SYSTEM> Published event " + entry.id + "." + eventname + " = " + str(params))
  
  data = { 'name': eventname, 'time': time, 'params': params, 'changed_params': {}, 'keys': {} }
  event_keys = entry_event_keys(entry, eventname)
  data['keys'] = {k:params[k] for k in event_keys if params and k in params}
  keys_index = entry_event_keys_index(data['keys'])
  
  if time > 0 and entry.id + '.' + eventname in events_groups:
    events_groups_push(entry.id + '.' + eventname, entry, data, event_keys, keys_index)
    return None
  
  return _entry_event_publish_internal(entry, eventname, params, time, data, event_keys, keys_index)

def _entry_event_publish_internal(entry, eventname, params, time, data, event_keys, keys_index):
  global events_published, events_published_lock
  
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
  
  if eventname not in entry.events:
    entry.events[eventname] = ['?']
    logging.warn("Generated an event not declared by the entry, added now to the declaration. Entry: {entry}, event: {eventname}".format(entry = entry.id, eventname = eventname))
  
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
        condition = " && ".join([ "params['" + k + "'] == " + utils.json_export(full_params[k]) for k in event_keys if full_params and k in full_params])

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
        for listener, condition, reference_entry_id in events_listeners[eventname][entry_ref]:
          if listener:
            _s = _stats_start()
            if condition is None or _entry_event_params_match_condition(eventdata, condition):
              #logging.debug("_entry_event_invoke_listeners_GO")
              listener(entry, eventname, copy.deepcopy(eventdata), caller, published_message)
            _stats_end('event_listener(' + str(listener) + '|' + str(condition) + ')', _s)
            
  if handler_on_all_events:
    for h in handler_on_all_events:
      h(entry, eventname, copy.deepcopy(eventdata), caller, published_message)

def _entry_event_params_match_condition(eventdata, condition):
  """
  @params eventdata { 'params' : ..., ... }
  """
  return scripting_js.script_eval(condition, {'params': eventdata['params'], 'changed_params': eventdata['changed_params'], 'keys': eventdata['keys']}, cache = True)

def _entry_event_publish_and_invoke_listeners(entry, eventname, params, time, caller, published_message):
  # @param caller is "events_passthrough" in case of events_passthrough, "group" for event grouping (see events_groups)
  eventdata = _entry_event_publish(entry, eventname, params, time)
  if eventdata:
    _entry_event_invoke_listeners(entry, eventdata, caller, published_message)




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
          s['listeners'].append({ 'topic_rule': rtopic_rule, 'expiry': timems() + (utils.read_duration(t['duration']) if 'duration' in t else default_duration) * 1000, 'count': 0, 'max_count': t['count'] if 'count' in t else default_count })
  
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
      if l['expiry'] + delay > now and (l['count'] == 0 or l['count'] < l['max_count']):
        matches = topic_matches(l['topic_rule'], message.topic, message.payload)
          
        if matches['matched']:
          l['count'] = l['count'] + 1
          final = False if [m['count'] for m in x['listeners'] if m['count'] == 0 and m['max_count'] > 0] else True
          if x['callback']:
            x['callback'](x['entry'], x['id'], message, final, x['message'])
          x['called'] = True
  for x in subscribed_response:
    if [l for l in x['listeners'] if l['expiry'] + delay <= now or l['count'] <= 0]:
      x['listeners'] = [l for l in x['listeners'] if l['expiry'] + delay > now and (l['count'] == 0 or l['count'] < l['max_count'])]

def _subscription_timer_thread():
  """
  An internal thread, initialied by init(), that scans for expired subscribed response topics (@see subscribe_response)
  """
  global subscribed_response, destroyed
  while not destroyed:
    now = timems()
    delay = round(mqtt.queueDelay() / 1000 + 0.49) * 1000
    # If there is a lot of delay in mqtt queue, we can assume there are probably a lot of messages managed by mqtt broker, so it could be a normal slowly processing of messages. So we add some more delay (20%).
    delay = delay * 1.2
    
    for x in subscribed_response:
      if [l for l in x['listeners'] if l['expiry'] + delay <= now]:
        # TODO DEBUG REMOVE
        if 'called' not in x:
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
    
    events_groups_check()
    sleep(.5)

def events_groups_push(egkey, entry, data, event_keys, keys_index):
  global events_groups, events_groups_lock
  with events_groups_lock:
    if keys_index in events_groups[egkey]['data'] and data['time'] - events_groups[egkey]['data'][keys_index]['time'] >= events_groups[egkey]['group_time']:
      eventdata = _entry_event_publish_internal(entry, data['name'], events_groups[egkey]['data'][keys_index]['data']['params'], events_groups[egkey]['data'][keys_index]['data']['time'], events_groups[egkey]['data'][keys_index]['data'], event_keys, keys_index)
      if eventdata:
        _entry_event_invoke_listeners(entry, eventdata, 'group', None)
      del events_groups[egkey]['data'][keys_index]
    if keys_index not in events_groups[egkey]['data']:
      events_groups[egkey]['data'][keys_index] = {
        'time': data['time'], 
        'data': data,
      }
    else:
      for k in data['params']:
        events_groups[egkey]['data'][keys_index]['data']['params'][k] = data['params'][k]

def events_groups_check():
  global events_groups, events_groups_lock
  now = mqtt.queueTimems()
  if now > 0:
    now = int(now / 1000)
  else:
    now = time()
  for egkey in events_groups:
    to_delete = []
    with events_groups_lock:
      for keys_index in events_groups[egkey]['data']:
        if now - events_groups[egkey]['data'][keys_index]['time'] >= events_groups[egkey]['group_time']:
          d = egkey.find(".")
          if d > 0:
            entry = entry_get(egkey[:d])
            if entry:
              eventname = egkey[d + 1:]
              event_keys = entry_event_keys(entry, eventname)
              eventdata = _entry_event_publish_internal(entry, eventname, events_groups[egkey]['data'][keys_index]['data']['params'], events_groups[egkey]['data'][keys_index]['data']['time'], events_groups[egkey]['data'][keys_index]['data'], event_keys, entry_event_keys_index(events_groups[egkey]['data'][keys_index]['data']['keys']))
              if eventdata:
                _entry_event_invoke_listeners(entry, eventdata, 'group', None)
              to_delete.append(keys_index)
    for i in to_delete:
      with events_groups_lock:
        del events_groups[egkey]['data'][i]


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
  global events_groups
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
        elif eventname.endswith(':group') and isinstance(entry.definition['publish'][topic]['events'][eventname], int) and entry.definition['publish'][topic]['events'][eventname] > 0:
          events_groups[entry.id + '.' + eventname[0:-6]] = { 'group_time': entry.definition['publish'][topic]['events'][eventname], 'data': {}}
  if 'events' in entry.definition:
    for eventname in entry.definition['events']:
      if eventname.endswith(':keys'):
        entry.events_keys[eventname[0:-5]] = entry.definition['events'][eventname]
      elif eventname.endswith(':init'):
        data = entry.definition['events'][eventname] if isinstance(entry.definition['events'][eventname], list) else [ entry.definition['events'][eventname] ]
        for eventparams in data:
          _entry_event_publish(entry, eventname[0:-5], eventparams, -1)
      elif eventname.endswith(':group') and isinstance(entry.definition['events'][eventname], int) and entry.definition['events'][eventname] > 0:
        events_groups[entry.id + '.' + eventname[0:-6]] = { 'group_time': entry.definition['events'][eventname], 'data': {}}

  # events_passthrough translates in event_propagation via on/events definition
  if 'events_passthrough' in entry.definition:
    if isinstance(entry.definition['events_passthrough'], str):
      entry.definition['events_passthrough'] = [ entry.definition['events_passthrough'] ]
    for ep in entry.definition['events_passthrough']:
      if isinstance(ep, str):
        ep = { "on": ep }
      if "remove_keys" not in ep:
        ep["remove_keys"] = True
      on_event(ep["on"], _on_events_passthrough_listener_lambda(entry, ep), entry, 'events_passthrough')
      eventname = ep["rename"] if "rename" in ep else None
      if not eventname:
        ref = decode_event_reference(ep["on"])
        eventname = ref['event']
      if eventname and eventname not in entry.events:
        entry.events[eventname] = ['#passthrough']
      

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
            entry.events['action/' + actionname[0:-5]] = [ '#action' ]
            _entry_event_publish(entry, 'action/' + actionname[0:-5], eventparams, -1)
  if 'actions' in entry.definition:
    for actionname in entry.definition['actions']:
      if actionname.endswith(':init'):
        data = entry.definition['actions'][actionname] if isinstance(entry.definition['actions'][actionname], list) else [ entry.definition['actions'][actionname] ]
        for eventparams in data:
          entry.events['action/' + actionname[0:-5]] = [ '#action' ]
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
  @param listener a callback, defined as listener(entry, eventname, eventdata, caller, published_message) - caller = "message|import|events_passthrough", published_message = source message (null for import)
  @param reference_entry The entry generating this reference. If an entry is doing this call, you MUST pass this parameter. It's used to clean the enviroment when the caller in unloaded. Also used for implicit reference (if eventref dont contains entry id).
  @param reference_tag Just for logging purpose, the context of the entry defining the the event_reference (so who reads the log could locate where the event_reference is defined)
  """
  global events_listeners
  if not reference_entry:
    logging.warn("SYSTEM> Called system.on_event without a reference entry: {eventref}".format(eventref = eventref))
  d = decode_event_reference(eventref, default_entry_id = reference_entry.id if reference_entry else None) if not isinstance(eventref, dict) else eventref
  if not d:
    logging.error("#{entry}> Invalid '{type}' definition{tag}: {defn}".format(entry = reference_entry.id if reference_entry else '?', type = 'on event' if listener else 'events_listen', tag = (' in ' + reference_tag) if reference_tag else '', defn = eventref))
  else:
    if not d['event'] in events_listeners:
      events_listeners[d['event']] = {}
    #OBSOLETE: d['entry'] = entry_id_expand(d['entry'])
    if not d['entry'] in events_listeners[d['event']]:
      events_listeners[d['event']][d['entry']] = []
    events_listeners[d['event']][d['entry']].append([listener, d['condition'], reference_entry.id if reference_entry else None])

def add_event_listener(eventref, reference_entry = None, reference_tag = None):
  """
  Add a generic listener for event (so the events are stored on "listened_events" parameter on handlers, and they can be fetched by event_get())
  """
  on_event(eventref, None, reference_entry, reference_tag)

def entry_on_event_lambda(entry):
  return lambda event, listener, condition = None, reference_entry = None, reference_tag = None: entry_on_event(entry, event, listener, condition, reference_entry, reference_tag)

def entry_on_event(entry, event, listener, condition = None, reference_entry = None, reference_tag = None):
  """
  Adds an event listener on the specified entry.event(condition)
  @param event name of event matched
  @param listener a callback, defined as listener(entry, eventname, eventdata, caller, published_message)
  @param condition javascript condition to match event. Example: "port = 1 && value < 10"
  @param reference_entry The entry generating this reference. If an entry is doing this call, you MUST pass this parameter. It's used to clean the enviroment when the caller in unloaded. Also used for implicit reference (if eventref dont contains entry id).
  @param reference_tag Just for logging purpose, the context of the entry defining the the event_reference (so who reads the log could locate where the event_reference is defined)
  """
  on_event({'entry': entry.id, 'event': event, 'condition': condition}, listener, reference_entry, reference_tag)

"""
Remove all event listener with target or reference equal to reference_entry_id
"""
def remove_event_listener_by_reference_entry(reference_entry_id):
  global events_listeners
  clean = False
  for eventname in events_listeners:
    clean2 = False
    for entry_ref in events_listeners[eventname]:
      if entry_id_match(entry_ref, reference_entry_id):
        events_listeners[eventname][entry_ref] = []
      else:
        events_listeners[eventname][entry_ref] = [[listener, condition, ref] for listener, condition, ref in events_listeners[eventname][entry_ref] if ref != reference_entry_id]
      if not events_listeners[eventname][entry_ref]:
        clean2 = True
    if clean2:
      events_listeners[eventname] = {entry_ref: events_listeners[eventname][entry_ref] for entry_ref in events_listeners[eventname] if events_listeners[eventname][entry_ref]}
    if not events_listeners[eventname]:
      clean = True
  if clean:
    events_listeners = { eventname: events_listeners[eventname] for eventname in events_listeners if events_listeners[eventname]}

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

def do_action(actionref, params = {}, reference_entry_id = None, if_event_not_match = False, if_event_not_match_keys = False, if_event_not_match_timeout = None):
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
    exec_context = { 'params': params }
    for topic in entry.actions[action]:
      actiondef = entry.definition['subscribe'][topic]['actions'][action]
      if isinstance(actiondef, str):
        actiondef = { 'payload': actiondef }
      if actiondef['payload']:
        if 'init' in actiondef and actiondef['init']:
          exec_context = scripting_js.script_exec(actiondef['init'], exec_context)
        if init:
          exec_context = scripting_js.script_exec(init, exec_context)
        payload = scripting_js.script_eval(actiondef['payload'], exec_context, to_dict = True)
        if payload != None:
          if 'topic' in actiondef and actiondef['topic']:
            if actiondef['topic'].startswith('js:') or actiondef['topic'].startswith('jsf:'):
              topic = scripting_js.script_eval(actiondef['topic'], exec_context, to_dict = True)
            else:
              topic = actiondef['topic']
          publish = [topic, payload]
          break
      else:
        publish = [topic, None]
        break

    if publish:
      entry.publish(publish[0], publish[1])
      event_get_invalidate_on_action(entry, action, exec_context['params'], if_event_not_match)
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
    return utils.nan_remove(ret[0] if len(ret) <= 1 else ret)

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

def events_import(data, import_mode = 0, invoke_mode = 2):
  global events_published, events_published_lock
  
  """
  @param import_mode = -1 only import events not present, or with time = -1, and set the time as -time [used to import stored events]
  @param import_mode = 0 only import events not present, or with time = -1|0
  @param import_mode = 1 ... also events with time >= than the one in memory
  @param import_mode = 2 ... also all events with time > 0
  @param import_mode = 3 import all events (even if time = -1|0)
  @param invoke_mode = 0 never invoke listeners of imported events
  @param invoke_mode = 1 invoke listeners of imported events with time >= 0 (recent events)
  @param invoke_mode = 2 invoke listeners of imported events with time != -1 (recent events & stored events, NOT for :init events)
  @param invoke_mode = 3 invoke listeners of ALL imported events
  """
  for entry_id in data:
    for eventname in data[entry_id]:
      for keys_index in data[entry_id][eventname]:
        eventdata = data[entry_id][eventname][keys_index]
        temporary = "temporary" in eventdata['params'] and eventdata['params']['temporary']
        #keys_index = entry_event_keys_index(eventdata['keys'] if 'keys' in eventdata else {}, temporary)
        with events_published_lock:
          if not eventname in events_published:
            events_published[eventname] = {}
          if not entry_id in events_published[eventname]:
            events_published[eventname][entry_id] = {}
          prevdata = events_published[eventname][entry_id][keys_index] if keys_index in this.events_published[eventname][entry_id] else None
          go = import_mode == 3 or (not prevdata) or (prevdata['time'] == -1 and eventdata['time'] > 0)
          if (not go) and import_mode == 0:
            go = prevdata['time'] <= 0 and eventdata['time'] > 0
          if (not go) and import_mode == 1:
            go = eventdata['time'] >= prevdata['time']
          if (not go) and import_mode == 2:
            go = eventdata['time'] > 0
          if go:
            if import_mode == -1 and eventdata['time'] > 0:
              eventdata['time'] = - eventdata['time']
            events_published[eventname][entry_id][keys_index] = eventdata
            if (not temporary) and invoke_mode > 0 and (invoke_mode == 3 or (invoke_mode == 2 or eventdata['time'] != -1) or (invoke_mode == 1 and eventdata['time'] >= 0)):
              _e = entry_get(entry_id)
              if _e:
                _entry_event_invoke_listeners(_e, eventdata, 'import')

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
      condition = condition + (' && ' if condition else '') + 'params["' + k + '"] == ' + utils.json_export(eventdata['keys'][k])
    condition = '(js: ' + condition + ')'
  return entry_id + '.' + eventname + condition

def transform_action_reference_to_event_reference(actionref, return_decoded = False):
  """
  Transform an action reference to a valid event reference.
  Delete the "-set" postfix to action name and replace "=" with "==" and ";" with "&&" in init code
  Example: "js: action-set(js: params['x'] = 1; params['y'] = 2;)" > "js: action(js: params['x'] == 1 && params['y'] == 2)"
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
  Example: "js: event(js: params['x'] == 1 && params['y'] == 2)" > "js: event-set(js: params['x'] = 1; params['y'] = 2;)"
  """
  d = decode_event_reference(eventref, no_event = True) if not isinstance(eventref, dict) else eventref
  if not d:
    return None
  r = {'entry': d['entry'], 'action': (d['event'] + '-set') if d['event'] else None, 'init': d['condition'].replace('==', '=').replace('&&', ';') if d['condition'] else None }
  return r if return_decoded else r['entry'] + (('.' + r['action']) if r['action'] else '') + (('(' + r['init'] + ')') if r['init'] else '')

