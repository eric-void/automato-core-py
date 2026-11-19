# Python API

[Documentation index](README.md)

Most users run the core through [automato-node-py](https://github.com/eric-void/automato-node-py), which adds modules, scheduling and storage. The core can also be used alone, for a script or a bot that reads events and calls actions. The module is `automato.core.system`; functions are module level (the core is a singleton in the process).

The JavaScript core has the same functions, with the same names and parameters, as methods of an `AutomatoSystem` object.

## A minimal program

```python
import time
from automato.core import system

system.set_config({
  'name': 'my-script',
  'mqtt': { 'broker_host': '127.0.0.1', 'broker_port': 1883 },
  'listen_all_events': True,
  'entries': [
    {
      'device': 'sensor1',
      'publish': {
        'home/sensor1': {
          'type': 'object',
          'events': { 'temperature': "js:({value: payload['t'], 'value:unit': '°C'})" }
        }
      }
    }
  ],
})
system.boot()

def on_temperature(entry, eventname, eventdata, caller, published_message):
  print(entry.id, eventdata['params']['value'])

system.on_event('*.temperature', on_temperature)
system.init(lambda: print('connected'))

while True:
  time.sleep(1)
```

`entries` can be empty: a script connected to a broker where a node publishes `automato/metadata` can load the definitions from there (this is what the `nodes` module of the node does).

## Configuration

`set_config()` takes the node configuration. Keys read by the core:

| Key | Description |
|---|---|
| `name` | Node name: the `@node` part of local entry ids |
| `entries` | List of entry definitions loaded by `init()` |
| `mqtt` | MQTT settings (below) |
| `listen_all_events` | `true` to compute every event, not only the listened ones |
| `base_path` | Base directory for relative log paths |
| `log`, `error-log` | Log files (rotated daily); `error-log` gets only errors |
| `messages-log` | Log of all MQTT messages |
| `debug-log` | Log of the `debug` logger |
| `console-log` | `false` to disable the log on the console |
| `color-log` | `true` for colored console logs |
| `log-level` | `debug` (default), `info`, `warning`, `error` |
| `stats-detailed` | `true` to collect timing statistics per topic |

### MQTT settings

| Key | Default | Description |
|---|---|---|
| `broker_host` | `127.0.0.1` | |
| `broker_port` | `1883` | |
| `broker_user`, `broker_pass` | empty | |
| `client_id` | `automato-node` | Client id |
| `client_id_random_postfix` | `true` | Adds a random suffix to `client_id` |
| `publish_before_connection_policy` | `queue` | What `publish` does before the connection: `queue`, `connect` (connect, publish and disconnect) or `error` |
| `topic_filter_ignore` | `[]` | Topics to ignore (MQTT patterns or `/regexp/`) |
| `check_broken_connection` | `5` | Seconds: if a published message is not received back in this time, the connection is considered broken and restarted |
| `warning_slow_queue_ms` | `5000` | Logs a warning when processing of received messages is late by this time |

## Lifecycle

| Function | Description |
|---|---|
| `set_config(config)` | Sets the configuration |
| `boot()` | Initializes logging, locale and the MQTT layer |
| `init(callback)` | Loads `config['entries']` and connects; `callback()` is called when the connection is established |
| `destroy()` | Unloads all entries and disconnects |
| `broker()` | The MQTT layer (`automato.core.mqtt`): `publish(topic, payload, qos, retain)`, `get(topic)` (last payload received on a topic) |
| `time()`, `timems()` | Current time in seconds / milliseconds (can be shifted in tests) |

Callbacks, registered before `init()`:

| Function | Handler |
|---|---|
| `on_entry_load(h)` | `h(entry)` when a definition is loaded, before it is normalized |
| `on_entry_load_batch(h)` | `h(entries)`, may return ids of entries to reload |
| `on_entry_init(h)` | `h(entry)` when an entry is ready |
| `on_entry_init_batch(h)` | `h(entries)` |
| `on_entry_unload(h)` | `h(entry)` |
| `on_entries_change(h)` | `h(loaded_ids, unloaded_ids)` |
| `on_initialized(h)` | `h(all_entries)` after the first load |
| `on_message(h)` | `h(message)` for every MQTT message |
| `on_all_events(h)` | `h(entry, eventname, eventdata, caller, published_message)` for every event |

## Entries

| Function | Description |
|---|---|
| `entry_load(definitions, node_name, unload_other_from_node, id_from_definition, ...)` | Loads definitions: a list (with `id_from_definition = True`) or an object `{ entry_id: definition }` |
| `entry_unload(ids)`, `entry_reload(ids)` | |
| `entries()` | All entries, by full id |
| `entry_get(id)` | An entry, by full or local id |
| `entries_definition_exportable()` | Exportable definitions of all entries |
| `entries_publishers_of(topic, payload)`, `entries_subscribed_to(topic, payload)` | Entries publishing / subscribed to a topic, with the matching rule |
| `topic_matches(rule, topic, payload)` | Tests a topic rule |

An `Entry` has: `id`, `id_local`, `node_name`, `is_local`, `type`, `caption`, `config`, `definition`, `definition_exportable`, `events` and `actions` (names supported), `last_seen`, and the methods `publish(topic, payload, qos, retain, response_callback, no_response_callback)`, `topic(rule)` (resolves `.`, `@` and aliases), `on(event, listener, condition)` and `do(action, params, init)`.

## Events and actions

| Function | Description |
|---|---|
| `on_event(eventref, listener, reference_entry, reference_tag)` | Listens to `entry.event(condition)`; `listener(entry, eventname, eventdata, caller, published_message)`. `caller` is `message`, `import`, `events_passthrough` or `group`. Pass `reference_entry` when an entry registers the listener: it is removed when that entry is unloaded |
| `add_event_listener(eventref, reference_entry)` | Marks an event as listened, so it is computed and stored |
| `event_get(eventref, timeout, keys)` | Params of the last matching event, or `None`. `keys` returns a tuple of params (`_time` is the event time); `timeout` ignores older events |
| `event_get_time(eventref)` | Time of the last matching event |
| `entry_event_get(entry, eventname, condition, keys, timeout)` | Same, with separate arguments |
| `do_action(actionref, params, reference_entry_id, if_event_not_match, ...)` | Calls `entry.action(init)`; returns `True` if something was published |
| `entry_do_action(entry, action, params, init, ...)` | Same, with separate arguments |
| `entry_support_event(entry, name)`, `entry_support_action(entry, name)` | |
| `events_export()` / `events_import(data, import_mode, invoke_mode)` | Save and restore the known events (used for storage and by clients with `automato/data`) |
| `decode_event_reference(s)`, `decode_action_reference(s)` | Parse references into `{ entry, event/action, condition/init }` |

`event_get` works only for events that are computed: listened ones, or all of them with `listen_all_events`.
