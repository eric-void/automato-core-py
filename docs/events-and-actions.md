# Events and actions

[Documentation index](README.md)

## Events

### Declaring events

Events are declared in the `events` key of a publish rule. Each event is a `js:` or `jsf:` expression (see [Scripting](scripting.md)) that receives `topic`, `payload` (after `payload_transform`) and `matches`, and returns the event params:

```json
"events": {
  "temperature": "js:({value: payload['temp'], 'value:unit': '°C'})",
  "battery": "js:('bat' in payload ? {value: payload['bat']} : null)",
  "output": [
    "js:({port: '0', value: payload['relay0'] ? 1 : 0})",
    "js:({port: '1', value: payload['relay1'] ? 1 : 0})"
  ]
}
```

- The result is an object with the params. `null` or `false` means "no event in this message"; `true` means an event with no params.
- A list emits one event per expression (typically one per `port`).
- Expressions must be deterministic: the same payload must give the same result.

The same event name can be declared on more publish rules: all of them update the same event.

### Meta declarations

Keys with a `:` suffix configure an event instead of declaring it. They can be in a publish rule's `events` or in the top level `events` of the definition:

| Key | Description |
|---|---|
| `name:keys` | Params that identify different instances of the event, instead of `event_keys` of the entry (default `["port"]`) |
| `name:init` | Initial params, set when the entry is loaded: `{ "unit": "W" }`, or a list with one object per key value |
| `name:group` | Seconds. Events with the same name arriving within that time are merged and emitted once, at the end (with a delay, and with no source message) |

### Event keys

Params listed in the event keys tell apart instances of the same event: a device with two relays emits `output` with `port: "0"` and `port: "1"`, and the core keeps the last value of each one separately. Data of an event with no keys is merged into every keyed instance (a `{"unit": "W"}` declared without `port` applies to every port).

### Event data

Listeners receive the event data:

```json
{
  "name": "output",
  "params": { "port": "0", "value": 1, "timer_to": 0 },
  "changed_params": { "value": 1 },
  "keys": { "port": "0" },
  "time": 1731400000
}
```

- `params` are the params of the message, merged with the last known params of the same instance (a message that carries only `value` keeps the previous `unit`).
- `changed_params` contains the params whose value changed since the previous event.
- `time` is the timestamp of the event; `0` for a retained message received at connection, `-1` for `:init` data, a negative timestamp for data restored from storage.

Params with special meaning:

| Param | Meaning |
|---|---|
| `value` | The main value of the event |
| `port` | The sub-device (a channel of a multi-relay device) |
| `channel` | A way of reaching the same device or port (`single`, `double`, `long` for a button) |
| `<param>:unit` | Unit of a param: `"power:unit": "W"`; for `value`, `"value:unit"` or simply `unit` |
| `<param>:def` | Allowed values: a list (`[0, 1]`), `"int"`, `"float"` (with `"<param>:def:limits": [1, 100]`) |
| `<param>_reported` | The original value reported by the device, when the event adjusts it |
| `temporary` | The state is temporary (a button press): it is not stored, and `event_get` keeps returning the previous state |
| `timer_to` | Timestamp at which a timed state ends |
| `source` | Where the data comes from (a module, a website) |
| `x_<name>` | Entry-specific params outside the standard |

### When events are computed

Computing events has a cost, so a core computes an event only if someone **listens** to it: an `on` rule, an `events_listen` declaration, a module calling `entry.on(...)`. With `listen_all_events: true` in the configuration every event is computed: browser clients do this, while a small node may not.

### Event references

Listeners, conditions and `event_get` use a string reference:

```
entry_id.event
entry_id.event(js: params['port'] == '1' && params['value'] > 0)
*.temperature
.output                       (an event of the entry that contains the reference)
kitchen_light@home-server.output
```

The condition is a JavaScript expression that receives `params`, `changed_params` and `keys`. An entry id with no `@` matches that local id on any node.

### Listening to events

In a definition (local to the node running the entry):

```json
"on": {
  "door_sensor.contact(js:params['value'] == 0)": {
    "do": "siren.output-set(js:params['value'] = 1)",
    "do_if_event_not_match": true
  },
  ".output": { "handler": "on_output" }
}
```

- `do`: one or more action references, called with the event params (and the init code of the reference).
- `do_if_event_not_match`: skip the action if the event of the target already has the requested value (for example the siren is already on).
- `handler`, `script`: code run by the node (see the [node documentation](https://github.com/eric-void/automato-node-py/blob/master/docs/entries.md)).

### Event passthrough

`events_passthrough` re-emits events of another entry as events of this one, for example an item that exposes the state of the device it controls:

```json
"events_passthrough": [
  "relay.output(js:params['port'] == '0')",
  { "on": "relay.energy", "rename": "power", "init": "js:params['x_source'] = 'relay'", "remove_keys": true }
]
```

`remove_keys` (default `true`) removes the keys of the source event (here `port`), `rename` changes the event name, `init` runs code on `params` before the event is emitted.

## Actions

### Declaring actions

Actions are declared in the `actions` key of a subscribe rule. The simple form is an expression that receives `params` and returns the payload to publish on the rule's topic:

```json
"subscribe": {
  "home/light/set": {
    "actions": {
      "output-set": "js:params['value'] ? 'on' : 'off'",
      "output-get": ""
    }
  }
}
```

- If the expression returns `null`, nothing is published on this topic: the core tries the next subscribe rule that declares the same action. This allows an action to choose its topic from the params.
- An empty string publishes an empty message on the topic.
- Objects and lists returned are published as JSON.

The full form:

```json
"output-set": {
  "init": "js:if (!('port' in params)) params['port'] = '0'",
  "topic": "js:'shellies/xyz/relay/' + params['port'] + '/command'",
  "payload": "js:params['value'] ? 'on' : 'off'",
  "qos": 1,
  "retain": false
}
```

`init` changes `params` before the payload is computed; `topic` is needed when the rule is a wildcard or a regular expression. Changes to event keys (like `port`) must be done in `init`, so the core knows which cached event the action affects.

`name:init` (in the rule's `actions` or in the top level `actions`) sets initial params of a pseudo-event `action/<name>`, used by clients to describe the action: `"output-set:init": { "value:def": [0, 1] }`.

### Calling actions

An action reference is `entry_id.action(init code)`:

```
kitchen_light.output-set
relay2.output-set(js:params['port'] = '1')
```

Params are passed separately (`do_action("kitchen_light.output-set", {"value": 1})`). The order is: the given params, the `init` of the action definition, the init code of the reference, then the payload expression.

With `if_event_not_match` the action is skipped when the corresponding event already has the requested params. The event is found by removing `-set` from the action name and turning the init code into a condition (`params['port'] = '1'` becomes `params['port'] == '1'`).

After an action, the cached event it affects is invalidated, so `event_get` does not return a stale value before the device answers.

## Standard events

Using the same names and params makes devices interchangeable. These are the ones used by the modules and the clients:

| Event | Params |
|---|---|
| `output` | `value` (usually 0/1, a string for multi-state outputs), `intensity`, `timer_to` |
| `input` | `value`, `channel` (`single`, `double`, `long`...) |
| `connected` | `value` (0/1), `temporary`, `ip_address`, `mac_address`, `was_connected`, `method`, `prev_ip_address` |
| `alive` | `value`: the entry is connected and sending messages (see the health module) |
| `failure` | `value`, `reason` |
| `clock` | `value`: a timestamp (the device time, or a time it reports) |
| `temperature` | `value`, `unit` (`°C`) |
| `humidity` | `value` (%) |
| `pressure` | `value`, `unit` (`hPa`) |
| `illuminance` | `value` |
| `energy` | `power`, `power_reactive`, `power_apparent`, `power_factor`, `energy_today`, `energy_yesterday`, `energy_total`, `energy_returned`, `voltage`, `current`, `frequency`, with their `:unit` params |
| `battery` | `value` (0-100) |
| `autonomy` | `value` (seconds) |
| `load` | `value` (%) |
| `netspeed`, `bandwidth` | `download`, `upload`, `ping` with units, `error` |
| `contact`, `occupancy`, `water-leak`, `tamper`, `vibration` | `value` (0/1); `vibration` is `temporary` |
| `presence-in` | `who`, `before_someone_inside`, `region` |
| `presence-out` | `who`, `after_someone_inside`, `region` |
| `location` | `latitude`, `longitude`, `altitude`, `radius`, `regions`, `source` |

Matching actions: `output-set` (`value`, `timer_to` in seconds or as a timestamp), `output-get`, `output-invert`, `input-get`, and in general `<event>-set` / `<event>-get`.
