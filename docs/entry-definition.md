# Entry definitions

[Documentation index](README.md)

A definition is a JSON object (a Python dict). In a node it is an element of the `entries` list of the configuration; a module can provide part of it in code. This page lists the keys understood by the core. The node and its modules add more keys (`run_interval`, `handler`, `script`, `install_on`, module-specific keys): see the [node documentation](https://github.com/eric-void/automato-node-py/blob/master/docs/entries.md).

Keys belong to one of these levels:

| Level | Meaning | Exported in metadata |
|---|---|---|
| Base | What every node and client must understand: types, descriptions, topic rules, events, actions | yes |
| Notifications | How to describe messages as text | yes |
| Shortcuts | Conveniences resolved when the entry is loaded: `entry_topic`, `topic_root`, topic aliases | no (already resolved) |
| Local | Handlers, scripts, scheduling, listeners: used only by the node running the entry | no |

The exported keys are listed in `ENTRY_DEFINITION_EXPORTABLE`, at the top of `system.py` (and identical in the JavaScript core).

## Top level keys

| Key | Level | Description |
|---|---|---|
| `module`, `device`, `item` | base | Type and local id of the entry (see [Concepts](concepts.md#entries)) |
| `id` | base | Local id, when it must differ from the module name (two instances of the same module) |
| `type` | base | `module`, `device` or `item`, if not given by the keys above |
| `caption` | base | Human readable name. Default: the local id |
| `description` | base | Longer description |
| `config` | base | Configuration of the entry (free form, used by modules) |
| `required` | base | Ids of entries this entry needs to work (used by the health module) |
| `disabled` | local | `true` to skip the entry when loading |
| `entry_topic` | shortcut | Base topic of the entry, used by topics starting with `@`. Default: `<type>/<local_id>` |
| `topic_root` | shortcut | Base topic used by topics starting with `.`. Default: `entry_topic` |
| `qos`, `retain` | shortcut | Defaults for all the publish rules of the entry |
| `notify*` | notifications | Defaults for all the topic rules, see [Notifications](notifications.md) |
| `event_keys` | base | Params that tell apart events with the same name, default `["port"]` (see [Events](events-and-actions.md#event-keys)) |
| `publish` | base | Topic rules of published messages (below) |
| `subscribe` | base | Topic rules of subscribed messages (below) |
| `events` | base | Meta declarations of events (`name:keys`, `name:init`, `name:group`) not tied to a topic |
| `actions` | base | Meta declarations of actions (`name:init`) |
| `events_passthrough` | base | Events of other entries re-emitted as events of this entry |
| `on` | local | Event listeners: `{ "entry.event(condition)": { "do": ..., "handler": ..., "script": ... } }` |
| `events_listen` | local | Events to keep in memory, so `event_get` can read them: `["entry.event", "*.event", ".event"]` |
| `ignore` | local | `true` to ignore every published message of the entry (subscriptions still work) |
| `ignore_interval` | local | A duration: a message on the same topic is processed only if this time passed since the previous one |

Durations are numbers of seconds, or strings like `30s`, `10m`, `2h`, `1d`, `1w`.

## Topic rules

`publish` and `subscribe` are objects whose keys are **topic rules** and whose values are the definitions of those topics.

| Rule | Matches |
|---|---|
| `home/light` | Exactly that topic |
| `home/+/status`, `home/#` | MQTT wildcards |
| `/^shellies/(.*)/relay/([0-9]+)$/` | A regular expression (between `/`), with capture groups |
| `home/light[on\|off]` | The topic, and a payload equal to one of the values |
| `home/#[/^on.*$/]` | The topic, and a payload matching the regular expression |
| `/^sensor/(.*)$/[js: payload['t'] > 0 && matches[1] == 'x']` | The topic, and a JavaScript condition on `topic`, `payload` and `matches` |

The groups captured by a regular expression are available to the events of the rule as `matches` (`matches[0]` is the whole topic, `matches[1]` the first group).

### Shortcuts

For local entries, topic rules are rewritten when the entry is loaded:

| Written | Becomes |
|---|---|
| `.` / `./status` | `topic_root` / `<topic_root>/status` |
| `@` / `@/health` | `entry_topic` / `<entry_topic>/health` |
| an alias | the value of its `topic` key |

An alias is a key that is not the real topic: `"state": { "topic": "./state", ... }`. The definition can then refer to `state` (for example in `response` or in `entry.publish('state', ...)`).

### When more rules match

A message is assigned to **one** publish rule and **one** subscribe rule per entry. When more rules of the same entry match, the one with the higher `topic_match_priority` wins; with the same priority an exact topic wins over wildcards and regular expressions.

`topic_match_priority` defaults to 1, or 0 for rules whose definition contains only `topic`, `description` and `notify*` keys (typically a catch-all like `"shellies/xyz/#": {}`). A catch-all with priority 0 is not used for topics that the same entry also subscribes to: that topic is considered a command, not a message of the entry.

## Publish rules

```json
"publish": {
  "./status": {
    "description": "Relay status",
    "type": "object",
    "qos": 0,
    "retain": true,
    "payload_transform": "js:payload['data']",
    "events": {
      "output": "js:({value: payload['relay'] ? 1 : 0})"
    },
    "notify": "{caption} is {payload[relay]}"
  }
}
```

| Key | Description |
|---|---|
| `topic` | Real topic rule, when the key is an alias |
| `description` | Text for humans and clients |
| `type` | Payload type: `object`, `array`, `string`, `int`, `float`, `none`, `unknown`... |
| `qos`, `retain` | Used when the entry publishes this topic |
| `payload` | Description of payload values, for clients: `{ "0": { "caption": "off" }, "1": { "caption": "on" } }` |
| `payload_transform` | A `js:`/`jsf:` expression that transforms the payload before events and notifications are computed |
| `events` | Events emitted by this message, see [Events and actions](events-and-actions.md) |
| `events_debug` | `1` logs every event computed from this rule; `2` logs them and does not emit them |
| `topic_match_priority` | See above |
| `notify`, `notify_handler`, `notify_level`, `notify_type`, `notify_if`, `notify_change_level`, `notify_change_duration` | See [Notifications](notifications.md) |
| `ignore`, `ignore_interval` | Like the top level keys, for this rule only |

A rule with no `description` and no `type` is **internal**: it is used to recognize messages, but clients may hide it.

The node adds local keys to publish rules: `handler`, `script`, `run_interval`, `run_cron`, `run_throttle`, `run_on`, `check_interval`.

## Subscribe rules

```json
"subscribe": {
  "./set": {
    "description": "Turn the relay on or off",
    "type": "string",
    "response": [ "./status" ],
    "actions": {
      "output-set": "js:params['value'] ? 'on' : 'off'"
    }
  }
}
```

| Key | Description |
|---|---|
| `topic` | Real topic rule, when the key is an alias |
| `description` | Text for humans and clients |
| `type` | Type of the payload received on this topic |
| `topic_syntax`, `topic_syntax_description`, `payload_syntax` | Free text describing the expected topic and payload, for rules with wildcards |
| `response` | Topics published in response to a message on this topic (below) |
| `actions` | Actions that publish on this topic, see [Events and actions](events-and-actions.md#actions) |
| `topic_match_priority` | See above |

A rule with no `description` and no `response` is **internal** (used by the entry itself, not offered to others).

The node adds `handler`, `script` and `publish` (topics to publish when a message arrives; it also sets `response` if missing).

### Responses

`response` lists the topics an entry publishes after receiving a message on the rule, so the sender can wait for them (`entry.publish(..., response_callback = ...)`):

```json
"response": [ "./status" ]
"response": [ { "topic": "./result/#", "count": 10, "duration": 10 } ]
```

A response is **final** when at least one message arrived for every topic with `count >= 1` (a plain string counts as `count: 1`); `count: 0` marks an optional topic. `count` is also the maximum number of messages passed to the callback, `duration` the seconds to wait (default 5). A response topic can contain `{matches[N]}`, replaced with the groups captured by the subscribe rule: `"/^device/(.*)/get$/": { "response": [ "device/{matches[1]}/status" ] }`.

## Example: a two channel relay

```json
{
  "device": "relay2",
  "caption": "Garden relay",
  "publish": {
    "/^garden/relay/([0-9]+)/status$/": {
      "type": "object",
      "events": { "output": "js:({port: matches[1], value: payload['value'] == 'on' ? 1 : 0})" }
    }
  },
  "subscribe": {
    "/^garden/relay/([0-9]+)/cmd$/": {
      "actions": {
        "output-set": {
          "topic": "js:'garden/relay/' + params['port'] + '/cmd'",
          "payload": "js:params['value'] ? 'on' : 'off'"
        }
      }
    }
  }
}
```

The `port` param tells apart the two channels: `event_get("relay2.output(js:params['port'] == '1')")` reads the second one, `do_action("relay2.output-set", {"port": "1", "value": 1})` turns it on.
