# Notifications

[Documentation index](README.md)

Every published message can be described as a human readable text, with a **level**. Nodes use these texts for logs, the notification history, Telegram and web push; clients show them in their message views. The text is built by the core from the definition of the publish rule.

## Levels

| Level | Value | Typical use |
|---|---|---|
| `debug` | 20 | Telemetry, periodic readings |
| `response` | 40 | Answers to requests |
| `info` | 50 | Normal state changes (the default) |
| `warn` | 60 | Something needs attention |
| `error` | 70 | A failure |
| `critical` | 90 | A failure that needs action now (a node is dead, an alarm fired) |

## Keys

They can be set on a publish rule, or at the top level of the definition as defaults for all rules.

| Key | Description |
|---|---|
| `notify` | A template (below) |
| `notify_handler` | A `js:`/`jsf:` expression returning the text, with `topic`, `payload` and `matches`. Used instead of `notify` when it returns a value |
| `notify_level` | Level of the notification, default `info` |
| `notify_type` | A category of the notification (free text, used to filter) |
| `notify_change_level` | Level to use when the text differs from the previous one of the same topic |
| `notify_change_duration` | A duration: after a change, `notify_change_level` is not used again until this time has passed |
| `notify_if` | Overrides for some payloads (below) |

## Templates

`notify` is a Python [format string](https://docs.python.org/3/library/string.html#format-string-syntax). Available fields:

| Field | Value |
|---|---|
| `{payload}` | The payload; for an object, `{payload[key]}` |
| `{_}` | For a payload that is not an object, the payload itself, so it can use formatters: `{_[payload!strftime]}` |
| `{matches[1]}` | Groups captured by the topic rule |
| `{caption}` | Caption of the entry |

Formatters, inside the brackets of an object field:

| Formatter | Result |
|---|---|
| `{payload[time!strftime]}`, `{payload[time!strftime(%H:%M)]}` | A timestamp as a date |
| `{payload[state!caption]}` | The caption of the value, from the `payload` description of the rule: `"payload": { "state": { "0": { "caption": "off" }, "1": { "caption": "on" } } }` |
| `{payload[x!'none']}` | The value, or `none` if missing (missing values are shown as `-` otherwise) |

```json
"notify": "{caption}: temperature {payload[temperature]}°, humidity {payload[humidity]}% at {payload[time!strftime(%H:%M)]}"
```

## Conditional notifications

`notify_if` maps conditions (`js:` expressions with `topic`, `payload`, `matches`) to overrides of the other keys. Every matching condition applies its overrides, in order:

```json
"notify": "{caption} is {payload}",
"notify_if": {
  "js:payload == 'offline'": { "notify_level": "warn", "notify": "{caption} is offline!", "notify_next_level": "info" }
}
```

`notify_next_level` sets the level of the **next** notification of the same topic: here, the message that says the device is back online is shown with `info` level even if the default is `debug`.

## Where notifications go

The core only builds the text and the level of a published message (`PublishedMessage.notificationString()`, `notificationLevel()`). Distributing them is the job of the `notifications` module of the node: it keeps a history, answers `notifications/history/get`, optionally republishes them on `notify/<level>/<topic>`, and passes them to the modules that send them (Telegram, web push). See the [node modules](https://github.com/eric-void/automato-node-py/blob/master/docs/modules.md#notifications).
