# MQTT protocol

[Documentation index](README.md)

This page describes what Automato itself puts on the broker. Device topics (Shelly, Tasmota, Zigbee2MQTT...) keep their own formats: definitions describe them.

## Payloads

- Outgoing: strings are published as they are; any other value (object, list, number, boolean) is published as JSON; `None`/`null` publishes an empty message.
- Incoming: a payload that starts with `{` and ends with `}`, or starts with `[` and ends with `]`, is parsed as JSON. Everything else is a string (numbers too: `"21.5"` stays a string, expressions convert it when needed).

Both cores subscribe to `#` and process every message: the definitions decide which ones matter. A node can exclude topics with the `topic_filter_ignore` MQTT setting.

## Node topics

These topics are handled by the `nodes` module of [automato-node-py](https://github.com/eric-void/automato-node-py), with the default topic root `automato`.

### `automato/metadata`

Every node publishes the definitions it knows, at start, every 30 minutes, and when it learns new ones from another node:

```json
{
  "from_node": "home-server",
  "time": 1731400000,
  "nodes": {
    "home-server": { "description": "...", "time": 1731400000 },
    "router": { "description": "...", "time": 1731399000 }
  },
  "entries": "eJy..."
}
```

`entries` maps full entry ids to their exportable definitions (see [Entry definitions](entry-definition.md)). With the default `compress: true` it is a string: the JSON, compressed with zlib and encoded in base64. Without compression it is the object itself.

A node that receives the metadata of another node loads the entries of that node (unless it is configured as `local`), and publishes its own metadata again if it learned something new: in this way every node knows all the entries. A node whose metadata is older than `dead_time` (default 90 minutes) is considered dead: its entries are unloaded and `automato/dead-node` is published, with `{ "name", "last_seen", "time" }`.

`automato/metadata/get` asks every node to publish its metadata.

### `automato/data`

Published by the **master** node (`master: true` in its `nodes` configuration) when it receives `automato/data/get`. It is what a client needs to start: definitions, and the last known events.

```json
{
  "from_node": "home-server",
  "time": 1731400000,
  "+": "eJy..."
}
```

`+` is the compressed form (zlib + base64 of the JSON) of:

```json
{
  "entries": { "entry_id@node": { ...exportable definition... } },
  "events": { "entry_id@node": { "event_name": { "<keys index>": { ...event data... } } } },
  "last_seen": { "entry_id@node": 1731399990 }
}
```

Without compression the three keys are at the top level. `events` has the format of `events_export()`; a client passes it to `events_import()`. The master exports only the events it computes (the ones it listens to, or all of them with `listen_all_events`).

## Entry topics

Some modules add topics to every entry:

| Topic | Module | Content |
|---|---|---|
| `<entry_topic>/health` | `health` | `{ "value": "alive\|idle\|failure\|dead", "reason": "...", "time": ... }`, retained; it emits the `alive` and `failure` events |
| `health/status` | `health` | Health of all the local entries, on request (`health/status/get`) and every 10 minutes |

`entry_topic` defaults to `<type>/<local id>` (`device/kitchen_light`).

## Notification topics

Handled by the `notifications` module:

| Topic | Direction | Content |
|---|---|---|
| `notifications/history/get` | request | Asks for the history |
| `notifications/history` | response | `{ "time": ..., "+history": "<compressed list>" }` (or `"history": [...]`): a list of `{ "time", "level", "topic", "message" }` |
| `notify/<level>/<topic>` | event | The text of a notification, if `notify_send` is enabled |
| `notifications/subscribe/<level>/<topic>` | request | Payload `<client type>:<client data>`: subscribes a client (like a Telegram chat) to notifications. `<level>` is `_` (any), a level, or a level followed by `_` (that level or higher) |
| `notifications/unsubscribe/<level>/<topic>` | request | The opposite |

## How a client starts

1. Connect to the broker (websocket, for a browser) and subscribe to `#`.
2. Load the definitions cached from a previous session, if any.
3. Publish `automato/data/get`, and load `entries` and `events` from the `automato/data` response of the master node.
4. Process the messages: retained messages arrive at connection, the others when devices publish. `automato/metadata` updates the definitions when nodes change.
5. Show the state with `event_get`, send commands with `do_action`.

A client works without a master node too: it then learns the definitions from the periodic `automato/metadata` messages.
