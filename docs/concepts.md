# Concepts

[Documentation index](README.md)

## MQTT at the center

Everything in Automato goes through an MQTT broker. Devices (Shelly, Tasmota, Zigbee2MQTT...) already talk MQTT with their own topics and payloads; Automato does not replace them, it **describes** them. A definition says which topics belong to a device and how to read them, so any participant can interpret the traffic.

## Entries

An **entry** is anything Automato knows about: a physical device, a service, a module of a node, a virtual item (a light made of a relay and a button, an alarm, a schedule). Every entry has a **definition**, a JSON object (see [Entry definitions](entry-definition.md)).

There are three entry types, set by the key used to name the entry:

| Type | Declared with | Typical use |
|---|---|---|
| `module` | `"module": "name"` | Code loaded by a node (a Python module): clock, health, scheduler, telegram... |
| `device` | `"device": "id"` | A physical device or an external service, described only by its topics |
| `item` | `"item": "id"` | A logical object, often built on other entries (a toggle on a relay) |

The type can also be set with `"type"`. Only a `module` loads code on the node; otherwise the type is used for the default topic of the entry (`<type>/<local_id>`) and helps clients to group and show entries.

### Entry ids

The full id of an entry is `local_id@node_name`, for example `kitchen_light@home-server`. The local part comes from `id`, or from the value of `module`, `device` or `item`; characters other than letters, digits, `_` and `-` are replaced by `-`. Inside a definition, references may use only the local part: it matches the entry with that local id on any node.

## Nodes

A **node** is a running process of [automato-node-py](https://github.com/eric-void/automato-node-py). It reads a configuration file with its entries, loads the modules they need, connects to the broker and:

- turns incoming messages into events, and runs the handlers of its entries;
- runs periodic tasks (polling a device, publishing a status);
- publishes its entries' **metadata**, so other nodes and clients learn about them.

An installation can have more nodes (a router, a server, a Raspberry Pi), all connected to the same broker. Each node runs its own entries (**local** entries) and knows the entries of the other nodes from their metadata (**remote** entries): it can listen to their events and call their actions, but their code runs elsewhere.

## Clients

A client (a browser page, a bot) uses a core too. It loads the metadata from the broker, subscribes to the topics, and uses the same definitions to show the state and send commands. A client has no local entries: it only interprets.

## Publish and subscribe

A definition has two lists of topic rules:

- `publish`: topics the entry **publishes** (a device's status, a sensor's reading). Every rule can declare the events found in its messages, a notification text, and, for local entries, a handler that produces the message.
- `subscribe`: topics the entry **listens to** (commands, requests). Every rule can declare the actions that publish on it, the responses to expect, and a handler.

The point of view is always the entry's one: a relay *publishes* its state and *subscribes* to its command topic, even if the relay firmware is not Automato.

## Messages

When a message arrives, the core finds the publish rules matching its topic and, for every entry that publishes it, creates a **published message** (the message seen as coming from that entry). The same is done with subscribe rules (**subscribed messages**). From a published message come the entry's events and its notification.

## Events

An **event** is a normalized fact about an entry: `output` is on, `temperature` is 21.5, `connected` is 0. Its data is a set of params (`{value: 21.5, "value:unit": "°C"}`). Events are computed from payloads by small JavaScript expressions in the definition, so different devices produce the same events: a widget that shows `temperature` works for every sensor.

The core keeps the last value of every event (per entry and per **keys**, like `port` for a device with more relays), so the current state can be asked at any time with `event_get`.

## Actions

An **action** is a command an entry accepts: `output-set` with `{value: 1}`. Its definition turns the params into a topic and a payload. A caller does not need to know the topic: `do_action("kitchen_light.output-set", {"value": 1})` works for any device whose definition has that action.

By convention an event `xxx` has actions `xxx-set` (to change it) and `xxx-get` (to ask the device to publish it again).

## Metadata

The **exportable** part of a definition (types, descriptions, topic rules with their events, actions and notifications) is what nodes publish as metadata. Handlers, scripts and other local details are not exported. See [MQTT protocol](mqtt-protocol.md).

## Lifecycle of a message, in short

1. A message arrives on `kitchen/light/status`.
2. The core finds `kitchen_light` as its publisher, and computes its `output` event (only if someone listens to it, or with `listen_all_events`).
3. The event is stored, and its listeners are called: an `on` rule of another entry, a client widget, the history module.
4. A listener calls an action, `scheduler.output-set` or `siren.output-set`: the core builds topic and payload and publishes them.
5. The node also builds the notification text of the message, and the notifications module may forward it (Telegram, web push).
