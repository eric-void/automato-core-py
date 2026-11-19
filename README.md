# automato-core-py

Python core of **Automato**, a home automation framework built on MQTT.

Automato connects devices, services and scripts through an MQTT broker. Each of them is described by a JSON **definition**: which topics it publishes and how they turn into **events**, which topics it listens to and which **actions** it accepts. Nodes share these definitions over MQTT, so every node and every client can read the messages of any device and command it without knowing its topics.

This repository contains the shared model: entries, definitions, topic matching, events, actions, notifications and the JavaScript expressions embedded in definitions. It is used by the node ([automato-node-py](https://github.com/eric-void/automato-node-py)) and has a JavaScript twin for browsers ([automato-core-js](https://github.com/eric-void/automato-core-js)).

## The Automato projects

| Repository | What it is |
|---|---|
| **automato-core-py** (this one) | Python core: the entry/event/action model and the MQTT protocol |
| [automato-core-js](https://github.com/eric-void/automato-core-js) | The same core in JavaScript, for browser clients |
| [automato-node-py](https://github.com/eric-void/automato-node-py) | The node: a service that runs entries and modules (Shelly, Tasmota, Zigbee2MQTT, presence, scheduler, Telegram...) |
| [automato-http-client](https://github.com/eric-void/automato-http-client) | A static web client, built on the JavaScript core (in progress) |

## A quick example

A device that publishes its state on `kitchen/light/status` and accepts commands on `kitchen/light/cmd`:

```json
{
  "device": "kitchen_light",
  "caption": "Kitchen light",
  "publish": {
    "kitchen/light/status": {
      "type": "object",
      "events": { "output": "js:({value: payload['state'] == 'on' ? 1 : 0})" }
    }
  },
  "subscribe": {
    "kitchen/light/cmd": {
      "actions": { "output-set": "js:params['value'] ? 'on' : 'off'" }
    }
  }
}
```

When `{"state": "on"}` arrives on `kitchen/light/status`, the core emits the event `kitchen_light.output` with `{value: 1}`. Calling the action `kitchen_light.output-set` with `{value: 0}` publishes `off` on `kitchen/light/cmd`. Other entries can react to the event (`"on": {"kitchen_light.output(js:params['value'] == 1)": {...}}`), and a client can show a switch for it, using only the definition.

## Documentation

- [Concepts](docs/concepts.md): entries, nodes, events, actions, metadata
- [Entry definitions](docs/entry-definition.md): the full reference of a definition, publish and subscribe rules, topic rules
- [Events and actions](docs/events-and-actions.md): declaring events and actions, references, conditions, standard event names
- [Scripting](docs/scripting.md): the `js:` and `jsf:` expressions in definitions
- [Notifications](docs/notifications.md): human readable messages built from MQTT messages
- [MQTT protocol](docs/mqtt-protocol.md): payloads, metadata and data topics, what nodes and clients exchange
- [Python API](docs/python-api.md): using the core from Python

## Requirements

- Python 3.11 or newer (tested on 3.11 and 3.13)
- [paho-mqtt](https://pypi.org/project/paho-mqtt/) 2.x (1.x is still accepted)
- [Js2Py](https://github.com/PiotrDabkowski/Js2Py), to evaluate the JavaScript expressions of definitions. The PyPI release works up to Python 3.11; for newer versions install a patched fork first, for example:
  ```sh
  pip install 'Js2Py @ git+https://github.com/a-j-albert/Js2Py.git@cc64a715727f110b79310b8031e0ade9c501d5d3'
  ```
- python-dateutil

```sh
pip install -r requirements.txt
```

There is no package on PyPI: add `src` to `PYTHONPATH` (the node launcher does it for you, with the two repositories checked out side by side).

## Tests

Unit tests need no broker and run in a few seconds. They are in [tests](tests) and run, together with the node ones, by the node script:

```sh
automato-node-py/bin/unit-tests.sh            # in the test docker image
automato-node-py/bin/unit-tests.sh --local    # with the local python
```

The "live" tests in `src/automato/core/system_test.py` need a broker and run with the node test suite (`automato-node-py/bin/docker-tests.sh`).

Changes to the model must stay aligned with [automato-core-js](https://github.com/eric-void/automato-core-js): it has a parity test that runs the same scenario on both cores.

## Repository layout

| Path | Content |
|---|---|
| `src/automato/core/system.py` | Entries, definitions, topic index and matching, events, actions |
| `src/automato/core/mqtt.py` | MQTT client wrapper: connection, publish queue, payload decoding, cache |
| `src/automato/core/scripting_js.py` | Evaluation of `js:`/`jsf:` expressions through Js2Py |
| `src/automato/core/notifications.py` | Notification strings and levels |
| `src/automato/core/utils.py` | Durations, dates, JSON, compression, merge helpers |
| `src/automato/core/system_extra.py` | Logging and locale setup |
| `src/locales` | Translations (gettext) |
| `tests` | Unit tests |
| `docs` | Documentation |

## License

[MIT](LICENSE)
