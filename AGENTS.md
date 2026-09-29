# automato-core-py

Python implementation of Automato's shared core. Read `../AGENTS.md` and `../ARCHITECTURE.md` for workspace/protocol context.

## Layout

- `src/automato/core/system.py`: central entry registry, definition loading/export, MQTT topic matching, events/actions and lifecycle callbacks.
- `mqtt.py`: MQTT broker abstraction and message handling.
- `scripting_js.py`: evaluates embedded `js:`/`jsf:` definition snippets using Js2Py and provides the script context/helper functions.
- `notifications.py`, `utils.py`, `system_extra.py`: notification, shared utility, locale/logging support.
- `system_test.py`, `test.py`: core tests; docs contain glossary/event notes and examples.

## Guidance

This is a protocol-defining implementation; changes to entry schemas, exported metadata, events/actions, topic matching or payloads can affect the node, JS core, clients and config. Keep JS behavior in `../automato-core-js/src/automato/core` aligned when the API/wire semantics are shared. The node adds runtime/module semantics around this core; avoid importing node-specific assumptions into it.

Dependencies listed in `requirements.txt` are paho-mqtt and Js2Py. Use `PYTHONPATH=src` for source-tree imports where needed. Consult repository tests before changing behavior; do not assume stale docs are authoritative.
