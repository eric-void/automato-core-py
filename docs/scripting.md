# Scripting in definitions

[Documentation index](README.md)

Definitions contain small JavaScript snippets: event expressions, action payloads, conditions, payload transformations, notification handlers. The same definition is read by Python nodes and JavaScript clients, so the language is JavaScript in both: the Python core runs it through one of its [two engines](#engines-of-the-python-core), the JavaScript core natively.

## Two forms

| Prefix | Form | Example |
|---|---|---|
| `js:` | An expression, whose value is the result | `js:({value: payload['t'], 'value:unit': '°C'})` |
| `jsf:` | The body of a function, with `return` | `jsf:if (!payload) return null; return {value: payload.length}` |

A string without prefix is evaluated as an expression where code is expected (`"({value: payload})"`), but the prefix makes the intent clear. An object literal must be wrapped in parentheses in the `js:` form.

Init code of actions and passthroughs (`js:params['port'] = '1'`) is executed as statements: changes to `params` are kept.

## Variables

Variables available depend on where the code runs:

| Where | Variables |
|---|---|
| Publish rule `events`, `payload_transform`, `notify_handler`, `notify_if` | `topic`, `payload`, `matches` |
| Topic rule payload filter `[js: ...]` | `topic`, `payload`, `matches` |
| Event conditions `entry.event(...)` | `params`, `changed_params`, `keys` |
| Action `init`, `topic`, `payload` | `params` |

`payload` is the decoded payload: JSON objects and arrays are already parsed, other payloads are strings.

## Helper functions

Available in every snippet, in both cores:

| Function | Description |
|---|---|
| `now` | Current timestamp, in seconds |
| `d(duration)` | Seconds of a duration string: `d('10m')` is 600 |
| `t(value)` | Timestamp, in seconds, of a date (ISO 8601 string, seconds or milliseconds) |
| `strftime(timestamp, format)` | Formats a timestamp, default format `%Y-%m-%d %H:%M:%S` |
| `round(n, digits)` | Rounds a number |
| `array_sum(a)`, `array_avg(a)`, `array_min(a)`, `array_max(a)` | Aggregates of a list |
| `is_dict(v)`, `is_array(v)` | Type checks |
| `camel_to_snake_case(s)` | `powerFactor` → `power_factor` |
| `payload_transfer(base, payload, keys, empty_result)` | Copies `keys` of `payload` into a copy of `base`: a list of keys, or an object `{ key: new_name }` to rename them (an empty name keeps the key). Returns `empty_result` (default `null`, so no event) if no key is found |
| `uniqid()` | A random id |
| `print(v)` | Logs a value |
| `str(v)` | Converts to string (Python core only) |
| `_(text)` | Translation placeholder |

Nodes can add more variables through module **exports** (for example a module that keeps `is_day` updated).

## Writing portable snippets

The same snippet runs on the Python core (with Js2Py, an ES5 interpreter written in Python, or QuickJS) and in browsers. To stay safe:

- use ES5 syntax: `var`/`let`, `function`, no arrow functions, no template strings, no spread;
- use `'key' in obj` and `obj['key']` rather than newer methods;
- return plain objects, lists, strings and numbers;
- keep expressions short and deterministic: they run for every message on slow nodes too;
- in a `js:` expression both branches of a ternary may be evaluated by Js2Py: use `jsf:` with an `if` when a branch has side effects or is expensive.

The parity test of [automato-core-js](https://github.com/eric-void/automato-core-js) runs the same definitions on both cores: add a case there when relying on a less common construct.

## Engines of the Python core

The `scripting-engine` key of the node configuration selects the engine (or the `AUTOMATO_SCRIPTING_ENGINE` environment variable, when the configuration does not set it). If the selected engine cannot be loaded, the node logs an error and uses the other one.

| Engine | Package | Notes |
|---|---|---|
| `js2py` (default) | [Js2Py](https://github.com/PiotrDabkowski/Js2Py) | Pure Python, ES5. Unmaintained: the PyPI release works up to Python 3.11, a fork is needed up to 3.13, nothing works after |
| `quickjs` | [quickjs-ng](https://pypi.org/project/quickjs-ng/) ([QuickJS](https://github.com/quickjs-ng/quickjs)) | C extension, ES2023, about 3 times faster (on golconda too), much smaller. It needs a wheel for the platform (see the installation docs for OpenWrt) |

Both engines give the same results on the snippets of definitions (the QuickJS engine reproduces Js2Py on purpose: Python `None` is `undefined`, object properties set to `undefined` are kept, integral numbers are `int`). The differences, where Js2Py is the wrong one:

- with Js2Py the Python helpers receive Js2Py values, not Python ones: `d()` always returns `-1`, `strftime()` without a format fails, `array_*()` fail. With QuickJS they work;
- a `null` inside an object or a list (for example `{"battery": null}` in a JSON payload) is `null` with QuickJS, as in browsers, and `undefined` with Js2Py: `== null` works on both, `=== null` and `typeof` do not. A whole variable set to `null` (as an empty `payload`) is `undefined` on both;
- module exports values (for example `is_day`) are updated at every call with QuickJS; Js2Py keeps the values of the last time the set of exports changed;
- QuickJS accepts modern syntax (arrow functions, spread, `?.`, `??`, ...), but browsers and Js2Py may not: keep ES5 while Js2Py nodes exist.

To verify a change on both engines: `AUTOMATO_SCRIPTING_ENGINE=quickjs automato-node-py/bin/unit-tests.sh` (the unit tests of `tests/test_scripting.py` always run both engines, skipping one not installed).
