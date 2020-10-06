# require python3
# -*- coding: utf-8 -*-

import logging
import collections
import datetime
import dateutil.parser
import traceback
import json
import base64
import zlib

def log_stacktrace(message = 'current stacktrace'):
  logging.debug(message + ": " + ''.join(traceback.format_stack()))

# @see https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
# In alternativa, per un merge del solo top_level: v = {**a, **v}
def dict_merge(dct, merge_dct, add_keys=True):
  """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
  updating only top-level keys, dict_merge recurses down into dicts nested
  to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
  ``dct``.

  This version will return a copy of the dictionary and leave the original
  arguments untouched.

  The optional argument ``add_keys``, determines whether keys which are
  present in ``merge_dict`` but not ``dct`` should be included in the
  new dict.

  Args:
    dct (dict) onto which the merge is executed
    merge_dct (dict): dct merged into dct
    add_keys (bool): whether to add new keys

  Returns:
    dict: updated dict
  """
  dct = dct.copy()
  if not add_keys:
    merge_dct = {
      k: merge_dct[k]
      for k in set(dct).intersection(set(merge_dct))
    }

  for k, v in merge_dct.items():
    if (k in dct and isinstance(dct[k], dict)
        and isinstance(merge_dct[k], collections.Mapping)):
      dct[k] = dict_merge(dct[k], merge_dct[k], add_keys=add_keys)
    else:
      dct[k] = merge_dct[k]

  return dct

def strftime(timestamp, tformat = '%Y-%m-%d %H:%M:%S'):
  return datetime.datetime.fromtimestamp(timestamp).strftime(tformat) if timestamp > 0 else '-'

def hour(d):
  if isinstance(d, int) or isinstance(d, float):
    d = datetime.datetime.fromtimestamp(d)
  if isinstance(d, datetime.datetime) or isinstance(d, datetime.time):
    return d.hour + (d.minute + d.second / 60)/60
  return -1

def read_duration(v):
  """
  Read duration strings, like '10m' or '1h' and returns the number of seconds (600 or 3600, for previous examples)
  """
  if not isinstance(v, str) or len(v) < 2 or (v[-1] >= "0" and v[-1] <= "9"):
    try:
      return int(v)
    except:
      return -1
  u = v[-1].upper()
  try:
    v = int(v[0:-1])
  except:
    return -1
  if u == 'S':
    return v
  elif u == 'M':
    return v * 60
  elif u == 'H' or u == 'O':
    return v * 3600
  elif u == "D" or u == "G":
    return v * 86400
  elif u == "W":
    return v * 7 * 86400
  return v

def read_duration_hour(v):
  return read_duration(v) / 3600

def parse_datetime(v, milliseconds_float = False):
  """
  Convert a passed datetime in various format (timestamp, ISO8601, ...) to timestamp (in seconds):
  '1396879090' > 1396879090
  1396879090123 > 1396879090
  "2014-04-07T13:58:10.104Z" > 1396879090
  "2014-04-07T15:58:10.104" > 1396879090
  """
  if not v:
    return 0
  try:
    ret = float(v)
    if ret > 9999999999:
      ret = ret / 1000
  except ValueError:
    try:
      ret = dateutil.parser.parse(v).timestamp()
    except ValueError:
      return 0
  return int(ret) if not milliseconds_float else ret

def json_sorted_encode(data):
  return json.dumps(data if not isinstance(data, dict) else {x:data[x] for x in sorted(data)})

def b64_compress_data(data):
  """
  Compress and base-64 encode data
  """
  try:
    return base64.b64encode(zlib.compress(json.dumps(data).encode('UTF-8'))).decode('UTF-8')
  except:
    logging.exception("utils.b64_compress_data> Error compressing: " + str(data))
    return ""

def b64_decompress_data(string):
  """
  Base 64 decode and decompress data
  """
  try:
    return json.loads(zlib.decompress(base64.b64decode(string.encode('UTF-8'))).decode('UTF-8')) if isinstance(string, str) else string
  except:
    logging.exception("utils.b64_decompress_data> Error decompressing: " + str(string))
    return None
