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
import hashlib

def log_stacktrace(message = 'current stacktrace'):
  logging.debug(message + ": " + ''.join(traceback.format_stack()))

"""
def dict_merge(dct, merge_dct, add_keys=True):
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
"""

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
  if isinstance(dct, dict) and isinstance(merge_dct, collections.Mapping):
    dct = dct.copy()
    if not add_keys:
      merge_dct = {
        k: merge_dct[k]
        for k in set(dct).intersection(set(merge_dct))
      }

    for k, v in merge_dct.items():
      dct[k] = dict_merge(dct[k] if k in dct else None, merge_dct[k], add_keys=add_keys)

    return dct
  return merge_dct

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

def json_export(v):
  """
  JSON-compliant json dumps: no "NaN" is generated (if NaN is in input, we'll try to convert it to None/null)
  No exception is thrown: if v is invalid, an error will be logged and None is returned
  """
  try:
    return json.dumps(v, allow_nan = False)
  except ValueError as e:
    if str(e) == 'Out of range float values are not JSON compliant':
      logging.exception("utils.json_export> trying to export a value with NaN values, i'll try to convert them to null... (value: " + str(v) + ")")
      v = nan_remove(v)
      try:
        return json.dumps(v, allow_nan = False)
      except ValueError as e:
        logging.exception(e)
    else:
      logging.exception(e)
  return None

def json_import(v):
  """
  JSON-compliant json load: no "NaN" (or other constant) is accepted (if NaN is in input, we'll try to convert it to None/null)
  No exception is thrown: if v is invalid, an error will be logged and None is returned
  """
  try:
    return json.loads(v, parse_constant = lambda x: None)
  except:
    logging.exception()
    return None
  
def nan_remove(v):
  if isinstance(v, list):
    return list(map(nan_remove, v))
  elif isinstance(v, dict):
    for k in v:
      v[k] = nan_remove(v[k])
  elif is_nan(v):
    v = None
  return v

def is_nan(v):
  try:
    return math.isnan(v)
  except:
    return False

def json_sorted_encode(data, recursive = False):
  return json_export(sort_map(data, recursive))

def sort_map(data, recursive = False):
  return data if not isinstance(data, dict) else {x:(sort_map(data[x]) if recursive else data[x]) for x in sorted(data)};

def b64_compress_data(data):
  """
  Compress and base-64 encode data
  """
  try:
    return base64.b64encode(zlib.compress(json_export(data).encode('UTF-8'))).decode('UTF-8')
  except:
    logging.exception("utils.b64_compress_data> Error compressing: " + str(data))
    return ""

def b64_decompress_data(string):
  """
  Base 64 decode and decompress data
  """
  try:
    return json_import(zlib.decompress(base64.b64decode(string.encode('UTF-8'))).decode('UTF-8')) if isinstance(string, str) else string
  except:
    logging.exception("utils.b64_decompress_data> Error decompressing: " + str(string))
    return None

def array_sum(a, decimals = -1):
  res = 0
  c = 0
  for i in a:
    if i is not None:
      res = res + i
      c = c + 1
  return (round(res) if decimals >= 0 else res) if c > 0 else None
  
def array_avg(a, decimals = -1):
  res = 0
  c = 0
  for i in a:
    if i is not None:
      res = res + i
      c = c + 1
  return (round(res / c, decimals) if decimals >= 0 else res / c) if c > 0 else None

def array_min(a):
  res = None
  for i in a:
    if i is not None and (res is None or i < res):
      res = i
  return res

def array_max(a):
  res = None
  for i in a:
    if i is not None and (res is None or i > res):
      res = i
  return res

def md5_hexdigest(string):
  return hashlib.md5(string.encode('utf-8')).hexdigest()

def data_signature(data):
  try:
    return md5_hexdigest(json_sorted_encode(data, True))
  except:
    return None
  
