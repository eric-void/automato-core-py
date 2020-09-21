#!/usr/bin/python3
# require python3
# -*- coding: utf-8 -*-

import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
import gettext
import copy

def init_locale(config):
  global _
  # https://docs.python.org/3/library/gettext.html
  localedir = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../locales')
  # el = gettext.translation('base', localedir='locales', fallback=True)
  el = gettext.translation('base', localedir, fallback=True)
  el.install()
  _ = el.gettext

RESET = 0
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(30, 38)
# The background is set with 40 plus the number of the color, and the foreground with 30

def _logging_ansi_color_emit(fn):
  def new(*args):
    if len(args) == 2:
      new_args = (args[0], copy.copy(args[1]))
    else:
      new_args = (args[0], copy.copy(args[1]), args[2:])
    if hasattr(args[0], 'baseFilename'):
      return fn(*args)
    levelno = new_args[1].levelno
    if levelno >= 50: # CRITICAL
      color = '\x1b[' + str(RED) + ';5;7m\n '  # blinking red with black
    elif levelno >= 40: # ERROR
      color = '\x1b[' + str(RED) + 'm'
    elif levelno >= 30: # WARNING
      color = '\x1b[' + str(YELLOW) + 'm'
    elif levelno >= 20: # INFO
      color = '\x1b[' + str(RESET) + 'm'  # green
    elif levelno >= 10: # DEBUG
      color = '\x1b[' + str(CYAN) + 'm'  # pink
    else:
      color = '\x1b[' + str(RESET) + 'm'   # normal
    try:
      new_args[1].msg = color + str(new_args[1].msg) + ' \x1b[' + str(RESET) + 'm'
    except Exception as reason:
      print(reason)  # Do not use log here.
    return fn(*new_args)
  return new

def init_logging(config):
  # TODO Per migliorare logging: https://docs.python.org/3/library/logging.html https://docs.python.org/3/howto/logging-cookbook.html#context-info
  
  if 'color-log' in config and config['color-log'] and not sys.platform.startswith("win") and sys.stderr.isatty():
    logging.StreamHandler.emit = _logging_ansi_color_emit(logging.StreamHandler.emit)
  
  # https://docs.python.org/3/library/logging.html#logrecord-attributes [per capire che logger sta facendo un certo log usare %(pathname)s
  format = '%(asctime)s - ' + (config['name'] if 'name' in config else '%(name)s') + '/%(module)s - %(levelname)s - %(message)s'
  dateformat = '%Y-%m-%d %H:%M:%S'
  
  if not 'console-log' in config or config['console-log']:
    logging.basicConfig(format=format, datefmt=dateformat, level=logging.DEBUG)
    # Alternative: h = logging.StreamHandler(); h.setFormatter(logging.Formatter(format, datefmt=dateformat)); logging.getLogger().addHandler(h)
  else:
    logging.getLogger().setLevel(logging.DEBUG)
    
  if 'log' in config:
    log = os.path.join(config['base_path'], config['log'])
    h = TimedRotatingFileHandler(log, when='midnight')
    #h.suffix = "%Y-%m-%d"
    h.setFormatter(logging.Formatter(format, datefmt=dateformat))
    logging.getLogger().addHandler(h)
  
  if 'error-log' in config:
    log = os.path.join(config['base_path'], config['error-log'])
    h = TimedRotatingFileHandler(log, when='midnight')
    h.setFormatter(logging.Formatter(format, datefmt=dateformat))
    h.setLevel(logging.ERROR)
    logging.getLogger().addHandler(h)
  
  # To support messages-log you must add 'mqtt_config["message-logger"] = "messages"' to mqtt configuration
  if 'messages-log' in config:
    log = os.path.join(config['base_path'], config['messages-log'])
    h = TimedRotatingFileHandler(log, when='midnight')
    h.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt=dateformat))
    l = logging.getLogger('messages')
    l.propagate = False
    l.addHandler(h)
  
  if 'debug-log' in config:
    log = os.path.join(config['base_path'], config['debug-log'])
    h = TimedRotatingFileHandler(log, when='midnight')
    h.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt=dateformat))
    l = logging.getLogger('debug')
    l.propagate = False
    l.addHandler(h)

  sys.excepthook = _log_all_uncaught_exceptions

def _log_all_uncaught_exceptions(exc_type, exc_value, exc_traceback):
  if not issubclass(exc_type, KeyboardInterrupt):
    logging.error('', exc_info=(exc_type, exc_value, exc_traceback))
    
  # To call original hook (but he logs to stderr):
  #sys.__excepthook__(exc_type, exc_value, exc_traceback)
  
  return

