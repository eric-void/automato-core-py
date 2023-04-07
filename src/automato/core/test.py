# require python3
# -*- coding: utf-8 -*-

import logging
import threading
import re

from automato.core import utils
from automato.core import system

node_config = {}
current_unit = None
current_unit_name = None

assertsRunning = {}
assertsDone = {}
checks = []
finished = False
destroyed = False
thread = None

def init(config, unit):
  global node_config, current_unit, current_unit_name, finished, destroyed, thread

  node_config = config
  current_unit = unit
  current_unit_name = unit.__name__

  finished = False
  destroyed = False
  thread = threading.Thread(target = _test_thread, daemon = True) # daemon = True allows the main application to exit even though the thread is running. It will also (therefore) make it possible to use ctrl+c to terminate the application
  thread.start()
  
  current_unit.test_init()

def destroy():
  global destroyed, thread
  destroyed = True
  thread.join()

def add_node_config(config):
  global node_config
  r = utils.dict_merge(node_config, config)
  for k in r:
    node_config[k] = r[k]

def run():
  global finished
  current_unit.test_run(system.entries())
  finished = True

def summary():
  logging.info('TEST SUMMARY:')
  assertsTree = {}
  for unitname in assertsDone:
    if not assertsDone[unitname]['unit'] in assertsTree:
      assertsTree[assertsDone[unitname]['unit']] = { 'ok': 0, 'error': 0, 'tests': []}
    assertsDone[unitname]['_tests_ok'] = []
    assertsDone[unitname]['_tests_error'] = []
    for t in assertsDone[unitname]['data']:
      if assertsDone[unitname]['data'][t][0]:
        assertsDone[unitname]['_tests_ok'].append(t)
      else:
        assertsDone[unitname]['_tests_error'].append(t)
    if not assertsDone[unitname]['_tests_error']:
      assertsDone[unitname]['_message'] = '{name}: {count}/{count} OK'.format(name = assertsDone[unitname]['name'], count = assertsDone[unitname]['count'])
    else:
      assertsDone[unitname]['_message'] = '{name}: {ok}/{count} OK, {error}/{count} ERROR! ({errors})'.format(name = assertsDone[unitname]['name'], count = assertsDone[unitname]['count'], ok = len(assertsDone[unitname]['_tests_ok']), error = len(assertsDone[unitname]['_tests_error']), errors = ", ".join(assertsDone[unitname]['_tests_error']))
    assertsTree[assertsDone[unitname]['unit']]['ok'] += len(assertsDone[unitname]['_tests_ok'])
    assertsTree[assertsDone[unitname]['unit']]['error'] += len(assertsDone[unitname]['_tests_error'])
    assertsTree[assertsDone[unitname]['unit']]['tests'].append(unitname)
    
  for unit in assertsTree:
    if not assertsTree[unit]['error']:
      logging.info('{unit}: {count}/{count} OK'.format(unit = unit, count = assertsTree[unit]['ok']))
    else:
      logging.error('{unit}: {ok}/{count} OK, {error}/{count} ERROR!'.format(unit = unit, count = assertsTree[unit]['ok'] + assertsTree[unit]['error'], ok = assertsTree[unit]['ok'], error = assertsTree[unit]['error']))
    for unitname in assertsTree[unit]['tests']:
      if not assertsDone[unitname]['_tests_error']:
        logging.info('    ' + assertsDone[unitname]['_message'])
      else:
        logging.error('    ' + assertsDone[unitname]['_message'])

def waitRunning():
  global assertsRunning
  while len(assertsRunning) > 0:
    system.sleep(.5)

def isRunning(name):
  unitname = current_unit_name + '.' + name
  return unitname in assertsRunning
  
def waitPublish(topic, payload, qos = 0, retain = False, timeoutms = 2000):
  """
  Publish a topic and wait it's received by node
  """
  global checks
  now = system.timems()
  checks.append({ 'unit': False, 'name': False, 'topic': topic, 'payload': payload, 'maxtime': now + timeoutms })
  system.broker().mqtt_subscribe_pause_on_topic = topic
  system.broker().publish(topic, payload, qos = qos, retain = retain)
  while len(checks) > 0:
    system.sleep(.1)
  system.broker().mqtt_subscribe_pause_on_topic = None

def assertx(name, waitPublish = None, assertSubscribe = None, assertSubscribeSomePayload = None, assertSubscribeNotReceive = None, assertEventsTopic = None, assertEventsData = False, assertEvents = None, assertSomeEvents = None, assertNotEvents = None, assertNotification = None, assertExports = None, assertChild = None, assertNotChild = None, assertEq = None, timeoutms = 2000, wait = True):
  """
  Generic assert
  """
  global assertsRunning, checks
  
  now = system.timems()
  unitname = current_unit_name + '.' + name
  logging.info("\n\nStarting test " + unitname + " ...")

  if waitPublish:
    for t in waitPublish:
      globals()['waitPublish'](t[0], t[1])

  assertsRunning[unitname] = { 'unit': current_unit_name, 'name': name, 'count': 0, 'data': {}, 'maxtime': now + timeoutms}
  if assertEvents:
    assertsRunning[unitname]['count'] += 1
    checks.append({ 'unit': current_unit_name, 'name': name, 'topic': assertEventsTopic, 'events': assertEvents, 'eventsdata': assertEventsData, 'maxtime': assertsRunning[unitname]['maxtime'] })
  elif assertSomeEvents:
    assertsRunning[unitname]['count'] += 1
    checks.append({ 'unit': current_unit_name, 'name': name, 'topic': assertEventsTopic, 'some_events': assertSomeEvents, 'some_events_init': assertSomeEvents, 'eventsdata': assertEventsData, 'maxtime': assertsRunning[unitname]['maxtime'] })
  if assertNotEvents:
    for t in assertNotEvents:
      assertsRunning[unitname]['count'] += 1
      checks.append({ 'unit': current_unit_name, 'name': name, 'topic': assertEventsTopic, 'not_events': assertNotEvents, 'maxtime': assertsRunning[unitname]['maxtime'] })
    
  if assertSubscribe:
    for t in assertSubscribe:
      assertsRunning[unitname]['count'] += 1
      checks.append({ 'unit': current_unit_name, 'name': name, 'topic': t, 'payload': assertSubscribe[t], 'maxtime': assertsRunning[unitname]['maxtime'] })
  if assertSubscribeSomePayload:
    for t in assertSubscribeSomePayload:
      assertsRunning[unitname]['count'] += 1
      checks.append({ 'unit': current_unit_name, 'name': name, 'topic': t, 'some_payload': assertSubscribeSomePayload[t], 'maxtime': assertsRunning[unitname]['maxtime'] })
  if assertSubscribeNotReceive:
    for t in assertSubscribeNotReceive:
      assertsRunning[unitname]['count'] += 1
      checks.append({ 'unit': current_unit_name, 'name': name, 'topic': t, 'not': True, 'maxtime': assertsRunning[unitname]['maxtime'] })
  if assertNotification:
    assertsRunning[unitname]['count'] += 1
    checks.append({ 'unit': current_unit_name, 'name': name, 'notification': assertNotification, 'maxtime': assertsRunning[unitname]['maxtime'] })
  if assertExports:
    for t in assertExports:
      assertsRunning[unitname]['count'] += 1
      checks.append({ 'unit': current_unit_name, 'name': name, 'export': t, 'value': assertExports[t], 'maxtime': assertsRunning[unitname]['maxtime'] })
  if assertChild:
    for c in assertChild:
      assertsRunning[unitname]['count'] += 1
      checks.append({ 'unit': current_unit_name, 'name': name, 'child': c, 'maxtime': assertsRunning[unitname]['maxtime'] })
  if assertNotChild:
    for c in assertNotChild:
      assertsRunning[unitname]['count'] += 1
      checks.append({ 'unit': current_unit_name, 'name': name, 'child': c, 'not': True, 'maxtime': assertsRunning[unitname]['maxtime'] })
  if assertEq:
    for d in assertEq:
      assertsRunning[unitname]['count'] += 1
      if _data_match(d[0], d[1]):
        _assertsRunningDone(unitname, name + '.eq', True)
      else:
        _assertsRunningDone(unitname, name + '.eq', False, 'received: ' + str(d[0]) + ', want: ' + str(d[1]))
  if not assertsRunning[unitname]['count']:
    logging.error("Test {unitname} is invalid because no assert is defined, it will be skipped".format(unitname = unitname))
    assertsRunning[unitname]['data']['INVALID_TEST'] = [False, 'Invalid test, no assert defined']
    assertDone(unitname)
  if len(assertsRunning[unitname]['data']) >= assertsRunning[unitname]['count']:
    assertDone(unitname)
  if wait:
    waitRunning()
  
def assertPublish(name, topic, payload, qos = 0, retain = False, assertEventsTopic = False, assertEventsData = False, assertEvents = None, assertSomeEvents = None, assertNotEvents = None, assertSubscribe = None, assertSubscribeSomePayload = None, assertSubscribeNotReceive = None, assertNotification = None, assertExports = None, assertChild = None, assertNotChild = None, timeoutms = 2000, wait = True):
  """
  Publish a topic and check results (events, topic responses)
  """
  assertx(name, assertEventsTopic = assertEventsTopic if assertEventsTopic != False else topic, assertEventsData = assertEventsData, assertEvents = assertEvents, assertSomeEvents = assertSomeEvents, assertNotEvents = assertNotEvents, assertSubscribe = assertSubscribe, assertSubscribeSomePayload = assertSubscribeSomePayload, assertSubscribeNotReceive = assertSubscribeNotReceive, assertNotification = assertNotification, assertExports = assertExports, assertChild = assertChild, assertNotChild = assertNotChild, timeoutms = timeoutms, wait = False)
  system.broker().publish(topic, payload, qos = qos, retain = retain)
  
  if wait:
    waitRunning()

def assertSubscribe(name, topic, assertPayload = None, assertSomePayload = None, assertNotification = None, assertExports = None, assertChild = None, assertNotChild = None, timeoutms = 2000, wait = True):
  """
  Subscribe to a topic and check if received and with a specificed payload
  """
  assertx(name, assertSubscribe = { topic: assertPayload } if assertPayload != None else None, assertSubscribeSomePayload = { topic: assertSomePayload } if assertSomePayload else None, assertNotification = assertNotification, assertExports = assertExports, assertChild = assertChild, assertNotChild = assertNotChild, timeoutms = timeoutms, wait = False)
  if wait:
    waitRunning()
  
def assertAction(name, entryname, action, params, init = None, if_event_not_match = False, if_event_not_match_keys = False, if_event_not_match_timeout = None, assertSubscribe = None, assertSubscribeSomePayload = None, assertEventsTopic = None, assertEventsData = False, assertEvents = None, assertSomeEvents = None, assertNotEvents = None, assertNotification = None, assertExports = None, assertChild = None, assertNotChild = None, timeoutms = 2000, wait = True):
  """
  Executes an action and check results (published topics)
  """
  global assertsDone
  
  entry = system.entry_get(entryname)
  if not entry:
    assertsDone[unitname] = { 'unit': current_unit_name, 'name': name, 'count': 0, 'data': {'entry_reference': [False, 'entry "' + entryname + '" referenced in assertAction ' + unitname + 'not found']} }
    
  else:
    assertx(name, assertSubscribe = assertSubscribe, assertSubscribeSomePayload = assertSubscribeSomePayload, assertEventsTopic = assertEventsTopic, assertEventsData = assertEventsData, assertEvents = assertEvents, assertSomeEvents = assertSomeEvents, assertNotEvents = assertNotEvents, assertNotification = assertNotification, assertExports = assertExports, assertChild = assertChild, assertNotChild = assertNotChild, timeoutms = timeoutms, wait = False)
    entry.do(action, params, init = init, if_event_not_match = if_event_not_match, if_event_not_match_keys = if_event_not_match_keys, if_event_not_match_timeout = if_event_not_match_timeout)
    if wait:
      waitRunning()

def assertChild(name, assertEq = None, done = True):
  global checks, assertsRunning
  
  delete = []
  for check in checks:
    if 'child' in check and check['child'] == name:
      unitname = check['unit'] + '.' + check['name'] if check['unit'] else False
      if assertEq:
        for d in assertEq:
          assertsRunning[unitname]['count'] += 1
          if _data_match(d[0], d[1]):
            _assertsRunningDone(unitname, name + '.child_eq', True)
          else:
            _assertsRunningDone(unitname, name + '.child_eq', False, 'received: ' + str(d[0]) + ', want: ' + str(d[1]))
      if done:
        if 'not' in check:
          _assertsRunningDone(unitname, name + '.child_not', False, 'received child assert: ' + str(name) + ', but i didn\'t want it')
        else:
          _assertsRunningDone(unitname, name + '.child', True)
        delete.append(check)

  for check in delete:
    checks.remove(check)
    unitname = check['unit'] + '.' + check['name'] if check['unit'] else False
    if unitname and unitname in assertsRunning and len(assertsRunning[unitname]['data']) >= assertsRunning[unitname]['count']:
      assertDone(unitname)

def assertDone(unitname, timeout = False):
  global assertsRunning, assertsDone
  for t in assertsRunning[unitname]['data']:
    if assertsRunning[unitname]['data'][t][0]:
      logging.info('TEST {unitname}.{t}: OK'.format(unitname = unitname, t = t))
    else:
      logging.error('TEST {unitname}.{t}: ERROR '.format(unitname = unitname, t = t) + assertsRunning[unitname]['data'][t][1])
  assertsDone[unitname] = assertsRunning[unitname]
  del assertsRunning[unitname]
  logging.info("\n... test done.\n")

def on_all_mqtt_messages(message):
  global assertsRunning, checks
  eventsdata = message.events()
  events = {e['name']: e['params'] for e in eventsdata}
  
  delete = []
  for check in checks:
    if 'topic' in check and (check['topic'] == message.topic or (check['topic'] == None and ('events' in check or 'some_events' in check))):
      unitname = check['unit'] + '.' + check['name'] if check['unit'] else False
      if not unitname and (check['payload'] == message.payload or (check['payload'] == None and message.payload == "")):
        delete.append(check)
      elif unitname:
        if 'events' in check and check['events']:
          if check['eventsdata']:
            _assertsRunningDone(unitname, 'events', _data_match(eventsdata, check['events']), 'received: ' + str(eventsdata) + ', want: ' + str(check['events']))
          else:
            _assertsRunningDone(unitname, 'events', _data_match(events, check['events']), 'received: ' + str(events) + ', want: ' + str(check['events']))
          delete.append(check)
        elif 'some_events' in check and check['some_events']:
          if check['topic'] != None:
            match = True
            for e in check['some_events']:
              if not e in events or not _data_match(events[e], check['some_events'][e]):
                match = False
                break
            _assertsRunningDone(unitname, 'some_events', match, 'received: ' + str(events) + ', want (some): ' + str(check['some_events']))
            delete.append(check)
          else:
            print(events)
            check['some_events'] = {e: check['some_events'][e] for e in check['some_events'] if not e in events or not _data_match(events[e], check['some_events'][e])}
            if not check['some_events']:
              _assertsRunningDone(unitname, 'some_events', True, 'received: all, want (some): ' + str(check['some_events_init']))
              delete.append(check)
        elif 'not_events' in check and check['not_events']:
          match = False
          for e in check['not_events']:
            if e in events:
              match = True
          if match:
            _assertsRunningDone(unitname, 'not_events', False, 'received event: ' + str(e) + ', but i didn\'t want it')
            delete.append(check)
        elif 'payload' in check:
          _assertsRunningDone(unitname, 'subscribe_payload', _data_match(message.payload, check['payload']), 'received: ' + str(message.payload) + ', want: ' + str(check['payload']))
          delete.append(check)
        elif 'some_payload' in check and check['some_payload']:
          if not isinstance(message.payload, dict):
            _assertsRunningDone(unitname, 'some_subscribe_payload', False, 'received: ' + str(message.payload) + ', want (some): ' + str(check['some_payload']))
            delete.append(check)
          else:
            match = True
            for e in check['some_payload']:
              if not e in message.payload or not _data_match(message.payload[e], check['some_payload'][e]):
                match = False
                break
            _assertsRunningDone(unitname, 'some_subscribe_payload', match, 'received: ' + str(message.payload) + ', want (some): ' + str(check['some_payload']))
            delete.append(check)
        elif 'not' in check and check['not']:
          _assertsRunningDone(unitname, 'not', False, 'received: ' + str(message.payload) + ', but i didn\'t want it')
          delete.append(check)
    elif 'notification' in check and message.firstPublishedMessage() and (len(check['notification']) == 2 or check['notification'][2] == message.topic):
      level = message.firstPublishedMessage().notificationLevelString()
      string = message.firstPublishedMessage().notificationString()
      _assertsRunningDone(unitname, 'notification', _data_match(level, check['notification'][0]) and _data_match(string, check['notification'][1]), 'received: [\'' + level + '\', \'' + string + '\'], want: ' + str(check['notification']))
      delete.append(check)

  for check in delete:
    checks.remove(check)
    unitname = check['unit'] + '.' + check['name'] if check['unit'] else False
    if unitname and unitname in assertsRunning and len(assertsRunning[unitname]['data']) >= assertsRunning[unitname]['count']:
      assertDone(unitname)

def _data_match(value, check):
  if value == check:
    return True
  if isinstance(value, dict) and isinstance(check, dict) and len(value) == len(check):
    for k in value:
      if not k in check or not _data_match(value[k], check[k]):
        return False
    return True
  if isinstance(value, list) and isinstance(check, list) and len(value) == len(check):
    for i in range(len(value)):
      if not _data_match(value[i], check[i]):
        return False
    return True
  if isinstance(check, tuple):
    if len(check) == 0:
      return True
    if check[0] == 'd' and value >= check[1] and value <= check[1] + (check[2] if len(check) > 2 else 1):
      return True
    elif check[0] == 're' and re.match(check[1], value):
      return True
    elif check[0] == '*':
      return True
  return False

def _assertsRunningDone(unitname, event, ok, failureDescr = None):
  global assertsRunning
  k = event
  i = 0
  while k in assertsRunning[unitname]['data']:
    i = i + 1
    k = event + str(i)
  assertsRunning[unitname]['data'][k] = [ok, failureDescr if not ok else None]

def _test_thread():
  global destroyed, assertsRunning, checks
  while not destroyed:
    now = system.timems()

    delete = []
    for check in checks:
      unitname = check['unit'] + '.' + check['name'] if check['unit'] else False
      _delete = False
      if 'topic' in check:
        topic = check['topic']
        if 'maxtime' in check and now > check['maxtime']:
          if not unitname:
            logging.error("waitPublish of " + topic + ": " + str(check['payload']) + " exceed timeout, the process will continue but problems will probably occours")
          elif 'events' in check:
            _assertsRunningDone(unitname, 'events', False, 'received NOTHING, want: ' + str(check['events']))
          elif 'some_events' in check:
            some_events_received = [e for e in check['some_events_init'] if e not in check['some_events']]
            _assertsRunningDone(unitname, 'some_events', False, 'received: ' + ('NOTHING' if not some_events_received else str(some_events_received)) + ', want (some): ' + str(check['some_events_init']))
          elif 'not_events' in check:
            _assertsRunningDone(unitname, 'not_events', True, None)
          elif 'payload' in check:
            _assertsRunningDone(unitname, 'subscribe_payload', False, 'received NOTHING, want: ' + topic + ' = ' + str(check['payload']))
          elif 'some_payload' in check:
            _assertsRunningDone(unitname, 'some_subscribe_payload', False, 'received NOTHING, want (some): ' + topic + ' = ' + str(check['some_payload']))
          elif 'not' in check and check['not']:
            _assertsRunningDone(unitname, 'not', True, None)
          _delete = True
      elif 'notification' in check:
        if 'maxtime' in check and now > check['maxtime']:
          if not unitname:
            logging.error("waitPublish of " + topic + ": " + str(check['payload']) + " exceed timeout, the process will continue but problems will probably occours")
          else:
            _assertsRunningDone(unitname, 'notification', False, 'received NOTHING, want: ' + str(check['notification']))
          _delete = True
      elif 'export' in check:
        if 'maxtime' in check and now > check['maxtime']:
          if 'not' in check:
            _assertsRunningDone(unitname, check['export'] + '.exports_not', True, None)
          else:
            _assertsRunningDone(unitname, check['export'] + '.exports', False, 'exports NOT MATCHES, received: ' + check['export'] + ' = ' + (str(system.exports[check['export']]) if check['export'] in system.exports else 'NULL') + ', want: ' + check['export'] + ' = ' + str(check['value']))
          _delete = True
        elif check['export'] in system.exports and _data_match(system.exports[check['export']], check['value']):
          _assertsRunningDone(unitname, 'exports', True)
          _delete = True
      elif 'child' in check:
        if 'maxtime' in check and now > check['maxtime']:
          if 'not' in check:
            _assertsRunningDone(unitname, check['child'] + '.child_not', True, None)
          else:
            _assertsRunningDone(unitname, check['child'] + '.child', False, 'NOT received child asserts from: ' + check['child'])
          _delete = True

      if _delete:
        delete.append(check)
        if unitname and len(assertsRunning[unitname]['data']) >= assertsRunning[unitname]['count']:
          assertDone(unitname)
        

    for check in delete:
      checks.remove(check)
    
    #for unitname in assertsRunning:
    #  if 'maxtime' in assertsRunning[unitname] and now > assertsRunning[unitname]['maxtime']:
    #    assertDone(unitname, timeout = True)
    
    system.sleep(.5)

