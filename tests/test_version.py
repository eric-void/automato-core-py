# require python3
# -*- coding: utf-8 -*-

import os
import re
import unittest

from automato.core import version

class VersionTest(unittest.TestCase):
  def test_format(self):
    self.assertRegex(version.VERSION, r'^\d+\.\d+\.\d+$')

  def test_api_level(self):
    self.assertEqual(version.api_level('1.2.3'), '1.2')
    self.assertEqual(version.api_level('1.2'), '1.2')
    self.assertIsNone(version.api_level('1'))
    self.assertIsNone(version.api_level(None))

  def test_api_level_is_the_same_of_the_js_core(self):
    # MAJOR.MINOR is shared with automato-core-js (checked also by its tests); skipped if the sibling checkout is not there (e.g. in the test image)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'automato-core-js', 'src', 'automato', 'core', 'version.js')
    if not os.path.exists(path):
      self.skipTest('automato-core-js not found')
    with open(path) as f:
      m = re.search(r"AUTOMATO_CORE_VERSION\s*=\s*'([^']+)'", f.read())
    self.assertTrue(m)
    self.assertEqual(version.api_level(m.group(1)), version.api_level())

if __name__ == '__main__':
  unittest.main()
