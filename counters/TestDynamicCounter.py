import unittest
import random
from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import ndb
from DynamicCounter import DynamicCounter as DC

INCREMENT_STEPS = 5
INCREMENT_VALUE = 40
RAND_INCREMENT_MAX = 100

class TestDynamicCounter(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.testbed = testbed.Testbed()
    cls.testbed.activate()
    cls.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=1)
    cls.testbed.init_datastore_v3_stub(consistency_policy=cls.policy)
    cls.testbed.init_memcache_stub()
    ndb.get_context().set_cache_policy(False)
    ndb.get_context().clear_cache()

  def test_exist(self):
    self.assertFalse(DC.exist('test-counter'))
    DC.increment('test-counter')
    print "ss = ", DC.minify('test-counter')
    print "ss = ", DC.minify('test-counter')
    self.assertTrue(DC.exist('test-counter'))
    DC.delete('test-counter')
    self.assertFalse(DC.exist('test-counter'))

  def test_increment(self):
    val = DC.get_value('incr-count')

    # Single Increment Steps
    for dummy in range(INCREMENT_STEPS):
      DC.increment('incr-count')
    val += INCREMENT_STEPS
    DC.minify('incr-count')
    self.assertEquals(DC.get_value('incr-count'), val)

    # Multiple Increment Steps
    for dummy in range(INCREMENT_STEPS):
      DC.increment('incr-count', INCREMENT_VALUE)
    val += INCREMENT_STEPS * INCREMENT_VALUE
    DC.minify('incr-count')
    self.assertEquals(DC.get_value('incr-count'), val)

    # Random Increment Steps
    for dummy in range(INCREMENT_STEPS):
      delta = random.randint(1, RAND_INCREMENT_MAX)
      val += delta
      DC.increment('incr-count', delta)
    DC.minify('incr-count')
    self.assertEquals(DC.get_value('incr-count'), val)

    DC.decrement('incr-count', val + 10)
    DC.minify('incr-count')
    self.assertEquals(DC.get_value('incr-count'), -10)

  def test_set(self):
    val = DC.get_value('set-count')
    val += 10
    DC.set('set-count', val)
    self.assertEquals(DC.get_value('set-count'), val)
