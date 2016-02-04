import unittest
import random
from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import ndb
from google.appengine.api import memcache
from MemcacheCounter import MemcacheCounter as MC

# Increment Test Constants
INCREMENT_STEPS = 10
INCREMENT_VALUE = 86
RAND_INCREMENT_MAX = 100

class TestMemcacheCounter(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    # Configuring AppEngine Testbed
    cls.testbed = testbed.Testbed()
    cls.testbed.activate()
    cls.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=1)
    cls.testbed.init_datastore_v3_stub(consistency_policy=cls.policy)
    cls.testbed.init_memcache_stub()

    # Setting Ndb Cache
    ndb.get_context().set_cache_policy(False)
    ndb.get_context().clear_cache()

    # Flushing Memcache
    memcache.flush_all()

    # Creating Creating test objects
    cls.counter_name = 'test-counter'
    cls.counter_name_list = ['test-1', 'test-2', 'test-3']

  def test_increment_decrement(self):

    expected_val = MC.get(self.counter_name) + INCREMENT_STEPS
    new_val = MC.increment(self.counter_name, INCREMENT_STEPS)

    for _ in range(INCREMENT_STEPS):
      MC.increment(self.counter_name)
    self.assertEqual(expected_val, new_val)

    for _ in range(INCREMENT_STEPS):
      MC.decrement(self.counter_name)
    self.assertEqual(0, MC.get(self.counter_name))

    # Bulk Increment / Decrement
    MC.decrement(self.counter_name, INCREMENT_STEPS)
    self.assertEqual(-INCREMENT_STEPS, MC.get(self.counter_name))

    MC.increment(self.counter_name, 2 * INCREMENT_STEPS)
    self.assertEqual(INCREMENT_STEPS, MC.get(self.counter_name))

    # Testing the aliases
    self.assertEqual(INCREMENT_STEPS, MC.value(self.counter_name))
    self.assertEqual(INCREMENT_STEPS, MC.count(self.counter_name))

    # Incrementing / Decrementing by Random amount
    expected_val = MC.get(self.counter_name)
    for _ in range(INCREMENT_STEPS):
      val = random.randint(-RAND_INCREMENT_MAX, RAND_INCREMENT_MAX)
      MC.increment(self.counter_name, val)
      expected_val += val
      val = random.randint(-RAND_INCREMENT_MAX, RAND_INCREMENT_MAX)
      MC.decrement(-RAND_INCREMENT_MAX, RAND_INCREMENT_MAX)
      expected_val -= val
    self.assertEqual(expected_val, MC.get(self.counter_name))

  def test_batch_operation(self):

    expected_val = {}
    for counter_name in self.counter_name_list:
      expected_val[counter_name] = 1
      MC.increment(counter_name)

    values = MC.get_multi(self.counter_name_list)
    self.assertDictEqual(expected_val, values)

    # Check if all counters exist
    for counter_name in self.counter_name_list:
      self.assertTrue(MC.exist(counter_name))

    # Delete all counters and check if they still exist
    MC.delete_multi(self.counter_name_list)
    for counter_name in self.counter_name_list:
      self.assertFalse(MC.exist(counter_name))

  def test_set_reset(self):

    expected_val = 1024
    MC.set(self.counter_name, expected_val)
    self.assertEqual(expected_val, MC.get(self.counter_name))

    MC.reset(self.counter_name)
    self.assertEqual(0, MC.get(self.counter_name))

  def check_memcache_flush(self):

    # Attempt to raise error by flushing memcache without persisting
    expected_val = MC.get(self.counter_name) + (INCREMENT_STEPS * 2)
    for _ in range(INCREMENT_STEPS):
      MC.increment(self.counter_name, 2, persist_delay=1000)
    self.assertEqual(expected_val, MC.get(self.counter_name))

    memcache.flush_all()
    self.assertNotEqual(expected_val, MC.get(self.counter_name))

    # Same Operation with Force persistence
    expected_val = MC.get(self.counter_name) + (INCREMENT_STEPS * 2)
    for _ in range(INCREMENT_STEPS):
      MC.increment(self.counter_name, 2, persist_delay=1000)
    self.assertEqual(expected_val, MC.get(self.counter_name))
    MC.put_to_datastore(self.counter_name)

    memcache.flush_all()
    self.assertEqual(expected_val, MC.get(self.counter_name))
