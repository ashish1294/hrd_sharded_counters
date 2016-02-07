import unittest
import random
import time
from threading import Event
from threading import Thread
from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import ndb
from google.appengine.api import memcache
from MemcacheCounter import MemcacheCounter as MC

# Increment Test Constants
INCREMENT_STEPS = 10
INCREMENT_VALUE = 86
RAND_INCREMENT_MAX = 100
NUM_THREADS = 10 # Number of concurrent request
TIME_AT_PEAK_QPS = 2 # seconds
DELAY_BETWEEN_THREADS = 1 # seconds

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

    val = MC.get(self.counter_name)
    for _ in range(INCREMENT_STEPS):
      MC.increment(self.counter_name)
    self.assertEqual(val + INCREMENT_STEPS, MC.get(self.counter_name))

    for _ in range(INCREMENT_STEPS):
      MC.decrement(self.counter_name)
    self.assertEqual(val, MC.get(self.counter_name))

    # Bulk Increment / Decrement
    MC.decrement(self.counter_name, INCREMENT_STEPS)
    self.assertEqual(val - INCREMENT_STEPS, MC.get(self.counter_name))

    MC.increment(self.counter_name, 2 * INCREMENT_STEPS)
    self.assertEqual(val + INCREMENT_STEPS, MC.get(self.counter_name))

    # Testing the aliases
    self.assertEqual(val + INCREMENT_STEPS, MC.value(self.counter_name))
    self.assertEqual(val + INCREMENT_STEPS, MC.count(self.counter_name))

    # Incrementing / Decrementing by Random amount
    expected_val = MC.get(self.counter_name)
    for _ in range(INCREMENT_STEPS):
      val = random.randint(-RAND_INCREMENT_MAX, RAND_INCREMENT_MAX)
      MC.increment(self.counter_name, val)
      expected_val += val
      val = random.randint(-RAND_INCREMENT_MAX, RAND_INCREMENT_MAX)
      MC.decrement(self.counter_name, val)
      expected_val -= val
    self.assertEqual(expected_val, MC.get(self.counter_name))

  def test_batch_operation(self):

    expected_val = {}
    for counter_name in self.counter_name_list:
      expected_val[counter_name] = 1
      MC.increment(counter_name)

    # Fetching multiple counters at the same time and checking values
    values = MC.get_multi(self.counter_name_list)
    self.assertDictEqual(expected_val, values)

    # Flushing all but first counter
    for counter_name in self.counter_name_list[1:]:
      MC.put_to_datastore(counter_name, flush=True)
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

    # Setting the counter to a particular value
    expected_val = 1024
    MC.set(self.counter_name, expected_val)
    self.assertEqual(expected_val, MC.get(self.counter_name))

    # Reseting the counter to a particular value
    MC.reset(self.counter_name)
    self.assertEqual(0, MC.get(self.counter_name))

    # Deleting the counter from memcache and checking it's existence
    MC.put_to_datastore(self.counter_name, flush=True)
    self.assertTrue(MC.exist(self.counter_name))

    # Now permanently deleting the counter and checking it's existence
    MC.delete(self.counter_name)
    self.assertFalse(MC.exist(self.counter_name))

  def test_memcache_flush(self):

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

  def threadproc(self, idx, results):
    '''This function is executed by each thread.'''
    no_of_req = 0
    while not self.quitevent.is_set():
      MC.increment(self.counter_name, persist_delay=0)
      MC.put_to_datastore(self.counter_name)
      no_of_req += 1
    results[idx] = no_of_req

  def test_concurrenct_increment(self):
    print "Starting Concurrent Test. This will take time ..."
    value = MC.get(self.counter_name)
    self.quitevent = Event()
    threads = []
    results = [None] * NUM_THREADS
    for i in range(NUM_THREADS):
      thread = Thread(target=self.threadproc, args=(i, results))
      thread.start()
      threads.append(thread)
      time.sleep(DELAY_BETWEEN_THREADS)
    time.sleep(TIME_AT_PEAK_QPS)

    self.quitevent.set()
    for thread in threads:
      thread.join(1.0)
    actual_val = MC.get(self.counter_name)
    self.assertEqual(actual_val, sum(results) + value)

  @classmethod
  def tearDownClass(cls):
    cls.testbed.deactivate()
