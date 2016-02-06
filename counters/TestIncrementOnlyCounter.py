import unittest
import random
import time
from threading import Event
from threading import Thread
from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util
from google.appengine.api import datastore_errors
from google.appengine.ext import ndb
from google.appengine.api import memcache
from IncrementOnlyCounter import IncrementOnlyCounter as IOC

# Increment Test Constants
INCREMENT_STEPS = 5
INCREMENT_VALUE = 40
RAND_INCREMENT_MAX = 100
NUM_THREADS = 3 # Number of concurrent request
TIME_AT_PEAK_QPS = 2 # seconds
DELAY_BETWEEN_THREADS = 1 # seconds

class TestIncrementOnlyTest(unittest.TestCase):

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

    # Normal Counter
    cls.normal_key = 'normal_counter'
    counter_normal = IOC(max_shards=30, id=cls.normal_key)
    counter_normal.put()

    # Idempotent Counter
    cls.idempotent_key = 'idempotent_counter'
    counter_idempotent = IOC(max_shards=30, id=cls.idempotent_key)
    counter_idempotent.put()

    # Static Counter
    cls.static_key = 'static_counter'
    counter_static = IOC(dynamic_growth=False, num_shards=10,
                         id=cls.static_key)
    counter_static.put()

    # Highly Sharded Counter
    cls.highly_sharded_key = 'highly_sharded_counter'
    counter_highly_sharded = IOC(dynamic_growth=True, max_shards=100,
                                 num_shards=50, id=cls.highly_sharded_key)
    counter_highly_sharded.put()

  def test_invalid_counter(self):
    with self.assertRaises(datastore_errors.BadValueError):
      IOC(num_shards=0, max_shards=2)
    with self.assertRaises(datastore_errors.BadValueError):
      IOC(num_shards=2, max_shards=0)

    self.assertIsNone(IOC.minify('dummy'))
    self.assertIsNone(IOC.increment('dummy'))
    self.assertIsNone(IOC.get('dummy', force_fetch=True))
    self.assertIsNone(IOC.expand_shards('dummy'))

  def test_increment(self):

    # Storing the Post Test Expected Count count of the counter
    normal_val = IOC.get(self.normal_key, force_fetch=True) + INCREMENT_STEPS
    idempotent_val = IOC.get(self.idempotent_key, force_fetch=True)
    idempotent_val += INCREMENT_STEPS
    static_val = IOC.get(self.static_key, force_fetch=True) + INCREMENT_STEPS

    # Testing Unit Increment Functionality
    for dummy in range(INCREMENT_STEPS):
      IOC.increment(self.normal_key)
      IOC.increment(self.idempotent_key, idempotency=True)
      IOC.increment(self.static_key)

    self.assertEqual(IOC.get(self.normal_key, force_fetch=True), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key, force_fetch=True),
                     idempotent_val)
    self.assertEqual(IOC.get(self.static_key, force_fetch=True), static_val)

    #Testing Alias
    self.assertEqual(ndb.Key(IOC, self.static_key).get().value, static_val)

    # Testing the non-unit delta increment functionality
    for dummy in range(INCREMENT_STEPS):
      IOC.increment(self.normal_key, INCREMENT_VALUE)
      IOC.increment(self.idempotent_key, INCREMENT_VALUE, idempotency=True)
      IOC.increment(self.static_key, INCREMENT_VALUE)

    # Updating the Expected Value
    total_increment = INCREMENT_STEPS * INCREMENT_VALUE
    normal_val += total_increment
    idempotent_val += total_increment
    static_val += total_increment

    self.assertEqual(IOC.get(self.normal_key, force_fetch=True), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key, force_fetch=True),
                     idempotent_val)
    self.assertEqual(IOC.get(self.static_key, force_fetch=True), static_val)

    # Testing with few random delta
    for dummy in range(INCREMENT_STEPS):
      delta = random.randint(1, RAND_INCREMENT_MAX)
      normal_val += delta
      IOC.increment(self.normal_key, delta)

      delta = random.randint(1, RAND_INCREMENT_MAX)
      idempotent_val += delta
      IOC.increment(self.idempotent_key, delta, True)

      delta = random.randint(1, RAND_INCREMENT_MAX)
      static_val += delta
      IOC.increment(self.static_key, delta)

    self.assertEqual(IOC.get(self.normal_key, force_fetch=True), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key, force_fetch=True),
                     idempotent_val)
    self.assertEqual(IOC.get(self.static_key, force_fetch=True), static_val)

  def test_minify_shard(self):

    # Storing the post-test expected value of each counter
    normal_val = IOC.get(self.normal_key, force_fetch=True)
    idempotent_val = IOC.get(self.idempotent_key, force_fetch=True)
    static_val = IOC.get(self.static_key, force_fetch=True)

    while IOC.minify_shards(self.normal_key):
      pass
    while IOC.minify_shards(self.idempotent_key):
      pass
    while IOC.minify_shards(self.static_key):
      pass

    self.assertEqual(IOC.get(self.normal_key, force_fetch=True), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key, force_fetch=True),
                     idempotent_val)
    self.assertEqual(IOC.get(self.static_key, force_fetch=True), static_val)

    # At this point all counters have only 1 shard and all have correct count
    # Doing some increments and testing again
    for i in range(1, 11):
      IOC.increment(self.normal_key, i)
      IOC.increment(self.idempotent_key, i, True)
      IOC.increment(self.static_key, i)

    normal_val += 55
    idempotent_val += 55
    static_val += 55

    self.assertEqual(IOC.get(self.normal_key, force_fetch=True), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key, force_fetch=True),
                     idempotent_val)
    self.assertEqual(IOC.get(self.static_key, force_fetch=True), static_val)

    # Expanding Shards to Max-Shards Now
    while IOC.expand_shards(self.normal_key):
      pass
    while IOC.expand_shards(self.idempotent_key):
      pass
    while IOC.expand_shards(self.static_key):
      pass

    self.assertEqual(IOC.get(self.normal_key, force_fetch=True), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key, force_fetch=True),
                     idempotent_val)
    self.assertEqual(IOC.get(self.static_key, force_fetch=True), static_val)

    # At this point all counters have max shards and correct value
    # Doing some increments and testing again
    for i in range(1, 10):
      IOC.increment(self.normal_key, i)
      IOC.increment(self.idempotent_key, i, True)
      IOC.increment(self.static_key, i)

    normal_val += 45
    idempotent_val += 45
    static_val += 45

    self.assertEqual(IOC.get(self.normal_key, force_fetch=True), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key, force_fetch=True),
                     idempotent_val)
    self.assertEqual(IOC.get(self.static_key, force_fetch=True), static_val)

  def test_highly_sharded_counters(self):
    counter_val = IOC.get(self.highly_sharded_key, force_fetch=True)
    for _ in range(INCREMENT_STEPS):
      delta = random.randint(1, RAND_INCREMENT_MAX)
      IOC.increment(self.highly_sharded_key, delta, True)
      counter_val += delta

    self.assertEqual(IOC.get(self.highly_sharded_key, force_fetch=True),
                     counter_val)

    # Minifying Shard with random updates in between
    for _ in range(3):
      IOC.minify_shards(self.highly_sharded_key)
      delta = random.randint(1, RAND_INCREMENT_MAX)
      IOC.increment(self.highly_sharded_key, delta, True)
      counter_val += delta
      self.assertEqual(IOC.get(self.highly_sharded_key, force_fetch=True),
                       counter_val)

    self.assertEqual(IOC.get(self.highly_sharded_key, force_fetch=True),
                     counter_val)

    # Totally minify the remaining
    while IOC.minify_shards(self.highly_sharded_key):
      pass

    self.assertEqual(IOC.get(self.highly_sharded_key, force_fetch=True),
                     counter_val)

    # Expand the shard to full extent
    while IOC.expand_shards(self.highly_sharded_key):
      delta = random.randint(1, RAND_INCREMENT_MAX)
      IOC.incr(self.highly_sharded_key, delta)
      counter_val += delta

    self.assertEqual(IOC.get(self.highly_sharded_key, force_fetch=True),
                     counter_val)

  def test_tx_logs(self):

    static_counter = ndb.Key(IOC, self.static_key).get()
    normal_counter = ndb.Key(IOC, self.normal_key).get()
    self.assertEqual(len(static_counter.get_all_tx_logs()), 0)
    self.assertEqual(len(normal_counter.get_all_tx_logs()), 0)

    IOC.increment(self.idempotent_key, True)
    IOC.increment(self.highly_sharded_key, True)
    idempotent_counter = ndb.Key(IOC, self.idempotent_key).get()
    highly_sharded_counter = ndb.Key(IOC, self.highly_sharded_key).get()
    self.assertGreater(len(idempotent_counter.get_all_tx_logs()), 0)
    self.assertGreater(len(highly_sharded_counter.get_all_tx_logs()), 0)

    idempotent_counter.clear_logs()
    highly_sharded_counter.clear_logs()
    self.assertEqual(len(idempotent_counter.get_all_tx_logs()), 0)
    self.assertEqual(len(highly_sharded_counter.get_all_tx_logs()), 0)

  def threadproc(self, idx, results):
    '''This function is executed by each thread.'''
    no_of_req = 0
    while not self.quitevent.is_set():
      try:
        IOC.increment(self.idempotent_key, idempotency=True)
        no_of_req += 1
      except datastore_errors.TransactionFailedError, err:
        print err
    results[idx] = no_of_req

  def test_concurrenct_increment(self):
    print "Starting Concurrent Test. This will take time ..."
    memcache.flush_all()
    value = IOC.get(self.idempotent_key, force_fetch=True)
    self.quitevent = Event()
    threads = []
    results = [None] * NUM_THREADS
    try:
      for i in range(NUM_THREADS):
        thread = Thread(target=self.threadproc, args=(i, results))
        thread.start()
        threads.append(thread)
        time.sleep(DELAY_BETWEEN_THREADS)
      time.sleep(TIME_AT_PEAK_QPS)
    except: #pylint: disable=W0702
      print "Some Unknown Exception"

    self.quitevent.set()
    for thread in threads:
      thread.join(1.0)
    actual_val = IOC.get(self.idempotent_key, force_fetch=True)
    self.assertEqual(actual_val, sum(results) + value)

  @classmethod
  def tearDownClass(cls):
    cls.testbed.deactivate()
