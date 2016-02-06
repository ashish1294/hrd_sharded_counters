import unittest
import random
from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util
from google.appengine.api import datastore_errors
from google.appengine.ext import ndb
from IncrementOnlyCounter import IncrementOnlyCounter as IOC

# Increment Test Constants
INCREMENT_STEPS = 5
INCREMENT_VALUE = 40
RAND_INCREMENT_MAX = 100

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
    counter_idempotent = IOC(idempotency=True, max_shards=30,
                             id=cls.idempotent_key)
    counter_idempotent.put()

    # Static Counter
    cls.static_key = 'static_counter'
    counter_static = IOC(dynamic_growth=False, num_shards=10,
                         id=cls.static_key)
    counter_static.put()

    # Highly Sharded Counter
    cls.highly_sharded_key = 'highly_sharded_counter'
    counter_highly_sharded = IOC(dynamic_growth=True, max_shards=500,
                                 num_shards=200, idempotency=True,
                                 id=cls.highly_sharded_key)
    counter_highly_sharded.put()

  def test_invalid_counter(self):
    with self.assertRaises(datastore_errors.BadValueError):
      IOC(idempotency=False, num_shards=0, max_shards=2)
    with self.assertRaises(datastore_errors.BadValueError):
      IOC(num_shards=2, max_shards=0)

    self.assertIsNone(IOC.minify('dummy'))
    self.assertIsNone(IOC.increment('dummy'))
    self.assertIsNone(IOC.get('dummy'))

  def test_increment(self):

    # Storing the Post Test Expected Count count of the counter
    normal_val = IOC.get(self.normal_key) + INCREMENT_STEPS
    idempotent_val = IOC.get(self.idempotent_key) + INCREMENT_STEPS
    static_val = IOC.get(self.static_key) + INCREMENT_STEPS

    # Testing Unit Increment Functionality
    for dummy in range(INCREMENT_STEPS):
      IOC.increment(self.normal_key)
      IOC.increment(self.idempotent_key)
      IOC.increment(self.static_key)

    self.assertEqual(IOC.get(self.normal_key), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key), idempotent_val)
    self.assertEqual(IOC.get(self.static_key), static_val)

    #Testing Alias
    self.assertEqual(ndb.Key(IOC, self.static_key).get().value, static_val)

    # Testing the non-unit delta increment functionality
    for dummy in range(INCREMENT_STEPS):
      IOC.increment(self.normal_key, INCREMENT_VALUE)
      IOC.increment(self.idempotent_key, INCREMENT_VALUE)
      IOC.increment(self.static_key, INCREMENT_VALUE)

    # Updating the Expected Value
    total_increment = INCREMENT_STEPS * INCREMENT_VALUE
    normal_val += total_increment
    idempotent_val += total_increment
    static_val += total_increment

    self.assertEqual(IOC.get(self.normal_key), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key), idempotent_val)
    self.assertEqual(IOC.get(self.static_key), static_val)

    # Testing with few random delta
    for dummy in range(INCREMENT_STEPS):
      delta = random.randint(1, RAND_INCREMENT_MAX)
      normal_val += delta
      IOC.increment(self.normal_key, delta)

      delta = random.randint(1, RAND_INCREMENT_MAX)
      idempotent_val += delta
      IOC.increment(self.idempotent_key, delta)

      delta = random.randint(1, RAND_INCREMENT_MAX)
      static_val += delta
      IOC.increment(self.static_key, delta)

    self.assertEqual(IOC.get(self.normal_key), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key), idempotent_val)
    self.assertEqual(IOC.get(self.static_key), static_val)

  def test_minify_shard(self):

    # Storing the post-test expected value of each counter
    normal_val = IOC.get(self.normal_key)
    idempotent_val = IOC.get(self.idempotent_key)
    static_val = IOC.get(self.static_key)

    while ndb.Key(IOC, self.normal_key).get().num_shards > 1:
      IOC.minify_shards(self.normal_key)

    while ndb.Key(IOC, self.idempotent_key).get().num_shards > 1:
      IOC.minify_shards(self.idempotent_key)

    while ndb.Key(IOC, self.static_key).get().num_shards > 1:
      IOC.minify_shards(self.static_key)

    self.assertEqual(IOC.get(self.normal_key), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key), idempotent_val)
    self.assertEqual(IOC.get(self.static_key), static_val)

    # At this point all counters have only 1 shard and all have correct count
    # Doing some increments and testing again
    for i in range(1, 11):
      IOC.increment(self.normal_key, i)
      IOC.increment(self.idempotent_key, i)
      IOC.increment(self.static_key, i)

    normal_val += 55
    idempotent_val += 55
    static_val += 55

    self.assertEqual(IOC.get(self.normal_key), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key), idempotent_val)
    self.assertEqual(IOC.get(self.static_key), static_val)

    # Expanding Shards to Max-Shards Now

    normal_counter = ndb.Key(IOC, self.normal_key).get()
    while normal_counter.can_expand():
      normal_counter.expand_shards()

    idempotent_counter = ndb.Key(IOC, self.idempotent_key).get()
    while idempotent_counter.can_expand():
      idempotent_counter.expand_shards()

    static_counter = ndb.Key(IOC, self.static_key).get()
    while static_counter.can_expand():
      static_counter.expand_shards()

    self.assertEqual(IOC.get(self.normal_key), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key), idempotent_val)
    self.assertEqual(IOC.get(self.static_key), static_val)

    # At this point all counters have max shards and correct value
    # Doing some increments and testing again
    for i in range(1, 10):
      IOC.increment(self.normal_key, i)
      IOC.increment(self.idempotent_key, i)
      IOC.increment(self.static_key, i)

    normal_val += 45
    idempotent_val += 45
    static_val += 45

    self.assertEqual(IOC.get(self.normal_key), normal_val)
    self.assertEqual(IOC.get(self.idempotent_key), idempotent_val)
    self.assertEqual(IOC.get(self.static_key), static_val)

  def test_highly_sharded_counters(self):

    counter_val = 0
    for _ in range(INCREMENT_STEPS * 15):
      delta = random.randint(1, RAND_INCREMENT_MAX)
      IOC.increment(self.highly_sharded_key, delta)
      counter_val += delta

    self.assertEqual(IOC.get(self.highly_sharded_key), counter_val)

    # Minifying Shard with random updates in between
    for _ in range(3):
      IOC.minify_shards(self.highly_sharded_key)
      delta = random.randint(1, RAND_INCREMENT_MAX)
      IOC.increment(self.highly_sharded_key, delta)
      counter_val += delta

    self.assertEqual(IOC.get(self.highly_sharded_key), counter_val)

    # Totally minify the remaining
    while ndb.Key(IOC, self.highly_sharded_key).get().num_shards > 1:
      IOC.minify_shards(self.highly_sharded_key)

    self.assertEqual(IOC.get(self.highly_sharded_key), counter_val)

    # Expand the shard to full extent
    while ndb.Key(IOC, self.highly_sharded_key).get().can_expand():
      ndb.Key(IOC, self.highly_sharded_key).get().expand_shards()
      delta = random.randint(1, RAND_INCREMENT_MAX)
      IOC.incr(self.highly_sharded_key, delta)
      counter_val += delta

    self.assertEqual(IOC.get(self.highly_sharded_key), counter_val)

  def test_tx_logs(self):

    static_counter = ndb.Key(IOC, self.static_key).get()
    normal_counter = ndb.Key(IOC, self.normal_key).get()
    self.assertEqual(len(static_counter.get_all_tx_logs()), 0)
    self.assertEqual(len(normal_counter.get_all_tx_logs()), 0)

    IOC.increment(self.idempotent_key)
    IOC.increment(self.highly_sharded_key)
    idempotent_counter = ndb.Key(IOC, self.idempotent_key).get()
    highly_sharded_counter = ndb.Key(IOC, self.highly_sharded_key).get()
    self.assertGreater(len(idempotent_counter.get_all_tx_logs()), 0)
    self.assertGreater(len(highly_sharded_counter.get_all_tx_logs()), 0)

    idempotent_counter.clear_logs()
    highly_sharded_counter.clear_logs()
    self.assertEqual(len(idempotent_counter.get_all_tx_logs()), 0)
    self.assertEqual(len(highly_sharded_counter.get_all_tx_logs()), 0)

  @classmethod
  def tearDownClass(cls):
    cls.testbed.deactivate()
