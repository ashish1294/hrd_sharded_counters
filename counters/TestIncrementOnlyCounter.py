from google.appengine.ext import testbed
from google.appengine.ext import ndb
import IncrementOnlyCounter
import unittest, random

class TestIncrementOnlyTest(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.testbed = testbed.Testbed()
    cls.testbed.activate()
    cls.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=1)
    cls.testbed.init_datastore_v3_stub(consistency_policy=self.policy)
    cls.testbed.init_memcache_stub()
    ndb.get_context().set_cache_policy(False)
    ndb.get_context().clear_cache()
    cls.counter_normal = IncrementOnlyCounter(max_shards=30)
    cls.counter_non_idempotent = IncrementOnlyCounter(idempotency=True,
      max_shards=30)
    cls.counter_static = IncrementOnlyCounter(dynamic_growth=False,
      num_shards=10)

  def test_increment(self):
    print "Testing the Increment Functionality"

    INCREMENT_STEPS = 100
    INCREMENT_VALUE = 86
    RAND_INCREMENT_MAX = 100

    # Storing the Post Test Expected Count count of the counter
    normal_val = self.counter_normal.count + INCREMENT_STEPS
    non_idempotent_val = self.counter_non_idempotent.count + INCREMENT_STEPS
    static_val = self.counter_static.count + INCREMENT_STEPS

    # Testing Unit Increment Functionality
    for i in range(INCREMENT_STEPS):
      self.counter_normal.increment()
      self.counter_non_idempotent.increment()
      self.counter_static.increment()

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_non_idempotent.count, non_idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

    # Testing the non-unit delta increment functionality
    for i in range(INCREMENT_STEPS):
      self.counter_normal.increment(INCREMENT_VALUE)
      self.counter_non_idempotent.increment(INCREMENT_VALUE)
      self.counter_static.increment(INCREMENT_VALUE)

    # Updating the Expected Value
    total_increment = INCREMENT_STEPS * INCREMENT_VALUE
    normal_val += total_increment
    non_idempotent_val += total_increment
    static_val += total_increment

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_non_idempotent.count, non_idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

    # Testing with few random delta
    for i in range(INCREMENT_STEPS):
      delta = random.randint(1, RAND_INCREMENT_MAX)
      normal_val += delta
      self.counter_normal.increment(delta)

      delta = random.randint(1, RAND_INCREMENT_MAX)
      non_idempotent_val += delta
      self.counter_non_idempotent.increment(delta)

      delta = random.randint(1, RAND_INCREMENT_MAX)
      static_val += delta
      self.counter_static.increment(delta)

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_non_idempotent.count, non_idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

  def test_expand_minify_shard(self):
    print "Testing the Shard Minifying and Expansion Functionality"

    # Storing the post-test expected value of each counter

    while self.counter_normal.num_shards > 1:
      self.counter_normal.minify_shards()

    while self.coun

  @classmethod
  def tearDownClass(cls):
    cls.testbed.deactivate()

if __name__ == "__main__":
  unittest.main()
