from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import ndb
from IncrementOnlyCounter import IncrementOnlyCounter
import unittest, random, time
from threading import current_thread, Thread, Event

# Increment Test Constants
INCREMENT_STEPS = 10
INCREMENT_VALUE = 86
RAND_INCREMENT_MAX = 100

# Parallel Increment Test Constants
NUM_THREADS = 5
TIME_AT_PEAK_QPS = 20
DELAY_BETWEEN_THREAD_START = 1

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
    cls.counter_normal = IncrementOnlyCounter(max_shards=30)
    cls.counter_normal.put()
    cls.counter_idempotent = IncrementOnlyCounter(idempotency=True,
                                                  max_shards=30)
    cls.counter_idempotent.put()
    cls.counter_static = IncrementOnlyCounter(dynamic_growth=False,
                                              num_shards=10)
    cls.counter_static.put()
    cls.quitevent = Event()

  def test_increment(self):

    # Storing the Post Test Expected Count count of the counter
    normal_val = self.counter_normal.count + INCREMENT_STEPS
    idempotent_val = self.counter_idempotent.count + INCREMENT_STEPS
    static_val = self.counter_static.count + INCREMENT_STEPS

    # Testing Unit Increment Functionality
    for dummy in range(INCREMENT_STEPS):
      self.counter_normal.increment()
      self.counter_idempotent.increment()
      self.counter_static.increment()

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_idempotent.count, idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

    # Testing the non-unit delta increment functionality
    for dummy in range(INCREMENT_STEPS):
      self.counter_normal.increment(INCREMENT_VALUE)
      self.counter_idempotent.increment(INCREMENT_VALUE)
      self.counter_static.increment(INCREMENT_VALUE)

    # Updating the Expected Value
    total_increment = INCREMENT_STEPS * INCREMENT_VALUE
    normal_val += total_increment
    idempotent_val += total_increment
    static_val += total_increment

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_idempotent.count, idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

    # Testing with few random delta
    for dummy in range(INCREMENT_STEPS):
      delta = random.randint(1, RAND_INCREMENT_MAX)
      normal_val += delta
      self.counter_normal.increment(delta)

      delta = random.randint(1, RAND_INCREMENT_MAX)
      idempotent_val += delta
      self.counter_idempotent.increment(delta)

      delta = random.randint(1, RAND_INCREMENT_MAX)
      static_val += delta
      self.counter_static.increment(delta)

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_idempotent.count, idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

  def test_minify_shard(self):

    # Storing the post-test expected value of each counter
    normal_val = self.counter_normal.count
    idempotent_val = self.counter_idempotent.count
    static_val = self.counter_static.count

    while self.counter_normal.num_shards > 1:
      self.counter_normal.minify_shards()

    while self.counter_idempotent.num_shards > 1:
      self.counter_idempotent.minify_shards()

    while self.counter_static.num_shards > 1:
      self.counter_static.minify_shards()

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_idempotent.count, idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

    # At this point all counters have only 1 shard and all have correct count
    # Doing some increments and testing again
    for i in range(1, 11):
      self.counter_normal.increment(i)
      self.counter_idempotent.increment(i)
      self.counter_static.increment(i)

    normal_val += 55
    idempotent_val += 55
    static_val += 55

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_idempotent.count, idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

    # Expanding Shards to Max-Shards Now

    while self.counter_normal.num_shards < self.counter_normal.max_shards:
      self.counter_normal.expand_shards()

    while self.counter_idempotent.num_shards < \
                self.counter_idempotent.max_shards:
      self.counter_idempotent.expand_shards()

    while self.counter_static.num_shards < self.counter_static.max_shards:
      self.counter_static.expand_shards()

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_idempotent.count, idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

    # At this point all counters have max shards and correct value
    # Doing some increments and testing again
    for i in range(1, 10):
      self.counter_normal.increment(i)
      self.counter_idempotent.increment(i)
      self.counter_static.increment(i)

    normal_val += 45
    idempotent_val += 45
    static_val += 45

    self.assertEqual(self.counter_normal.count, normal_val)
    self.assertEqual(self.counter_idempotent.count, idempotent_val)
    self.assertEqual(self.counter_static.count, static_val)

  def idempotent_increment_only_task(self):
    """
      This task is executed by each thread. It only continuously increments the
      idempotent counter.
    """
    # Storing the post-test expected value of each counter
    idempotent_val = self.counter_idempotent.count

    while not self.quitevent.is_set():
      try:
        self.counter_idempotent.increment()
        idempotent_val += 1
        ndb.transaction(lambda: self.table.increment_instock(1))
        self.assertEqual(cart_context(test_user, {})['cart'].cart_total,
                         (no_of_prod - 1) * self.table.price)
        context = cart_context(test_user, params)
        self.assertIn('cart', context)
        cart = context['cart']
        self.assertIsInstance(cart, Cart)
        self.assertEqual(cart.user_key, test_user.key)
        self.assertEqual(len(cart.items), 1)
        self.assertEqual(cart.items[0].product_key, self.table.key)
        expected_total = self.table.price * no_of_prod
        self.assertEqual(cart.cart_total, expected_total)
        time.sleep(1)
      except Product.WriteContentionException:
        print "Too much Contention! Giving Up----------------------------------"
        return
      except Exception:
        print "Exception. Test Failed------------------------------------------"
        self.quitevent.set()
        traceback.print_exc(file=sys.stdout)
    return

  def test_parallel_increment_idempotent(self):
    """
      This test is intended to verify the behavior of the idempotent counter
      when it is bombarded with parallel increment requests.
    """
    # First testing for increment only request
    threads = []
    for _ in range(self.NUM_THREADS):
      thread = Thread(target=self.thread_task_increment)
      thread.start()
      threads.append(thread)
      time.sleep(self.DELAY_BETWEEN_THREAD_START)
    time.sleep(self.TIME_AT_PEAK_QPS)
    self.quitevent.set()
    for thread in threads:
      thread.join(1.0)

  @classmethod
  def tearDownClass(cls):
    cls.testbed.deactivate()

if __name__ == "__main__":
  unittest.main()
