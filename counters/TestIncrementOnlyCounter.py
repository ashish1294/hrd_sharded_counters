from google.appengine.ext import testbed
from google.appengine.ext import ndb
import IncrementOnlyCounter
import unittest

class TestIncrementOnlyTest(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=0.7)
    self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)
    self.testbed.init_memcache_stub()
    ndb.get_context().set_cache_policy(False)
    ndb.get_context().clear_cache()
    self.counter = IncrementOnlyCounter(num_shards=10, max_shards=15)

  def test_increment(self):
    for i in range(20):
