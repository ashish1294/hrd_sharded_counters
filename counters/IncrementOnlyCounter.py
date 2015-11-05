from google.appengine.ext import ndb
from google.appengine.api import memcache

SHARD_KEY_TEMPLATE = 'in_stock_shard-{0}-{1}'

class IncrementOnlyShard(ndb.Model):
  count = ndb.IntegerProperty(default=0, indexed=False)

class ShardIncrementTransaction(ndb.Model):
  shard_key = ndb.KeyProperty(kind=IncrementOnlyShard)

class IncrementOnlyCounter(ndb.Model):
  num_shards = ndb.IntegerProperty(default=0, indexed=False)
  max_shards = ndb.IntegerProperty(default=20, indexed=False)
  dynamic_growth = ndb.BooleanProperty(default=True, indexed=False)

  def _format_shard_key(self, index):
    """
      Formats the Shard Key Template and returns the key-string for the shard at
      given index
      Args:
        index : Index of the shard whose key-string is required
    """
    return SHARD_KEY_TEMPLATE.format(self.key.id(), index)

  def _get_shard_key(self, index):
    """
      This function returns a particular shard key with the given index
      Args:
        index : Index of the shard whose's key is required
    """
    return ndb.Key(IncrementOnlyShard, self._format_shard_key(index))

  def _get_all_shard_keys(self):
    """
      This function returns all the keys of each shard of this counter
    """
    return [self._get_shard_key(index) for index in range(self.num_shards)]

  def _get_all_shards(self):
    """
      This function returns all the shards associated with this counter
    """
      return ndb.get_multi(self._get_all_shard_keys())

  @property
  def count(self):
    """
      Retrieve the value for a
    """
    memcache_var = str(self.key.id())
    count = memcache.get(memcache_var)
    if count is None:
      shard_list = self._get_shards()
      count = 0
      for shard in shard_list:
        if shard is not None:
          count += shard.count
      memcache.add(memcache_var, count)
    return count

  @ndb.transactional(xg=True)
  def _increment(self, delta, request_id):
    """
      This function increases a random shard by a given quantity. We randomly
      pick any shard counter and add the quantity to it. This function is an
      internal function and should not be called from external application
      directly.
      Args:
        delta : Quantity to be incremented
        request_id : unique request id generated for this operation
    """
    log_key = ndb.Key(ShardIncrementTransaction, request_id)
    if log_key.get() is not None:
      return
    # Re-fetching counter because it might be stale
    counter = self.key.get()
    index = random.randint(0, counter.num_shards - 1)
    shard_key = self._format_shard_key(index)
    shard = IncrementOnlyShard.get_or_insert(shard_key)
    shard.count += delta
    shard.put()
    #Incrementing The value in memcache. No action taken if key is absent
    memcache.incr(str(self.key.id()), delta=quantity)
    ShardIncrementTransaction.get_or_insert(log_key, shard_key=shard_key)

  def increment(self, delta):
    """
      Function that increments a random shard. It generates a unique request id
      for each call
      Args:
        delta : Quantity by which a shard has to be incremented
    """

  @ndb.transactional(retries=0)
