from google.appengine.ext import ndb
from google.appengine.api import memcache

SHARD_KEY_TEMPLATE = '<increment_only></increment_only>_shard-{0}-{1}'
MAX_ENTITIES_PER_TRANSACTION = 25

class IncrementOnlyShard(ndb.Model):
  count = ndb.IntegerProperty(default=0, indexed=False)

  def remove_tx_log(self):
    log_list = ShardIncrementTransaction.query(shard_key=self.key())
    for log in log_list.iter():
      log.delete()

class ShardIncrementTransaction(ndb.Model):
  shard_key = ndb.KeyProperty(kind=IncrementOnlyShard)

def validate_counter(prop, value):
  print "Validating Model", prop._name
  if prop._name == 'max_shards' or prop._name == 'num_shards':
    if value < 1:
      raise datastore_errors.BadValueError(prop._name)

class IncrementOnlyCounter(ndb.Model):
  num_shards = ndb.IntegerProperty(default=1,
    indexed=False,
    validator=validate_counter)
  max_shards = ndb.IntegerProperty(default=20,
    indexed=False,
    validator=validate_counter)
  dynamic_growth = ndb.BooleanProperty(default=True, indexed=False)
  idempotency = ndb.BooleanProperty(deafault=False, indexed=False)

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

  def _get_shard_keys(self, start = 0, end = self.num_shards):
    """
      This function returns all the keys of each shard of this counter in a
      range [start, end). Defaults to all shards
      Args:
        start : Start index of the shard range (defaults to 0)
        end : End index (last index + 1) of the shard range (defaults to
              num_shards)
      Returns:
        A list of keys in the given range
    """
    return [self._get_shard_key(index) for index in range(start, end)]

  def _get_shards(self, start = 0, end = self.num_shards):
    """
      This function returns all the shards associated with this counter within a
      given range [start, end). Defaults to all shards
      Args:
        start : Start index of the shard range (defaults to 0)
        end : End index (last index + 1) of the shard range (defaults to
              num_shards)
      Returns:
        A list of shards in the given range
    """
      return ndb.get_multi(self._get_all_shard_keys(start, end))

  def clear_logs(self, start=0, end=self.num_shards):
    """
      This functions deletes increment tx logs for all shards in the given range
      [start, end). Defaults to all shards
      Args:
        start : Start index of the stard range (defaults to 0)
        end : End  index (last index + 1) of the shard range (default to
              num_shards)
    """
    for i in range(start, end):
      self._get_shard_key(i).get().remove_tx_log()

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

  def reset_counter(self, num_shards)

  @ndb.transactional
  def expand_shards(self):
    """
      This function doubles the number of current shards associated with this
      counter - provided it doesn't grow more than the max_shards limit.
      Returns:
        It returns if there is further space for expansion
    """
    counter = self.key.get()
    counter.num_shards = min(counter.max_shards, counter.num_shards * 2)
    counter.put()
    return counter.num_shards < counter.max_shards

  @ndb.transactional(xg = True)
  def minify_shards(self):
    """
      Function to minify shards. Since NDB allows 25 entity groups per
      transaction we can only minify 25 shards in a single Tx. Thus we either
      reduce the num shards by half or decrease it by 25. This is sensible
      because we do not expect the number of shards to be more than ~100

      Note: Although this function is not totally idempotent, there is not much
      problem even if the function is executed multiple times
    """
    counter = self.key.get()

    value = counter.num_shards / 2
    value = min(value, MAX_ENTITIES_PER_TRANSACTION - 1)
    if value != 0:
      shard_list = counter._get_shards(
                    counter.num_shards - value, counter.num_shards)
      total_count = 0

      if shard_list[0] is None:
        key_string = self._format_shard_key(counter.num_shards - value)
        shard_list[0] = IncrementOnlyShard.get_or_insert(key_string)

      for shard in shard_list:
        if shard is not None:
          total_count += shard.count

      shard_list[0].count = total_count
      shard_list[0].put()

      for shard in shard_list[1:]:
        if shard is not None:
          shard.count = 0
          shard.put()

      counter.num_shards -= value
      counter.put()

  @ndb.transactional
  def _increment_shard(self, delta):
    # Re-fetching counter because it might be stale
    counter = self.key.get()
    index = random.randint(0, counter.num_shards - 1)
    shard_key = self._format_shard_key(index)
    shard = IncrementOnlyShard.get_or_insert(shard_key)
    shard.count += delta
    shard.put()

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


    # Inserting Log for this shard
    ShardIncrementTransaction.get_or_insert(log_key, shard_key=shard_key)

    # Incrementing The value in memcache. No action taken if key is absent
    memcache.incr(str(self.key.id()), delta=quantity)

  def increment(self, delta):
    """
      Function that increments a random shard. It generates a unique request id
      for each call using the uuid model
      Args:
        delta : Quantity by which a shard has to be incremented (positive)
    """
    request_id = str(uuid.uuid4())
    success = False
    shard_growth = self.dynamic_growth
    retry = True
    while retry:
      try:
        self._increment(delta, request_id)
        success = True
        retry = False
      except datastore_errors.TransactionFailedError:
        if shard_growth:
          # Contention Detected. Increasing number of shards here and then
          # retrying transaction.
          shard_growth = self.expand_shards()
        else:
          # If the shard is already max stop retrying and give up. There is too
          # much contention - beyond what we can handle
          retry = False
