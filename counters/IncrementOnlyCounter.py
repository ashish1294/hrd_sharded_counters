import random
import uuid
from google.appengine.ext import ndb
from google.appengine.api import memcache
from google.appengine.api import datastore_errors

SHARD_KEY_TEMPLATE = '{1}-{0}-increment_only_shard'
MAX_ENTITIES_PER_TRANSACTION = 25

class IncrementOnlyShard(ndb.Model):
  count = ndb.IntegerProperty(default=0, indexed=False)

class ShardIncrementTransaction(ndb.Model):
  shard_key = ndb.KeyProperty(kind=IncrementOnlyShard)

def validate_counter(prop, value):
  if value < 1:
    raise datastore_errors.BadValueError(
        prop.__class__.__name__ + ' should be >= 1')

class IncrementOnlyCounter(ndb.Model):

  READ_WRITE = 0
  MINIFYING = 1
  RESET = 2

  num_shards = ndb.IntegerProperty(
      default=10,
      indexed=False,
      validator=validate_counter,
      verbose_name='Number of Shards')
  max_shards = ndb.IntegerProperty(
      default=20,
      indexed=False,
      validator=validate_counter,
      verbose_name='Maximum Number of Shards')
  dynamic_growth = ndb.BooleanProperty(default=True, indexed=False)
  state = ndb.IntegerProperty(default=READ_WRITE)

  def __str__(self):
    return "(Num = %d, Max = %d, Dynamic = %r)" % (
        self.num_shards, self.max_shards, self.dynamic_growth)

  def __repr__(self):
    return self.__str__()

  def _format_shard_key(self, index):
    '''
      Formats the Shard Key Template and returns the key-string for the shard at
      given index
      Args:
        index : Index of the shard whose key-string is required
    '''
    return SHARD_KEY_TEMPLATE.format(self.key.id(), index)

  def _get_shard_key(self, index):
    '''
      This function returns a particular shard key with the given index
      Args:
        index : Index of the shard whose's key is required
    '''
    return ndb.Key(IncrementOnlyShard, self._format_shard_key(index))

  def _get_shard_keys(self, start=0, end=-1):
    '''
      This function returns all the keys of each shard of this counter in a
      range [start, end). Defaults to all shards
      Args:
        start : Start index of the shard range (defaults to 0)
        end : End index (last index + 1) of the shard range (defaults to
              num_shards)
      Returns:
        A list of keys in the given range
    '''
    if end == -1:
      end = self.num_shards
    return [self._get_shard_key(index) for index in range(start, end)]

  def _get_shards(self, start=0, end=-1):
    '''
      This function returns all the shards associated with this counter within a
      given range [start, end). Defaults to all shards
      Args:
        start : Start index of the shard range (defaults to 0)
        end : End index (last index + 1) of the shard range (defaults to
              num_shards)
      Returns:
        A list of shards in the given range
    '''
    return ndb.get_multi(self._get_shard_keys(start, end))

  def clear_logs(self, start=0, end=-1):
    '''
      This functions deletes increment tx logs for all shards in the given range
      [start, end). Defaults to all shards
      Args:
        start : Start index of the stard range (defaults to 0)
        end : End  index (last index + 1) of the shard range (default to
              num_shards)
    '''
    shard_keys = self._get_shard_keys(start, end)
    for shard_key in shard_keys:
      log_list = ShardIncrementTransaction.query(
          ShardIncrementTransaction.shard_key == shard_key).fetch()
      for log in log_list:
        log.key.delete()

  def get_all_tx_logs(self, start=0, end=-1):
    '''
      This function fetches all the logs from the shards in the given range
      [start, end). Defaults to all shards
      Args:
        start : Start index of the shard range (defaults to 0)
        end : End Index (last index + 1) of the shard range (defaults to
              num_shards)
      Returns : A list of all transactions
    '''
    log_list = []
    shard_keys = self._get_shard_keys(start, end)
    for shard_key in shard_keys:
      log_list += ShardIncrementTransaction.query(
          ShardIncrementTransaction.shard_key == shard_key).fetch()
    return log_list

  @property
  def count(self):
    '''
      Retrieve the current counter value. Sums up values of all shards.
    '''
    shard_list = self._get_shards()
    count = 0
    for shard in shard_list:
      if shard is not None:
        count += shard.count
    return count

  @classmethod
  def get(cls, name, force_fetch=False, cache_duration=30):
    count = None if force_fetch else memcache.get(name)
    if count is None:
      counter = ndb.Key(cls, name).get()
      if counter is None:
        return None
      count = counter.count
      memcache.add(name, count, cache_duration)
    return count

  @property
  def value(self):
    '''
      This function is just an alias for the count function
    '''
    return self.count

  @classmethod
  @ndb.transactional
  def expand_shards(cls, name):
    '''
      This function doubles the number of current shards associated with this
      counter - provided it doesn't grow more than the max_shards limit.
      Returns:
        It returns if the was expansion. None if no counter is found
    '''
    counter = ndb.Key(cls, name).get()
    if counter is None:
      return None
    new_shards = min(counter.max_shards, counter.num_shards * 2)
    if new_shards > counter.num_shards and counter.dynamic_growth:
      counter.num_shards = new_shards
      counter.put()
      return True
    else:
      return False

  @classmethod
  @ndb.transactional(xg=True)
  def minify_shards(cls, name):
    '''
      Function to minify shards. Since NDB allows 25 entity groups per
      transaction we can only minify 25 shards in a single Tx. Thus we either
      reduce the num shards by half or decrease it by 25. This is sensible
      because we do not expect the number of shards to be more than ~100

      Note: Although this function is not totally idempotent, there is not much
      problem even if the function is executed multiple times
    '''
    counter = ndb.Key(cls, name).get()
    if counter is None:
      return None

    value = min(counter.num_shards / 2, MAX_ENTITIES_PER_TRANSACTION - 1)
    if value is 0:
      return False
    elif value is 1:
      value = 2
    shard_list = counter._get_shards(
        counter.num_shards - value, counter.num_shards)
    total_count = sum(shard.count for shard in shard_list if shard is not None)
    if shard_list[0] is None:
      key_string = counter._format_shard_key(counter.num_shards - value)
      shard_list[0] = IncrementOnlyShard.get_or_insert(key_string,
                                                       count=total_count)
    else:
      shard_list[0].count = total_count
      shard_list[0].put()

    for shard in shard_list[1:]:
      if shard is not None:
        shard.count = 0
        shard.put()

    counter.num_shards = counter.num_shards - value + 1
    counter.put()
    return True

  @classmethod
  @ndb.transactional(xg=True)
  def _increment_normal(cls, name, delta):
    '''
      This function increases a random shard by a given quantity. We randomly
      pick any shard counter and add the quantity to it.
      Note: This is not an idempotent function ! It may be incremented multiple
            times for the same request.
      Args:
        name : name of the counter
        delta : Quantity to be incremented
      Returns: the shard_key string that was incremented
    '''
    # Re-fetching counter because it might be stale
    counter = ndb.Key(IncrementOnlyCounter, name).get()
    if counter is None:
      return None
    index = random.randint(0, counter.num_shards - 1)
    shard_key = counter._format_shard_key(index)
    shard = IncrementOnlyShard.get_or_insert(shard_key)
    shard.count += delta
    shard.put()
    return shard_key

  @classmethod
  @ndb.transactional(xg=True)
  def _increment_idempotent(cls, name, delta, request_id):
    '''
      This function increases a random shard by a given quantity. We randomly
      pick any shard counter and add the quantity to it. This function is an
      internal function and should not be called from external application
      directly. It is an idempotent function - which maintain logs of all
      increment counters and doesn't re-applies them.
      Args:
        name : Name of the counter
        delta : Quantity to be incremented
        request_id : unique request id generated for this operation
    '''
    log_key = ndb.Key(ShardIncrementTransaction, request_id)
    if log_key.get() is not None:
      return
    shard_key_str = cls._increment_normal(name, delta)
    if shard_key_str is None:
      return None

    # Inserting Log for this shard
    trx = ShardIncrementTransaction(
        id=request_id,
        shard_key=ndb.Key(IncrementOnlyShard, shard_key_str))
    trx.put()
    return shard_key_str

  @classmethod
  def increment(cls, name, delta=1, idempotency=False):
    '''
      Function that increments a random shard. It generates a unique request id
      for each call using the uuid model
      Args:
        name : Name of the counter
        delta : Quantity by which a shard has to be incremented (positive)
    '''
    if idempotency is False:
      # Call Normal Version
      return cls._increment_normal(name, delta)

    request_id = str(uuid.uuid4())
    retry = True
    while retry:
      try:
        return cls._increment_idempotent(name, delta, request_id)
      except datastore_errors.TransactionFailedError:
        retry = cls.expand_shards(name)
    raise datastore_errors.TransactionFailedError('Failed')

  # Creating useful aliases for popular function
  incr = increment
  minify = minify_shards
  expand = expand_shards
