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
  idempotency = ndb.BooleanProperty(default=False, indexed=False)
  state = ndb.IntegerProperty(default=READ_WRITE)

  def __str__(self):
    return "(Num = %d, Max = %d, Dynamic = %r, Idempotency = %r)" % (
        self.num_shards, self.max_shards, self.dynamic_growth, self.idempotency)

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
  def get(cls, name):
    count = memcache.get(name)
    if count is None:
      counter = ndb.Key(cls, name).get()
      if counter is None:
        return None
      count = counter.count
      memcache.add(name, count)
    return count

  @property
  def value(self):
    '''
      This function is just an alias for the count function
    '''
    return self.count

  @ndb.transactional
  def expand_shards(self):
    '''
      This function doubles the number of current shards associated with this
      counter - provided it doesn't grow more than the max_shards limit.
      Returns:
        It returns if there is further space for expansion
    '''
    counter = self.key.get()
    counter.num_shards = min(counter.max_shards, counter.num_shards * 2)
    counter.put()

    #Updating the local copy of object
    self.num_shards = counter.num_shards
    return counter.num_shards < counter.max_shards

  def can_expand(self):
    '''
      This function returns if the counter can be further expanded
    '''
    return self.num_shards < self.max_shards

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

    value = counter.num_shards / 2
    value = min(value, MAX_ENTITIES_PER_TRANSACTION - 1)

    if value != 0:
      shard_list = counter._get_shards(
          counter.num_shards - value, counter.num_shards)
      total_count = 0

      if shard_list[0] is None:
        key_string = counter._format_shard_key(counter.num_shards - value)
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

    return True

  def _increment(self, delta):
    '''
      This function increases a random shard by a given quantity. We randomly
      pick any shard counter and add the quantity to it.
      Note: This is not an idempotent function ! It may be incremented multiple
            times for the same request.
      Args:
        delta : Quantity to be incremented

      Returns: the shard_key string that was incremented
    '''
    # Re-fetching counter because it might be stale
    index = random.randint(0, self.num_shards - 1)
    shard_key = self._format_shard_key(index)
    shard = IncrementOnlyShard.get_or_insert(shard_key)
    shard.count += delta
    shard.put()

    memcache.incr(self.key.id(), delta)
    return shard_key

  @ndb.transactional(xg=True)
  def _increment_idempotent(self, delta, request_id):
    '''
      This function increases a random shard by a given quantity. We randomly
      pick any shard counter and add the quantity to it. This function is an
      internal function and should not be called from external application
      directly. It is an idempotent function - which maintain logs of all
      increment counters and doesn't re-applies them.
      Args:
        delta : Quantity to be incremented
        request_id : unique request id generated for this operation
    '''
    log_key = ndb.Key(ShardIncrementTransaction, request_id)
    if log_key.get() is not None:
      return
    shard_key_str = self._increment(delta)

    # Inserting Log for this shard
    trx = ShardIncrementTransaction.get_or_insert(request_id)
    trx.shard_key = ndb.Key(IncrementOnlyShard, shard_key_str)
    trx.put()

  @classmethod
  @ndb.transactional(xg=True)
  def increment(cls, name, delta=1):
    '''
      Function that increments a random shard. It generates a unique request id
      for each call using the uuid model
      Args:
        delta : Quantity by which a shard has to be incremented (positive)
    '''
    counter = ndb.Key(IncrementOnlyCounter, name).get()
    if counter is None:
      return None

    if counter.idempotency is False:
      # Call Normal Version
      counter._increment(delta)
      return

    request_id = str(uuid.uuid4())
    success = False
    shard_growth = counter.dynamic_growth and counter.can_expand()
    retry = True
    while retry:
      try:
        counter._increment_idempotent(delta, request_id)
        success = True
        retry = False
      except datastore_errors.TransactionFailedError:
        if shard_growth:
          # Contention Detected. Increasing number of shards here and then
          # retrying transaction.
          shard_growth = counter.expand_shards()
        else:
          # If the shard is already max stop retrying and give up. There is too
          # much contention - beyond what we can handle
          retry = False
    if not success:
      raise datastore_errors.TransactionFailedError("Unable to increment \
        counter even after max expansion")

  # Creating useful aliases for popular function
  incr = increment
  minify = minify_shards
  expand = expand_shards
