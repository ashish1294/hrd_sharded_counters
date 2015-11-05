from google.appengine.ext import ndb

SHARD_KEY_TEMPLATE = 'in_stock_shard-{0}-{1}'
MAX_ENTITIES_PER_TRANSACTION = 25

class IncrementOnlyShard(ndb.Model):
  count = ndb.IntegerProperty(default=0)

class IncrementOnlyCounter(ndb.Model):
  num_shards = ndb.IntegerProperty(default=0)
  max_shards = ndb.IntegerProperty(default=20)
  dynamic_growth = ndb.BooleanProperty(default=True)
  auto_shrink = ndb.IntegerProperty(default=1000)
  name = ndb.StringProperty()

  def _get_shard_key(self, index):
    """
      This function returns a particular shard key with the given index
      Args:
        index : Index of the shard whose's key is required
    """
    return ndb.Key(IncrementOnlyShard, SHARD_KEY_TEMPLATE.format
                   (self.key.id(), index))

  def _get_shard_keys(self, start = 0, end = self.num_shards):
    """
      This function returns all the keys of each shard of this counter
    """
    return [self._get_shard_key(index) for index in range(start, end)]

  def _get_shards(self, start = 0, end = self.num_shards):
    """
      This function returns all the shards associated with this counter
    """
      return ndb.get_multi(self._get_all_shard_keys(start, end))

  def get_quantity(self):
    shard_list = self._get_all_shards()
    value = 0
    for shard in shard_list:
      if shard is not None:
        value += shard.count
    return value

  @ndb.transactional(xg = True)
  def shard_minify(self):
    """
      Function to minify shards. Since NDB allows 25 entity groups per 
      transaction we can only minify 25 transactions per minification. Thus we 
      either reduce the num shards by half or decrease it by 25. This is sensible
      because we do not expect the number of shards to be more than ~100
    """
    counter = self.key.get()

    value = counter.num_shards / 2
    value = min(value, MAX_ENTITIES_PER_TRANSACTION - 1)
    if value != 0:
      shard_list = counter._get_shards(counter.num_shards - value, counter.num_shards)
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

  def increment(self):

