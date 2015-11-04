from google.appengine.ext import ndb

SHARD_KEY_TEMPLATE = 'in_stock_shard-{0}-{1}'

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

  def _get_all_shard_keys(self):
    """
      This function returns all the keys of each shard of this counter
    """
    return [self._get_shard_key(index) for index in range(self.num_shards)]

  def _get_all_shards(self):
    """
      This function returns all the shards associated with this counter
    """
      return ndb.get_multi(self._get_all_shard_keys)

  def get_quantity(self):
    shard_list = self._get_all_shards()
    value = 0
    for shard in shard_list:
      if shard is not None:
        value += shard.count
    return value

  def increment(self):

