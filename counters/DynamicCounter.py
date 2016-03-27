from google.appengine.ext import ndb

COUNTER_KEY_TEMPLATE = '{1}-DynamicCounter'

class DynamicShard(ndb.Model):
  value = ndb.IntegerProperty(default=0, indexed=False)
  counter_key = ndb.KeyProperty(kind='DynamicCounter')

class DynamicCounter(ndb.Model):
  count = ndb.IntegerProperty(default=0, indexed=False)

  def __str__(self):
    return "(Count = %d)" % (self.count)

  def __repr__(self):
    return self.__str__()

  @classmethod
  def _get_key(cls, counter_name):
    '''
      This method returns the key of the given counter
    '''
    return ndb.Key(cls, COUNTER_KEY_TEMPLATE.format(counter_name))

  @classmethod
  def get_counter(cls, counter_name):
    '''
      Utility function to check if a counter with given name exist
      Returns:
        Counter value if it exists, False if no such counter exists
    '''
    counter = cls._get_key(counter_name).get()
    return counter.count if counter is not None else None

  @ndb.transactional
  @classmethod
  def _add_to_count(cls, counter_name, value=0):
    '''
      This function adds to the main counter value.
      Returns : None if counter name is invalid. Else it returns the new counter
        value
    '''
    counter = cls._get_key(counter_name).get()
    if counter is None:
      return None
    counter.count += value
    counter.save()
    return counter.count

  @ndb.transactional
  @classmethod
  def _set_count(cls, counter_name, value=0):
    '''
      This function sets the main counter value.
    '''
    counter = cls._get_key(counter_name).get()
    if counter is None:
      return None
    counter.count = value
    counter.save()

  @classmethod
  def increment(cls, counter_name, value=1):
    '''
      This function creates a new shard with the increment value. This will be
      counted in the next minify operation
    '''
    counter_key = cls._get_key(counter_name)
    shard = DynamicShard(value=value, counter_key=counter_key)
    shard.save()

  @classmethod
  def minify(cls, counter_name):
    '''
      This function deletes all the existing shards and adds the value to the
      main counter.
      Returns : None if counter name is invalid. New counter value otherwise
    '''
    counter_key = cls._get_key(counter_name)
    shards = DynamicShard.query(DynamicShard.counter_key == counter_key).fetch()
    total = 0
    for shard in shards:
      total += shard.value
    value = cls._add_to_count(counter_name, total)
    for shard in shards:
      shard.delete()
    return value

  @classmethod
  def set(cls, counter_name, value=0):
    '''
      This function sets the count of the counter to a given value
    '''
    counter_key = cls._get_key(counter_name)
    shards = DynamicShard.query(DynamicShard.counter_key == counter_key).fetch()
    for shard in shards:
      shard.delete()
    return cls._set_count(value)
