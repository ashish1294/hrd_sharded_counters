from google.appengine.ext import ndb

COUNTER_KEY_TEMPLATE = '{0}-DynamicCounter'

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
  def _format_key(cls, counter_name):
    '''
      This method returns the key of the given counter
    '''
    return COUNTER_KEY_TEMPLATE.format(counter_name)

  @classmethod
  def _get_counter(cls, counter_name, default=0):
    '''
      Utility function to check if a counter with given name exist
      Returns:
        Counter value if it exists, False if no such counter exists
    '''
    return cls.get_or_insert(cls._format_key(counter_name), count=default)

  @classmethod
  def get_value(cls, counter_name, default=0):
    '''
      Function to read the value of a counter
    '''
    return cls._get_counter(counter_name, default=default).count

  @classmethod
  @ndb.transactional
  def _add_to_count(cls, counter_name, value=0):
    '''
      This function adds to the main counter value.
      Returns : None if counter name is invalid. Else it returns the new counter
        value
    '''
    counter = cls._get_counter(counter_name)
    counter.count += value
    counter.put()
    print "add key = ", counter.key
    return counter.count

  @classmethod
  @ndb.transactional
  def _set_count(cls, counter_name, value=0):
    '''
      This function sets the main counter value.
    '''
    counter = cls._get_counter(counter_name)
    counter.count = value
    counter.put()

  @classmethod
  def increment(cls, counter_name, value=1):
    '''
      This function creates a new shard with the increment value. This will be
      counted in the next minify operation
    '''
    counter_key = ndb.Key(cls, cls._format_key(counter_name))
    shard = DynamicShard(value=value, counter_key=counter_key)
    shard.put()

  @classmethod
  def minify(cls, counter_name):
    '''
      This function deletes all the existing shards and adds the value to the
      main counter.
      Returns : None if counter name is invalid. New counter value otherwise
    '''
    counter_key = ndb.Key(cls, cls._format_key(counter_name))
    shards = DynamicShard.query(DynamicShard.counter_key == counter_key).fetch()
    print len(shards), "shards found"
    total = 0
    for shard in shards:
      total += shard.value
    value = cls._add_to_count(counter_name, total)
    for shard in shards:
      shard.key.delete()
    return value

  @classmethod
  def delete(cls, counter_name):
    '''
      Function to delete an existing counter
    '''
    cls.minify(counter_name)
    counter = cls._get_counter(counter_name)
    counter.key.delete()

  @classmethod
  def set(cls, counter_name, value=0):
    '''
      This function sets the count of the counter to a given value
    '''
    counter_key = ndb.Key(cls, cls._format_key(counter_name))
    shards = DynamicShard.query(DynamicShard.counter_key == counter_key).fetch()
    for shard in shards:
      shard.key.delete()
    return cls._set_count(counter_name, value)

  @classmethod
  def exist(cls, counter_name):
    '''
      Utility function to check if a counter with given name exist
    '''
    counter_key = ndb.Key(cls, cls._format_key(counter_name))
    counter = counter_key.get()
    print "Exist Key = ", counter_key, counter
    if counter is None:
      print "ret false"
      return False
    else:
      print "ret True"
      return True

  @classmethod
  def decrement(cls, counter_name, value=1):
    '''
      Function to decrese the counter by a given value
    '''
    return cls.increment(counter_name, -value)
