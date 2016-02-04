from google.appengine.ext import ndb
from google.appengine.api import memcache
from google.appengine.api import datastore_errors

'''
  Important things to remember :
  1. Range should be [-2**63, 2**63]
'''

MEMCACHE_NAME_TEMPLATE = '{0}-memcache-counter'
MIDDLE_VALUE = 2 ** 63
LOCK_VAR_TEMPLATE = '{0}-memlock'

class MemcacheCounter(ndb.Model):

  data = ndb.IntegerProperty(default=0, indexed=False)

  def __str__(self):
    if self.key is None:
      return "In-memory Counter Value = %d" % self.data
    else:
      return "Name = %s, Value = %s" % (self.key.id(), self.data)

  def __repr__(self):
    return self.__str__()

  @classmethod
  def _get_memcache_id(cls, counter_name):
    '''
      This function returns the string id of the counter in memcache
      Args:
        counter_name : Name of the Counter
      Returns: The string id of the memcache value
    '''
    return MEMCACHE_NAME_TEMPLATE.format(counter_name)

  @classmethod
  def _get_multi_memcache_ids(cls, counter_names):
    '''
      This function computes a list of counter ids given a list of counter name
      Args:
        counter_names : List of counter names whose ids have to fetched
      Returns: A list of string ids of the request names
      These ids may or may not exist in memcache
    '''
    return [cls._get_memcache_id(name) for name in counter_names]

  @classmethod
  def _lock_counter(cls, counter_name, duration):
    lock_var = LOCK_VAR_TEMPLATE.format(counter_name)
    return memcache.add(lock_var, None, duration)

  @classmethod
  @ndb.transactional
  def _update_datastore(cls, counter_id, value):
    '''
      This function changes the counter value in datastore transactionally.
      Args :
        counter_id : id of datastore counter
        value : The new value to be persisted
    '''
    # Fetching Counter from Datastore
    counter = cls.get_or_insert(counter_id)
    counter.data = value
    counter.put()
    return counter.data

  @classmethod
  @ndb.transactional
  def _reset_datastore(cls, counter_id):
    '''
      This function resets the datastore. It simply deletes
      Args:
        counter_id : id of the datastore counter
    '''
    ndb.Key(cls, counter_id).delete()

  @classmethod
  def put_to_datastore(cls, name, flush=False):
    '''
      This function explicitly updates value in datastore if the counter exist
      Args:
        name : The name of the counter
        flush : Optional argument that determines if the counter should be
        deleted from memcache after saving to datastore. Defaults to False
      Returns: Updated value in operation was successful, None otherwise
    '''
    counter_id = cls._get_memcache_id(name)
    val = memcache.get(counter_id)
    if val is not None:
      persist_value = val - MIDDLE_VALUE
      try:
        cls._update_datastore(counter_id, persist_value)
      except datastore_errors.TransactionFailedError:
        pass
      else:
        if flush:
          memcache.delete(counter_id)
        return persist_value

  @classmethod
  def increment(cls, name, delta=1, persist_delay=10):
    '''
      This function increments the counter in memcache.
      Args:
        delta : Amount to be incremented
        interval : seconds to wait before updating Datastore
    '''
    counter_id = cls._get_memcache_id(name)

    if delta >= 0:
      val = memcache.incr(counter_id, delta, initial_value=MIDDLE_VALUE)
    else:
      val = memcache.decr(counter_id, -delta, initial_value=MIDDLE_VALUE)

    persist_value = val - MIDDLE_VALUE
    if cls._lock_counter(name, persist_delay):
      # It's time to persist the value in datastore
      try:
        cls._update_datastore(counter_id, persist_value)
      except datastore_errors.TransactionFailedError:
        # Just avoid this transaction failure and try again in next iteration
        pass
    return persist_value

  @classmethod
  def decrement(cls, name, delta=1, persist_delay=10):
    '''
      Just a useful alias for increment
    '''
    return cls.increment(name, -delta, persist_delay)

  @classmethod
  def get(cls, name, initial_value=0):
    '''
      This function will fetch the counter value. It will create a new counter
      if it doesn't exist.
      Args:
        name : Name of the counter to fetch
        initial_value : value to be used if new counter is created. Defaults to
        0
      Returns : The value of the counter
    '''
    counter_id = cls._get_memcache_id(name)
    val = memcache.get(counter_id)
    if val is None:
      # Fetch from Datastore
      counter = cls.get_or_insert(counter_id, data=initial_value)
      # Put the value to Memcache
      memcache.add(counter_id, counter.data + MIDDLE_VALUE)
      return counter.data
    else:
      return val - MIDDLE_VALUE

  @classmethod
  def get_multi(cls, names, initial_value=0):
    '''
      This function gets the value of multiple counters at once. It creates new
      counters if it doesn't already exist
      Args:
        names : list of names of counter values to be fetched
        initial_value : value to be used if new counter is created. Defaults to
        0
      Returns : A dictionary with all the name-value mapping.
    '''
    counter_id_list = cls._get_multi_memcache_ids(names)
    values = memcache.get_multi(counter_id_list)
    ret_values = {}
    for i, counter_id in enumerate(counter_id_list):
      if counter_id not in values:
        # Doesn't exist in Memcache. Fetch from Datastore
        counter = cls.get_or_insert(counter_id, data=initial_value)
        # Put in Memcache
        memcache.add(counter_id, counter.data + MIDDLE_VALUE)
        ret_values[names[i]] = counter.data
      else:
        ret_values[names[i]] = values[counter_id] - MIDDLE_VALUE
    return ret_values

  @classmethod
  def reset(cls, name):
    '''
      This function resets the counter to 0. This function also resets datastore
      Args:
        name : Name of the counter
      Returns:
        True if successful, False if not
        If it returns False, it may still be successful id reset.
        Whenever it returns true, it is successful
    '''
    counter_id = cls._get_memcache_id(name)
    cls._reset_datastore(counter_id)

    # Using CAS to avoid race condition
    client = memcache.Client()
    client.get(counter_id, for_cas=True)
    return client.cas(counter_id, MIDDLE_VALUE)

  @classmethod
  def set(cls, name, value=0):
    '''
      This function sets the counter value to a given value. Resets datastore.
      Args:
        name : Name of the counter
      Returns:
        True if successful, False if not
        If it returns False, it may be successful
    '''
    counter_id = cls._get_memcache_id(name)
    cls._update_datastore(counter_id, value)

    # Using CAS to avoid race condition
    client = memcache.Client()
    client.get(counter_id, for_cas=True)
    return client.cas(counter_id, MIDDLE_VALUE + value)

  @classmethod
  def exist(cls, name):
    '''
      This is a utility function to check if the counter with given name exist
      Args:
        name : Name of the counter
      Returns : True if counter exist. False otherwise
    '''
    counter_id = cls._get_memcache_id(name)
    val = memcache.get(counter_id)
    if val is not None:
      return True
    else:
      counter = ndb.Key(cls, counter_id).get()
    if counter is not None:
      # Put value into Memcache
      memcache.add(counter_id, counter.data + MIDDLE_VALUE)
      return True
    else:
      return False

  @classmethod
  def delete(cls, name):
    '''
      This function deletes the counter both from datastore and memcache
      Args:
        name : Name of the counter
    '''
    counter_id = cls._get_memcache_id(name)
    ndb.Key(cls, counter_id).delete()
    memcache.delete(counter_id)

  @classmethod
  def delete_multi(cls, names):
    '''
      This function deletes multiple counters at once
      Args:
        name : list of counter names to be deleted
    '''
    counter_id_list = cls._get_multi_memcache_ids(names)
    ndb.delete_multi([ndb.Key(cls, cid) for cid in counter_id_list])
    memcache.delete_multi(counter_id_list)

  # Useful Aliases
  value = count = get
  reinitialize = reset
  incr = offset = increment
  decr = decrement
