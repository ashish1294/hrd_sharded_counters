from django.shortcuts import render
from django.http import HTTPResponse
from google.appengine.ext import ndb
from counters import IncrementOnlyCounter as IOC

UNSHARDED_COUNTER_KEY = 'unsharded_counter'
SHARDED_COUNTER_KEY = 'sharded_counter'

class IncrementTransaction(ndb.Model):
  shard_key = ndb.KeyProperty(kind=IOC.IncrementOnlyShard)

@ndb.transactional
def increment_normal_counter(delta, request_id):
  log_key = ndb.Key(IncrementTransaction, request_id)
  if log_key.get() is not None:
    return

  IncrementTransaction.get_or_insert(log_key)

  counter = IOC.IncrementOnlyShard.get_or_insert(UNSHARDED_COUNTER_KEY)
  counter.count += delta
  counter.put()

def increment_sharded_counter(delta):
  counter = IOC.IncrementOnlyShard.get_or_insert(SHARDED_COUNTER_KEY)
  counter.increment(delta)

def shard_minify(request):
  counters = IOC.IncrementOnlyCounter.query()
  if len(counters) != 0:
    counters[0].shard_minify()
  else:
    IOC.IncrementOnlyCounter.get_or_insert(SHARDED_COUNTER_KEY)

  return HTTPResponse("Successfully minified")

def get_increment_counters():
  counters = []
  
  counter1 = IOC.IncrementOnlyShard.get_or_insert(SHARDED_COUNTER_KEY)
  counters.append(counter1.count)

  counter2 = IOC.IncrementOnlyShard.get_or_insert(UNSHARDED_COUNTER_KEY)
  counters.append(counter2.count)

def IncrementOnlyCounter(request):
  counters = get_increment_counters()
      response = render(request,
                'IncrementOnlyCounter.html', {
                 'sharded_counter'   : counters[0],
                 'unsharded_counter' : counters[1]
                })
  return response

def increment_counter(request):
  params = request.GET
  counter_type = params.get('type', ['-1'])
  delta = params.get('delta', ['0'])
  try:
    counter_type = int(counter_type[0])
    delta = int(delta[0])
  except ValueError:
    counter_type = -1
    delta = 0

  if counter_type == 0:
    # Increment Normal Counter
    try:
      request_id = str(uuid.uuid4())
      increment_normal_counter(delta, request_id)
    except datastore_errors.TransactionFailedError:
      response = HTTPResponse("Request dropped")
    else:
      response = IncrementOnlyCounter(request)

  elif counter_type == 1:
    # Increment Sharded Counter
    try:
      increment_sharded_counter(delta)
    except datastore_errors.TransactionFailedError, e:
      response = HTTPResponse("Request Dropped : " + str(e))
    else:
      response = IncrementOnlyCounter(request)

  else:
    response = HTTPResponse("Invalid parameters")

  return response