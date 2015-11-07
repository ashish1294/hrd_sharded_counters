from django.shortcuts import render
from django.http import HttpResponse
from google.appengine.ext import ndb
from counters import IncrementOnlyCounter as IOC
from google.appengine.api import datastore_errors
import uuid

UNSHARDED_COUNTER_KEY = 'unsharded_counter'
SHARDED_COUNTER_KEY = 'sharded_counter'

class IncrementTransaction(ndb.Model):
  shard_key = ndb.KeyProperty(kind=IOC.IncrementOnlyShard)

@ndb.transactional(xg=True)
def increment_normal_counter(delta, request_id):
  log_key = ndb.Key(IncrementTransaction, request_id)
  if log_key.get() is not None:
    return

  IncrementTransaction.get_or_insert(request_id)

  counter = IOC.IncrementOnlyShard.get_or_insert(UNSHARDED_COUNTER_KEY)
  counter.count += delta
  counter.put()

def increment_sharded_counter(delta):
  counter = IOC.IncrementOnlyCounter.get_or_insert(SHARDED_COUNTER_KEY)
  counter.increment(delta)

def minify_shard(request):
  counters = IOC.IncrementOnlyCounter.query()
  for counter in counters.iter():
    counter.minify_shards()
    return HttpResponse("Successfully minified")
  return HttpResponse("No Shard to Minify")

def get_increment_counters():
  counters = []

  counter1 = IOC.IncrementOnlyCounter.get_or_insert(SHARDED_COUNTER_KEY)
  counters.append(counter1.count)

  counter2 = IOC.IncrementOnlyShard.get_or_insert(UNSHARDED_COUNTER_KEY)
  counters.append(counter2.count)
  return counters

def IncrementOnlyCounter(request):
  counters = get_increment_counters()
  print counters
  response = render(request, 'IncrementOnlyCounter.html', {
     'sharded_counter'   : counters[0],
     'unsharded_counter' : counters[1]
    })
  return response

def increment_counter(request):
  params = request.GET
  counter_type = params.get('type', '-1')
  delta = params.get('delta', '0')
  try:
    counter_type = int(counter_type)
    delta = int(delta)
  except ValueError:
    counter_type = -1
    delta = 0

  if counter_type == 0:
    # Increment Normal Counter
    try:
      request_id = str(uuid.uuid4())
      increment_normal_counter(delta, request_id)
    except datastore_errors.TransactionFailedError:
      response = HttpResponse("Request dropped")
    else:
      response = HttpResponse("Request Successful")

  elif counter_type == 1:
    # Increment Sharded Counter
    try:
      increment_sharded_counter(delta)
    except datastore_errors.TransactionFailedError, e:
      response = HttpResponse("Request Dropped : " + str(e))
    else:
      response = HttpResponse("Request Successful")

  else:
    response = HttpResponse("Invalid parameters")

  return response
