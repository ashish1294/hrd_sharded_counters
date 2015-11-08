from django.shortcuts import render
from django.http import HttpResponse
from google.appengine.ext import ndb
from google.appengine.api import memcache
from counters import IncrementOnlyCounter as IOC
from google.appengine.api import datastore_errors
from models import IncrementTransaction
import uuid

UNSHARDED_COUNTER_KEY = 'unsharded_counter'
SHARDED_COUNTER_KEY = 'sharded_counter'
REQ_UNSHARDED = "0"
REQ_SHARDED_INCREMENT = "1"
REQ_SHARDED_GENERAL = "2"

@ndb.transactional(xg=True)
def increment_unsharded_counter(delta, request_id):
  log_key = ndb.Key(IncrementTransaction, request_id)
  if log_key.get() is not None:
    return

  IncrementTransaction.get_or_insert(request_id)

  counter = IOC.IncrementOnlyShard.get_or_insert(UNSHARDED_COUNTER_KEY)
  counter.count += delta
  counter.put()
  memcache.incr(UNSHARDED_COUNTER_KEY, delta=delta)

def unsharded_counter_value():
  count = memcache.get(UNSHARDED_COUNTER_KEY)
  if count is None:
    count = ndb.Key(IOC.IncrementOnlyShard, UNSHARDED_COUNTER_KEY).get().count
    memcache.add(UNSHARDED_COUNTER_KEY, count)
  return count

def increment_sharded_counter(delta):
  counter = IOC.IncrementOnlyCounter.get_or_insert(
      SHARDED_COUNTER_KEY,
      idempotency=True,
      max_shards=30)
  counter.increment(delta)

#pylint: disable=unused-argument
def minify_shard(request):
  counters = IOC.IncrementOnlyCounter.query()
  for counter in counters.iter():
    counter.minify_shards()
    return HttpResponse("Successfully minified")
  return HttpResponse("No Shard to Minify")

def status(request):
  params = request.GET
  counter_type = params.get('type', '-1')

  if counter_type == REQ_UNSHARDED:
    response = HttpResponse(str(unsharded_counter_value()))
  elif counter_type == REQ_SHARDED_INCREMENT:
    val = IOC.IncrementOnlyCounter.get_or_insert(SHARDED_COUNTER_KEY).count
    response = HttpResponse(str(val))
  else:
    # Status of all counters
    val2 = ndb.Key(IOC.IncrementOnlyCounter, SHARDED_COUNTER_KEY).get().count
    response = render(request, 'status.html', {
        'unsharded_counter' : unsharded_counter_value(),
        'sharded_counter' : val2
    })
  return response

def increment_counter(request):
  params = request.GET
  counter_type = params.get('type', '-1')
  delta = params.get('delta', '0')
  try:
    delta = int(delta)
  except ValueError:
    counter_type = "-1"
    delta = 0

  if counter_type == REQ_UNSHARDED:
    # Increment Unsharded Counter
    try:
      request_id = str(uuid.uuid4())
      increment_unsharded_counter(delta, request_id)
    except datastore_errors.TransactionFailedError:
      response = HttpResponse("Request dropped")
    else:
      response = HttpResponse("Request Successful")

  elif counter_type == REQ_SHARDED_INCREMENT:
    # Increment Sharded Counter
    try:
      increment_sharded_counter(delta)
    except datastore_errors.TransactionFailedError, error_message:
      response = HttpResponse("Request Dropped : " + str(error_message))
    else:
      response = HttpResponse("Request Successful")
  else:
    response = HttpResponse("Invalid parameters" + str(counter_type))

  return response
