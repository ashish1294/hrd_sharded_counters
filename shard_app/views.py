import uuid
from django.shortcuts import render
from django.http import HttpResponse
from google.appengine.ext import ndb
from google.appengine.api import datastore_errors
from counters import IncrementOnlyCounter as IOC
from counters.MemcacheCounter import MemcacheCounter as MC
from counters.DynamicCounter import DynamicCounter as DC
from models import IncrementTransaction

UNSHARDED_COUNTER_KEY = 'unsharded_counter'
SHARDED_COUNTER_KEY = 'sharded_counter'
MEMCACHE_COUNTER_KEY = 'memcache_counter'
DYNAMIC_COUNTER_KEY = 'dynamic-counter'
REQ_UNSHARDED = "0"
REQ_SHARDED_INCREMENT = "1"
REQ_MEMCACHE = "2"
REQ_DYNAMIC = "3"

@ndb.transactional(xg=True)
def increment_unsharded_counter(delta, request_id):
  log_key = ndb.Key(IncrementTransaction, request_id)
  if log_key.get() is not None:
    return

  counter = IOC.IncrementOnlyShard.get_or_insert(UNSHARDED_COUNTER_KEY)
  counter.count += delta
  key = counter.put()

  # Adding Transaction Log Here
  trx = IncrementTransaction(id=request_id, shard_key=key)
  trx.put()

def unsharded_counter_value():
  return IOC.IncrementOnlyShard.get_or_insert(UNSHARDED_COUNTER_KEY).count

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
    val = IOC.IncrementOnlyCounter.get(SHARDED_COUNTER_KEY)
    if val is None:
      IOC.IncrementOnlyCounter.get_or_insert(SHARDED_COUNTER_KEY,
                                             max_shards=80,
                                             num_shards=40,
                                             dynamic_growth=False)
      val = IOC.IncrementOnlyCounter.get(SHARDED_COUNTER_KEY)
    response = HttpResponse(str(val))
  elif counter_type == REQ_MEMCACHE:
    response = HttpResponse(str(MC.get(MEMCACHE_COUNTER_KEY)))
  elif counter_type == REQ_DYNAMIC:
    response = HttpResponse(str(DC.get_value(DYNAMIC_COUNTER_KEY)))
  else:
    # Status of all counters
    sharded_val = IOC.IncrementOnlyCounter.get(SHARDED_COUNTER_KEY)
    if sharded_val is None:
      IOC.IncrementOnlyCounter.get_or_insert(SHARDED_COUNTER_KEY,
                                             num_shards=40,
                                             max_shards=80,
                                             dynamic_growth=False)
    response = render(request, 'status.html', {
        'unsharded_counter' : unsharded_counter_value(),
        'sharded_counter' : IOC.IncrementOnlyCounter.get(SHARDED_COUNTER_KEY),
        'memcache_counter' : MC.get(MEMCACHE_COUNTER_KEY),
        'dynamic_counter' : DC.get_value(DYNAMIC_COUNTER_KEY)
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
      response = HttpResponse("Request dropped", status=250)
    else:
      response = HttpResponse("Request Successful")

  elif counter_type == REQ_SHARDED_INCREMENT:
    # Increment Sharded Counter
    try:
      IOC.IncrementOnlyCounter.increment(SHARDED_COUNTER_KEY, delta,
                                         idempotency=True)
    except datastore_errors.TransactionFailedError:
      response = HttpResponse("Request Dropped", status=250)
    else:
      response = HttpResponse("Request Successful")
  elif counter_type == REQ_MEMCACHE:
    try:
      MC.incr(MEMCACHE_COUNTER_KEY, delta)
    except datastore_errors.TransactionFailedError:
      response = HttpResponse("Request Dropped", status=250)
    else:
      response = HttpResponse("Request Successful")
  elif counter_type == REQ_DYNAMIC:
    DC.increment(DYNAMIC_COUNTER_KEY)
    response = HttpResponse("Request Successful")
  else:
    response = HttpResponse("Invalid parameters" + str(counter_type))

  return response

def minify_dynamic(request):
  DC.minify(DYNAMIC_COUNTER_KEY)
  return HttpResponse("Successfully minified")
