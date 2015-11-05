from django.shortcuts import render
from django.http import HTTPResponse
from google.appengine.ext import ndb
from counters import IncrementOnlyCounter
# Create your views here.

def shard_minify(request):
  counters = IncrementOnlyCounter.IncrementOnlyCounter.query()
  for counter in counters:
    counter.shard_minify()
  return HTTPResponse("Successfully minified")
