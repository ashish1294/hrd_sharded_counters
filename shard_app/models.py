from google.appengine.ext import ndb
from counters import IncrementOnlyCounter as IOC

# Create your models here.

class IncrementTransaction(ndb.Model):
  shard_key = ndb.KeyProperty(kind=IOC.IncrementOnlyShard)
