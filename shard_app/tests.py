# Setting Up Global Environment for Test
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "hrd_sharded_counters.settings")
sys.path.append('/Users/ashish/Documents/repos/hrd_sharded_counters/')
sys.path.append('/Users/ashish/Desktop/google_appengineSdk/google_appengine')
sys.path.append('/Users/ashish/Desktop/google_appengineSdk/google_appengine/lib/yaml/lib')

# Importing Required Library
from django.test import TestCase
from google.appengine.ext import testbed
from google.appengine.ext import ndb
import unittest

class Test
