# hrd_sharded_counters

[![Build Status](https://travis-ci.org/ashish1294/hrd_sharded_counters.svg?branch=master)](https://travis-ci.org/ashish1294/hrd_sharded_counters)
[![Coverage Status](https://coveralls.io/repos/github/ashish1294/hrd_sharded_counters/badge.svg?branch=master)](https://coveralls.io/github/ashish1294/hrd_sharded_counters?branch=master)
[![Stories in Ready](https://badge.waffle.io/ashish1294/hrd_sharded_counters.svg?label=ready&title=Ready)](http://waffle.io/ashish1294/hrd_sharded_counters)
[![Code Health](https://landscape.io/github/ashish1294/hrd_sharded_counters/master/landscape.svg?style=flat)](https://landscape.io/github/ashish1294/hrd_sharded_counters/master)

Sharded Counter Implementation using NDB for Google Cloud Datastore

Dependencies
* Google App Engine SDK
* Django

Please note that this application requires > Django 1.8 - however App Engine
runtime only supports django1.5. So you have to package django 1.8 separately
before deploying. To do this run :
<code>pip install -t django django</code><br/>

This will create a folder name django in the root directory with latest Django
runtime.

TO DO
=====
* Add Async function wherever possible in Memcache Counter
* Use Multi Version in Increment Only Counter Wherever possible
* Counter State
* Implement Multi get / delete / update function in IncrementOnlyCounters
* Plot a simpler graph. For average response time.
* Assume Load while plotting instead to computing
* Consider Mean time
