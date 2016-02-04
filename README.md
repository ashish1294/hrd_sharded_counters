# hrd_sharded_counters

[![Build Status](https://travis-ci.com/ashish1294/hrd_sharded_counters.svg?token=2Nn4SpQTSFpAmqNW7tqr&branch=master)](https://travis-ci.com/ashish1294/hrd_sharded_counters)
[![Coverage Status](https://coveralls.io/repos/github/ashish1294/hrd_sharded_counters/badge.svg?branch=master)](https://coveralls.io/github/ashish1294/hrd_sharded_counters?branch=master)
[![Stories in Ready](https://badge.waffle.io/ashish1294/hrd_sharded_counters.svg?label=ready&title=Ready)](http://waffle.io/ashish1294/hrd_sharded_counters)


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
