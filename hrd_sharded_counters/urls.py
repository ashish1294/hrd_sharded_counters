"""hrd_sharded_counters URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.8/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Add an import:  from blog import urls as blog_urls
    2. Add a URL to urlpatterns:  url(r'^blog/', include(blog_urls))
"""
from django.conf.urls import include, url
from django.contrib import admin
from shard_app.views import minify_shard
from shard_app.views import increment_counter
from shard_app.views import status
from shard_app.views import minify_dynamic

urlpatterns = [
    url(r'^admin/', include(admin.site.urls)),
    url(r'^cron/minify_shard/?$', minify_shard),
    url(r'^cron/minify_dynamic', minify_dynamic),
    url(r'^increment/?$', increment_counter),
    url(r'^status/?$', status),
]
