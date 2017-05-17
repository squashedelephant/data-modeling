from django.conf.urls import url

from mysql.views import create

app_name = 'mysql'
urlpatterns = [
    url(r'^create', create, name='create'),
]
