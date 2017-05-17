from django.conf.urls import include, url
from django.contrib import admin

urlpatterns = [
    url(r'^mysql/', include('mysql.urls')),
    url(r'^admin/', admin.site.urls),
]
