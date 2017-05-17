from django.contrib import admin

from mysql.models import MtM, OtM, OtO, Reference

admin.site.register(MtM)
admin.site.register(OtM)
admin.site.register(OtO)
admin.site.register(Reference)
