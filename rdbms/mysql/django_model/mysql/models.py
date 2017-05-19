from __future__ import unicode_literals
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import django.utils.timezone
from django.core.validators import URLValidator
from django.db import models
#from django.utils import timezone

class Reference(models.Model):
    id = models.UUIDField(primary_key=True, editable=False, default=uuid4)
    count = models.IntegerField(default=1)
    description = models.TextField(blank=False, null=False, default='description belongs here')
    email = models.EmailField(max_length=70, blank=True, default='nobody@nowhere.com')
    money = models.DecimalField(max_digits=9, decimal_places=2, default=Decimal('0.00'))
    start_time = models.DateTimeField(db_index=True, null=False, default=django.utils.timezone.now)
    status = models.BooleanField(default=False)
    name = models.CharField(max_length=20, blank=False, null=False, default='something')
    url = models.TextField(validators=[URLValidator()], default='http://localhost:8000/')

    def __str__(self):
        return self.id

class MtM(models.Model):
    id = models.UUIDField(primary_key=True, editable=False, default=uuid4)
    ref = models.ManyToManyField(Reference)

    def __str__(self):
        return self.id

class OtM(models.Model):
    id = models.UUIDField(primary_key=True, editable=False, default=uuid4)
    ref = models.ForeignKey(Reference, on_delete=models.CASCADE)

    def __str__(self):
        return self.id

class OtO(models.Model):
    # http://stackoverflow.com/questions/5870537/whats-the-difference-between-django-onetoonefield-and-foreignkey
    id = models.UUIDField(primary_key=True, editable=False, default=uuid4)
    ref = models.OneToOneField(Reference)
    
    def __str__(self):
        return self.id
