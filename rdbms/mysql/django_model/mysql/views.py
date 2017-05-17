from django.http import Http404, HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.utils import timezone
from django.views import generic
from django.views.decorators.csrf import csrf_exempt

from mysql.models import MtM, OtM, OtO, Reference

def create(request):
    if not request.method == 'POST':
        return 
    context = {}
    context['title'] = 'Hello World'
    try:
        pass
    except X.DoesNotExist as e:
        return Http404
    html = render(request, 'template.htm', context)
