from decimal import Decimal
from random import randint

from BeautifulSoup import BeautifulSoup
from django.test import Client, TestCase, TransactionTestCase
from django.utils import timezone
from django.urls import reverse

from mysql.models import MtM, OtM, OtO, Reference

class TestReference(TestCase):

    def setUp(self):
        self.count = randint(1000, 50000)
        self.description = 'I solemnly swear I am up to no good.'
        self.email = 'harry_potter@hogwarts.edu'
        self.money = Decimal(str(self.count / 100.00))
        self.start_time = timezone.now()
        self.status = True
        self.name = 'Godric Gryffindor'
        self.url = 'http://www.hogwarts.edu/'

    def tearDown(self):
        self.choice = 0

    def test_01_create_get(self):
        expected = Reference.objects.create()
        actual = Reference.objects.get(id=expected.id)
        self.assertEqual(expected.id, actual.id)

    def test_02_create_get_count(self):
        expected = Reference.objects.create(count=self.count)
        actual = Reference.objects.get(id=expected.id)
        self.assertEqual(expected.count, actual.count)

    def test_03_create_get_description(self):
        expected = Reference.objects.create(description=self.description)
        actual = Reference.objects.get(id=expected.id)
        self.assertEqual(expected.description, actual.description)

    def test_04_create_get_email(self):
        expected = Reference.objects.create(email=self.email)
        actual = Reference.objects.get(id=expected.id)
        self.assertEqual(expected.email, actual.email)

    def test_05_create_get_money(self):
        expected = Reference.objects.create(money=self.money)
        actual = Reference.objects.get(id=expected.id)
        self.assertEqual(expected.money, actual.money)

    def test_06_create_get_start_time(self):
        expected = Reference.objects.create(start_time=self.start_time)
        actual = Reference.objects.get(id=expected.id)
        self.assertEqual(expected.start_time, actual.start_time)

    def test_07_create_get_status(self):
        expected = Reference.objects.create(status=self.status)
        actual = Reference.objects.get(id=expected.id)
        self.assertEqual(expected.status, actual.status)

    def test_08_create_get_name(self):
        expected = Reference.objects.create(name=self.name)
        actual = Reference.objects.get(id=expected.id)
        self.assertEqual(expected.name, actual.name)

    def test_09_create_get_url(self):
        expected = Reference.objects.create(url=self.url)
        actual = Reference.objects.get(id=expected.id)
        self.assertEqual(expected.url, actual.url)

class TestMtM(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_01_create_get(self):
        ref = Reference.objects.create()
        expected = MtM.objects.create()
        expected.ref.add(ref)
        expected.save()
        actual = MtM.objects.get(id=expected.id)
        self.assertEqual(expected.id, actual.id)
        self.assertEqual(expected.ref, actual.ref)

class TestOtM(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_01_create_get(self):
        reference = Reference.objects.create()
        expected = OtM.objects.create(ref=reference)
        actual = OtM.objects.get(id=expected.id)
        self.assertEqual(expected.id, actual.id)

class TestOtO(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_01_create_get(self):
        reference = Reference.objects.create()
        expected = OtO.objects.create(ref=reference)
        actual = OtO.objects.get(id=expected.id)
        self.assertEqual(expected.id, actual.id)
