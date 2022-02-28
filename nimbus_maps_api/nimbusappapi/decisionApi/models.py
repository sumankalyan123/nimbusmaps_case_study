from django.db import models


class sample_data(models.Model):
    uid = models.CharField(max_length=60)
    address = models.CharField(max_length=60)
    city = models.CharField(max_length=60)
    postcode = models.CharField(max_length=60)
    decision = models.CharField(max_length=60)

    def __str__(self):
        return self.uid

# Create your models here.
