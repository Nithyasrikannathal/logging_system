

from djongo import models

class Log(models.Model):
    log_id = models.CharField(max_length=100, unique=True)
    timestamp = models.DateTimeField()
    log_type = models.CharField(max_length=50)
    message = models.TextField()

    def __str__(self):
        return self.log_id

