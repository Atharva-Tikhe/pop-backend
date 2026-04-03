from .celery_app import celery
from time import sleep

@celery.task
def add(x: int, y: int):
    return x + y

