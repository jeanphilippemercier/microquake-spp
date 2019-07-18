from redis import Redis
from spp.core.settings import settings
from rq import Queue

def connect_redis():
    if 'REDIS_MASTER_SERVICE_HOST' in settings:
        redis_db = dict(
            host=settings.REDIS_MASTER_SERVICE_HOST,
            port=settings.REDIS_MASTER_SERVICE_PORT,
            password=settings.REDIS_PASSWORD
        )
    else:
        redis_db = settings.get('redis_db')

    redis_config = redis_db

    return Redis(**redis_config)

class RedisQueue():
    def __init__(self, queue, timeout=600):
        self.redis = connect_redis()
        self.timeout = timeout
        self.queue = queue
        self.rq_queue = Queue(self.queue, connection=self.redis,
                              default_timeout=self.timeout)

    def submit_task(self, func, *args, **kwargs):
        return self.rq_queue.enqueue(func, *args, **kwargs)


# def submit_task_to_rq(queue, func, *args, **kwargs):
#     with connect_redis() as redis:
#         rq_queue = Queue(queue, connection=redis)
#         return rq_queue.enqueue(func, *args, **kwargs)

# rq worker --url redis://redisdb:6379 --log-format '%(asctime)s '  api