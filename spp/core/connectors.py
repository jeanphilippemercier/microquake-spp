from pymongo import MongoClient
from redis import Redis
from spp.core.settings import settings


def connect_mongo():
    if 'MONGO_MONGODB_SERVICE_HOST' in settings:
        mongo_url = f"mongodb://root:{settings.MONGODB_PASSWORD}@{settings.MONGO_MONGODB_SERVICE_HOST}:{settings.MONGO_MONGODB_SERVICE_PORT}"
    else:
        mongo_url = settings.get('mongo_db').url

    return MongoClient(mongo_url)


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
