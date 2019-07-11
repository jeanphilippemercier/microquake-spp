from pymongo import MongoClient
from redis import Redis
# from elasticsearch import Elasticsearch
from spp.core.settings import settings
import sqlalchemy as db


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

#
# def connect_elastic():
#     if 'ELASTIC_MASTER_SERVICE_HOST' in settings:
#         elastic_config = dict(host=settings.ELASTIC_MASTER_SERVICE_HOST,
#                               port=settings.ELASTIC_MASTER_SERVICE_PORT)
#
#         # will need to add the credentials here
#
#     else:
#         elastic_config = settings.get('elastic_db')
#
#     return Elasticsearch(**elastic_config)


def connect_postgres(db_name='spp'):

    from spp.core import db_models

    if 'POSTGRES_MASTER_SERVICE_HOST' in settings:
        pass
        # postgres_config = dict(host=settings.POSTGRES_MASTER_SERVICE_HOST,
        #                        port=settings.POSTGRES_MASTER_SERVICE_PORT,
        #                        user=settings.POSTGRES_MASTER_SERVICE_USER,
        #                        password=settings.POSTGRES_MASTER_SERVICE_PASSWORD)

    else:
        postgres_url = settings.get('postgres_db').url + db_name

    engine = db.create_engine(postgres_url)
    connection = engine.connect()
    # Create tables if they do not exist
    db_models.metadata.create_all(engine)

    return connection


