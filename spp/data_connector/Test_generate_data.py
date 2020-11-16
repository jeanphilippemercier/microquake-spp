import numpy as np
from spp.db.connectors import connect_postgres
from datetime import datetime, timedelta
from microquake.helpers.logging import logger
from time import time
from spp.db.connectors import connect_redis
import psycopg2
from psycopg2.extras import execute_values

connection = psycopg2.connect(user = "postgres",
                              password = "password",
                              host = "spp-postgres",
                              port = "5432",
                              database = "postgres")

cursor = connection.cursor()

# postgres_db.url = 'postgresql://postgres:password@spp-postgres:5432/'

rc = connect_redis()
pg, session = connect_postgres()

sensor_id = 1

sample_rate = 6000

sensor_count = 125
sensor_ids = range(0, sensor_count)
average_sample_count = 65000
deviation = 0.1
last_endtime = [datetime.now() - timedelta(seconds=1000)] * sensor_count
sample_rate = 6000

while 1:
    print(last_endtime[0])
    wfs = []
    base_query = """
    INSERT INTO recordings (starttime, endtime, sensor_id, sample_count, 
    sample_rate, data) VALUES %s
    """
    values = []
    for sensor_id in sensor_ids:

        if last_endtime[sensor_id] > datetime.now() - timedelta(seconds=30):
            continue
        sample_count = int((1 + np.random.randn(1) * deviation) *
                           average_sample_count)

        duration_seconds = sample_count / sample_rate
        starttime = last_endtime[sensor_id]
        endtime = starttime + timedelta(seconds=duration_seconds)
        data = np.random.randn(3, sample_count).tobytes()
        # y = np.random.randn(sample_count)
        # z = np.random.randn(sample_count)
        # data =

        # redis_key = str(uuid4())
        # wf = Recording(time=last_endtime[sensor_id], sensor_id=sensor_id,
        #                 sample_count=sample_count, sample_rate=sample_rate,
        #                 data=data)

        # wfs.append(wf)

        # id = Column(db.Integer, primary_key=True)
        # starttime = Column(db.DateTime, nullable=False)
        # endtime = Column(db.DateTime, nullable=False)
        # sensor_id = Column(db.Integer, nullable=False)
        # sample_count = Column(db.Integer, nullable=False)
        # sample_rate = Column(db.Float, nullable=False)
        # # x = Column(db.ARRAY(db.Float), nullable=False)
        # # y = Column(db.ARRAY(db.Float), nullable=False)
        # # z = Column(db.ARRAY(db.Float), nullable=False)
        # data = Column(db.String(255))

        value = (starttime, endtime, sensor_id, sample_count,
                 sample_rate, data)

        values.append(value)
        # rc.set(redis_key, data)

        # execute_values(cursor, "INSERT INTO recordings (id, starttime, "
        #                        "endtime, sensor_id, sample_count, "
        #                        "sample_rage, data"
        #                        ") VALUES %s",
        # ...[(1, 2, 3), (4, 5, 6), (7, 8, 9)])

        last_endtime[sensor_id] += timedelta(seconds=duration_seconds)

    logger.info('inserting data in the database')
    t0 = time()
    execute_values(cursor, base_query, values)
    # session.add_all(wfs)
    # session.commit()
    connection.commit()
    t1 = time()
    t = t1 - t0
    logger.info('done inserting data in the database in {} seconds'.format(t))




# def insert_data_pg():

    #
    #
    # with connect_postgres(db_name=db_name) as pg:
    #     query = db.insert(processing_logs)
    #     values_list = [document]
    #
    #     result = pg.execute(query, values_list)

# class waveforms(Base):
#     __tablename__ = 'recordings'
#
#     id = Column(db.Integer, primary_key=True)
#     time = Column(db.DateTime, nullable=False)
#     sensor_id = Column(db.Integer, nullable=False)
#     sample_count = Column(db.Integer, nullable=False)
#     sample_rate = Column(db.Float, nullable=False)
#     x = Column(db.ARRAY(db.Float), nullable=False)
#     y = Column(db.ARRAY(db.Float), nullable=False)
#     z = Column(db.ARRAY(db.Float), nullable=False)
