import numpy as np
from spp.core.models import recordings
from spp.core.connectors import connect_postgres
from datetime import datetime, timedelta

pg, session = connect_postgres()

time = datetime.now() - timedelta(seconds=30)
sensor_id = 1
sample_count = 65000
sample_rate = 6000
x = np.random.randn(sample_count)
y = np.random.randn(sample_count)
z = np.random.randn(sample_count)

wf = recordings(time=time, sensor_id=sensor_id, sample_count=sample_count,
               sample_rate=sample_rate, x=x, y=y, z=z)

session.add(wf)
session.commit()




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