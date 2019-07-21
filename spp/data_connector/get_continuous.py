from datetime import datetime, timedelta
from loguru import logger
from spp.core.settings import settings
from spp.core.connectors import (connect_postgres)
from spp.core.db_models import Recording

from sqlalchemy.orm import sessionmaker

import sqlalchemy as db


db_name = settings.get('postgres_db').db_name
postgres_url = settings.get('postgres_db').url + db_name
engine = db.create_engine(postgres_url)
pg = connect_postgres(db_name=db_name)
Session = sessionmaker(bind=engine)
session = Session()

endtime = datetime.utcnow() - timedelta(minutes=5)
starttime = endtime - timedelta(minutes=1)

session.query(Recording).filter_by(
    time<=endtime).filter_by(end_time>=starttime).all()


from spp.data_connector import waveform_extractor
from importlib import reload
reload(waveform_extractor)
starttime = starttime - timedelta(hours=12)
cat = web_client.get_catalogue(base_url, starttime, endtime, sites, utc,
                               accepted=False, manual=False)
message = serialize(cat[0])
waveform_extractor.extract_waveform(data=message, serialized=True)
