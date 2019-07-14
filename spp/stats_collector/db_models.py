import sqlalchemy as db
metadata = db.MetaData()

processing_logs = db.Table('processing_logs', metadata,
                            db.Column('event_id',db.String(255)),
                            db.Column('event_timestamp', db.DateTime),
                            db.Column('processing_timestamp', db.DateTime),
                            db.Column('processing_step_name', db.String(255)),
                            db.Column('processing_step_id', db.Integer),
                            db.Column('processing_delay_second', db.Float),
                            db.Column('processing_time_second', db.Float),
                            db.Column('processing_status', db.String(255)))

processing = db.Table('processing', metadata,
                      db.Column('event_id', db.String(255)),
                      db.Column('P_picks', db.Integer),
                      db.Column('S_picks', db.Integer),
                      db.Column('processing_delay_second', db.Float),
                      db.Column('processing_completed_timestamp',
                                db.Float),
                      db.Column('event_category', db.String(255)),
                      db.Column('event_status', db.String(255)))

