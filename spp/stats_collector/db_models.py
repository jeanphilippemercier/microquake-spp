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

data_quality = db.Table('data_quality', metadata,
                        db.Column('timestamp', db.DateTime),
                        db.Column('processing_timestamp', db.DateTime),
                        db.Column('data_quality_index', db.Float),
                        db.Column('station', db.String(8)),
                        db.Column('location', db.String(8)),
                        db.Column('component', db.String(3)),
                        db.Column('percent_recovery', db.Float),
                        db.Column('signal_std', db.Float),
                        db.Column('signal_energy', db.Float),
                        db.Column('signal_max_amplitude', db.Float),
                        db.Column('signal_dominant_frequency', db.Float),
                        db.Column('average_cross_correlation', db.Float))

