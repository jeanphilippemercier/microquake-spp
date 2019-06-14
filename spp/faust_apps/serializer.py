from redis import StrictRedis
import pickle
from loguru import logger


class Seismic:
    def __init__(self, redis_settings):
        """

        :param redis_key: base redis key used to insert or read the data
        :param redis_settings: setting to connect to the redis database
        :param ttl: time to leave for object inserted in Redis
        """

        self.redis_conn = StrictRedis(**redis_settings)

    def serialize(self, process_id: str, input_dictionary: dict):
        for key in input_dictionary.keys():

            redis_key = '%s.%s' % (process_id, key)

            self.redis_conn.set(redis_key,
                                pickle.dumps(input_dictionary[key]))
            # from pdb import set_trace; set_trace()

    def deserialize(self, process_id: str, object_types: list):
        output_dict = {}
        for object_type in object_types:
            redis_key = '%s.%s' % (process_id, object_type)
            logger.info('Getting data for the following redis_key: %s' %
                        redis_key)
            obj = self.redis_conn.get(redis_key)
            logger.info('The size of the object retrieved is %d' % len(obj))
            output_dict[object_type] = pickle.loads(obj)

        return output_dict


