from redis import StrictRedis
import pickle


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

    def deserialize(self, process_id: str, output_keys: list):
        output_dict = {}
        for key in output_keys:
            redis_key = '%s.%s' % (process_id, key)
            output_dict[key] = pickle.loads(self.redis_conn.get(redis_key))

        return output_dict


