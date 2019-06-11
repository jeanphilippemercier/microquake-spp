from redis import StrictRedis
import pickle


class Seismic:
    def __init__(self, redis_key, redis_settings, types):
        """

        :param redis_key: base redis key used to insert or read the data
        :param redis_settings: setting to connect to the redis database
        :param types: list of types to serialize or deserialize
        """
        self.accepted_types = ['fixed_length', 'variable_length', 'context',
                               'continuous', 'catalog']

        if type(types) is str:
            if types not in self.accepted_types:
                raise ValueError('waveform_type must be in %s' %
                                 self.accepted_types)
            self.types = [types]

        elif type(types) is list:
            for otype in types:
                if otype not in self.accepted_types:
                    raise ValueError('waveform_type must be in %s' %
                                     self.accepted_types)
            self.types = types

        else:
            raise TypeError('types must either be a <str> or a '
                            '<list>')

        self.redis_conn = StrictRedis(**redis_settings)
        self.redis_base_key = redis_key

    def serialize(self, input_dictionary):
        for key in input_dictionary.keys():
            if key not in self.types:
                raise ValueError('%s is not a valid <type>! \n'
                                 'valid types are %s'
                                 % (key, self.accepted_types))

            redis_key = '%s.%s' % (self.redis_base_key, key)

            self.redis_conn.set(redis_key,
                                pickle.dumps(input_dictionary[key]))
            # from pdb import set_trace; set_trace()

    def deserialize(self):
        output_dict = {}
        for key in self.types:
            redis_key = '%s.%s' % (self.redis_base_key, key)
            output_dict[key] = pickle.loads(self.redis_conn.get(redis_key))

        return output_dict


