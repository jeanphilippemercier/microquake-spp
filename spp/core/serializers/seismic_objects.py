import msgpack
from io import BytesIO
from microquake.core import read, read_events
from functools import wraps

def serialize(catalogue=None, fixed_length=None, context=None,
              variable_length=None):
    dict_out = {}
    if catalogue is not None:
        file_out = BytesIO()
        catalogue.write(file_out, format='quakeml')
        dict_out['catalogue'] = file_out.getvalue()

    if fixed_length is not None:
        file_out = BytesIO()
        fixed_length.write(file_out, format='mseed')
        dict_out['fixed_length'] = file_out.getvalue()

    if context is not None:
        file_out = BytesIO()
        context.write(file_out, format='mseed')
        dict_out['context'] = file_out.getvalue()

    if variable_length is not None:
        file_out = BytesIO()
        variable_length.write(file_out, format='mseed')
        dict_out['variable_length'] = file_out.getvalue()

    return msgpack.dumps(dict_out)

def deserialize(message):
    serialized_dict_in = msgpack.loads(message)

    dict_in = {}
    if b'catalogue' in serialized_dict_in.keys():
        bytes = serialized_dict_in[b'catalogue']
        dict_in['catalogue'] = read_events(BytesIO(bytes), format='quakeml')

    if b'fixed_length' in serialized_dict_in.keys():
        bytes = serialized_dict_in[b'fixed_length']
        dict_in['fixed_length'] = read(BytesIO(bytes), format='mseed')

    if b'context' in serialized_dict_in.keys():
        bytes = serialized_dict_in[b'context']
        dict_in['context'] = read(BytesIO(bytes), format='mseed')

    if b'variable_length' in serialized_dict_in.keys():
        bytes = serialized_dict_in[b'variable_length']
        dict_in['variable_length'] = read(BytesIO(bytes),
                                               format='mseed')

    return dict_in

def deserialize_message(func):
    @wraps(func)
    def wrapper(*args, data, serialized, **kwargs):
        if not serialized:
            return func(**kwargs)
        else:
            dict_in = deserialize(data)
        return func(**dict_in)
    return wrapper

