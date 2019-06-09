from io import BytesIO

from microquake.core import read_events
from microquake.io import msgpack

from ..utils import timing
from ..settings import settings

from .serializer import Serializer


class Message(Serializer):
    def __init__(self, msg_in):
        self.message = msg_in
        self.deserialize()

    @timing
    def deserialize(self):
        self.waveform_stream, quake_ml_bytes = msgpack.unpack(self.message)
        self.catalog = read_events(BytesIO(quake_ml_bytes), format='QUAKEML')

    @timing
    def serialize(self):
        ev_io = BytesIO()

        if self.catalog is not None:
            self.catalog[0].write(ev_io, format='QUAKEML')

        return msgpack.pack([self.waveform_stream, ev_io.getvalue()])
