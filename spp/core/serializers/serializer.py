from ..settings import settings


class Serializer():
    def __init__(self, msg_in):
        self.message = msg_in
        self.deserialize()

    @property
    def eventid(self):
        return str(self.catalog[0].resource_id)

    @property
    def waveform(self):
        return self.clean_waveform_stream()

    @property
    def catalog(self):
        return self.catalog.copy()

    def clean_waveform_stream(self):
        black_list = settings.sensors.black_list
        waveforms = self.waveform_stream.copy()

        if black_list is None:
            return

        for trace in waveforms:
            if trace.stats.station in black_list:
                waveforms.remove(trace)

        return waveforms
