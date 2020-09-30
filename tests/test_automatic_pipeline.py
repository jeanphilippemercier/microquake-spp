from spp.clients.api_client import get_event_by_id
from microquake.core.settings import settings
from spp.pipelines.automatic_pipeline import automatic_pipeline


def test_automatic_pipeline():
    event_id = 'smi:local/2020/03/04/23/29_32_096747140.e'
    api_url = settings.get('api_base_url')
    re = get_event_by_id(api_url, event_id)
    cat = re.get_event()
    stream = re.get_waveforms()
    automatic_pipeline(cat, stream)


if __name__ == '__main__':
    test_automatic_pipeline()
