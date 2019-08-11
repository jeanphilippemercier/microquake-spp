from typing import List

from uplink import Body, Consumer, Part, get, multipart, post, response_handler, put
from uplink.auth import ApiTokenHeader

from microquake.core.settings import settings
# Local imports
from .schemas import Cable, CableSchema, Event, EventSchema, Ray, RaySchema


# @headers({"Accept": "application/vnd.seismicplatform.v1.full+json"})
class SeismicClient(Consumer):
    def __init__(self, access_token, base_url=None):
        if base_url is None:
            base_url = settings.API_BASE_URL

        token_auth = ApiTokenHeader("Authorization", access_token)
        super(SeismicClient, self).__init__(base_url=base_url, auth=token_auth)

    def raise_for_status(response):
        """Checks whether or not the response was successful."""

        if 200 <= response.status_code < 300:
            # Pass through the response.

            return response

            raise Exception(response.url)

    @response_handler(raise_for_status)
    @get("/events")
    def get_events(self) -> EventSchema(many=True):
        """Lists all events."""

    @get("cables")
    def get_cables(self) -> CableSchema(many=True):
        """Lists all cables."""

    @post("rays", args={"rays": Body})
    def post_rays(self, rays: List[Ray]) -> RaySchema(many=True):
        """post_ray."""

    @post("cables", args={"cables": Body})
    def post_cables(self, cables: List[Cable]) -> CableSchema(many=True):
        """post_cable."""

    @post("events", args={"events": Body})
    def post_events(selfs, events: List[Event]) -> EventSchema(many=True):
        """post_event"""

    @multipart
    @put("weightsfiles")
    def upload_weigths(self, weigths: Part): pass
