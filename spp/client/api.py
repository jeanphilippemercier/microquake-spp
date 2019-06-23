import typing

from uplink import Consumer, get, headers, response_handler, returns, json
from uplink.auth import ApiTokenHeader

from ..core.settings import settings
# Local imports
from .schemas import Cable, CableSchema, EventSchema


# @headers({"Accept": "application/vnd.seismicplatform.v1.full+json"})
# @returns.json
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

    # @response_handler(raise_for_status)
    # @get("/events")
    # def get_events(self) -> EventSchema(many=True):
    #     """Lists all events."""

    # def get_cables(self) -> typing.List[Cable]:
        # """Lists all cables."""
    # @json
    @get("cables")
    def get_cables(self) -> CableSchema(many=True):
        """Lists all cables."""
