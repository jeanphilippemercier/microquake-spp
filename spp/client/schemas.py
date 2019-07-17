import collections

import marshmallow
from marshmallow import Schema, post_load

Event = collections.namedtuple(
    "Event", field_names=["event_type"]
)

cable_fields = ["id", "code", "r", "l", "g", "c"]

Cable = collections.namedtuple(
    "Cable", field_names=cable_fields
)

ray_fields = ["id", "code", "r", "l", "g", "c"]

Ray = collections.namedtuple(
    "Ray", field_names=ray_fields
)


class SchemaBase(Schema):
    class Meta:
        # Pass EXCLUDE as Meta option to keep marshmallow 2 behavior
        # ref: https://marshmallow.readthedocs.io/en/3.0/upgrading.html
        unknown = getattr(marshmallow, "EXCLUDE", None)


class EventSchema(SchemaBase):
    @post_load
    def make_event(self, data):
        return Event(**data)


class CableSchema(SchemaBase):
    class Meta:
        fields = cable_fields

    @post_load
    def make_cable(self, data):
        return Cable(*data)


class RaySchema(SchemaBase):
    class Meta:
        fields = ray_fields

    @post_load
    def make_cable(self, data):
        return Ray(*data)
