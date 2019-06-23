import collections

import marshmallow

Event = collections.namedtuple(
    "Event", field_names=["event_type"]
)

# "Cable", ["id"]
Cable = collections.namedtuple(
    "Cable", field_names=["id", "code", "r", "l", "g", "c"]
)


class SchemaBase(marshmallow.Schema):
    class Meta:
        # Pass EXCLUDE as Meta option to keep marshmallow 2 behavior
        # ref: https://marshmallow.readthedocs.io/en/3.0/upgrading.html
        unknown = getattr(marshmallow, "EXCLUDE", None)


class EventSchema(SchemaBase):
    @marshmallow.post_load
    def make_event(self, data):
        return Event(**data)


class CableSchema(SchemaBase):
    @marshmallow.post_load
    def make_cable(self, data):
        print(data)
        exit(0)
        return Cable(*data)
