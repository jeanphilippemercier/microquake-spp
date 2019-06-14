# f
from faust import Record

class PipelineMessage(Record):
    process_id: str
    module_type: str

# Class RawMsgPack(codecs.codecs):
#
#     def _dumps(self, obj: Any) -> bytes:
#      return msgpack.dumps(obj)
#
#
#     def _loads(self, s: bytes) -> Any:
#         return msgpack.loads(s)

