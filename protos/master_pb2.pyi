from google.protobuf import empty_pb2 as _empty_pb2
import common_pb2 as _common_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class NodeUnregisterRequest(_message.Message):
    __slots__ = ("node_id",)
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    node_id: int
    def __init__(self, node_id: _Optional[int] = ...) -> None: ...

class NodeRegisterRequest(_message.Message):
    __slots__ = ("username", "password", "machine_fingerprint", "memory_bytes")
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    MACHINE_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    username: str
    password: str
    machine_fingerprint: str
    memory_bytes: int
    def __init__(self, username: _Optional[str] = ..., password: _Optional[str] = ..., machine_fingerprint: _Optional[str] = ..., memory_bytes: _Optional[int] = ...) -> None: ...

class JobCompleteRequest(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    def __init__(self, job_id: _Optional[int] = ...) -> None: ...
