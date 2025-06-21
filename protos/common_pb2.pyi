from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class DataType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DATA_TYPE_UNSPECIFIED: _ClassVar[DataType]
    DATA_TYPE_FILE: _ClassVar[DataType]
    DATA_TYPE_PYTHON_PICKLE: _ClassVar[DataType]
DATA_TYPE_UNSPECIFIED: DataType
DATA_TYPE_FILE: DataType
DATA_TYPE_PYTHON_PICKLE: DataType

class StatusResponse(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class DataIdentifier(_message.Message):
    __slots__ = ("data_id",)
    DATA_ID_FIELD_NUMBER: _ClassVar[int]
    data_id: int
    def __init__(self, data_id: _Optional[int] = ...) -> None: ...

class FileIdentifier(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class DataMetadata(_message.Message):
    __slots__ = ("data_id", "data_name", "total_size_bytes", "ip_address", "port", "hash", "is_on_master", "task_id")
    DATA_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_NAME_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    IS_ON_MASTER_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    data_id: int
    data_name: str
    total_size_bytes: int
    ip_address: str
    port: int
    hash: str
    is_on_master: bool
    task_id: int
    def __init__(self, data_id: _Optional[int] = ..., data_name: _Optional[str] = ..., total_size_bytes: _Optional[int] = ..., ip_address: _Optional[str] = ..., port: _Optional[int] = ..., hash: _Optional[str] = ..., is_on_master: bool = ..., task_id: _Optional[int] = ...) -> None: ...

class DataChunk(_message.Message):
    __slots__ = ("chunk_data", "is_last_chunk")
    CHUNK_DATA_FIELD_NUMBER: _ClassVar[int]
    IS_LAST_CHUNK_FIELD_NUMBER: _ClassVar[int]
    chunk_data: bytes
    is_last_chunk: bool
    def __init__(self, chunk_data: _Optional[bytes] = ..., is_last_chunk: bool = ...) -> None: ...

class DataInfo(_message.Message):
    __slots__ = ("data_id", "task_id", "file_name", "size", "hash")
    DATA_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_NAME_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    data_id: int
    task_id: int
    file_name: str
    size: str
    hash: str
    def __init__(self, data_id: _Optional[int] = ..., task_id: _Optional[int] = ..., file_name: _Optional[str] = ..., size: _Optional[str] = ..., hash: _Optional[str] = ...) -> None: ...
