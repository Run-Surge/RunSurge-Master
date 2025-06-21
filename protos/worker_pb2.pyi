import common_pb2 as _common_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WorkerState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WORKER_STATE_UNSPECIFIED: _ClassVar[WorkerState]
    WORKER_STATE_IDLE: _ClassVar[WorkerState]
    WORKER_STATE_BUSY: _ClassVar[WorkerState]
    WORKER_STATE_INITIALIZING: _ClassVar[WorkerState]
    WORKER_STATE_SHUTTING_DOWN: _ClassVar[WorkerState]
WORKER_STATE_UNSPECIFIED: WorkerState
WORKER_STATE_IDLE: WorkerState
WORKER_STATE_BUSY: WorkerState
WORKER_STATE_INITIALIZING: WorkerState
WORKER_STATE_SHUTTING_DOWN: WorkerState

class WorkerStatus(_message.Message):
    __slots__ = ("state", "current_job_id")
    STATE_FIELD_NUMBER: _ClassVar[int]
    CURRENT_JOB_ID_FIELD_NUMBER: _ClassVar[int]
    state: WorkerState
    current_job_id: int
    def __init__(self, state: _Optional[_Union[WorkerState, str]] = ..., current_job_id: _Optional[int] = ...) -> None: ...

class OutputDataInfo(_message.Message):
    __slots__ = ("data_id", "data_name")
    DATA_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_NAME_FIELD_NUMBER: _ClassVar[int]
    data_id: int
    data_name: str
    def __init__(self, data_id: _Optional[int] = ..., data_name: _Optional[str] = ...) -> None: ...

class TaskAssignment(_message.Message):
    __slots__ = ("task_id", "python_file", "python_file_name", "required_data_ids", "output_data_infos", "job_id")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    PYTHON_FILE_FIELD_NUMBER: _ClassVar[int]
    PYTHON_FILE_NAME_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_DATA_IDS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_DATA_INFOS_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    python_file: bytes
    python_file_name: str
    required_data_ids: _containers.RepeatedScalarFieldContainer[int]
    output_data_infos: _containers.RepeatedCompositeFieldContainer[OutputDataInfo]
    job_id: int
    def __init__(self, task_id: _Optional[int] = ..., python_file: _Optional[bytes] = ..., python_file_name: _Optional[str] = ..., required_data_ids: _Optional[_Iterable[int]] = ..., output_data_infos: _Optional[_Iterable[_Union[OutputDataInfo, _Mapping]]] = ..., job_id: _Optional[int] = ...) -> None: ...

class DataUploadRequest(_message.Message):
    __slots__ = ("data_info", "chunk")
    DATA_INFO_FIELD_NUMBER: _ClassVar[int]
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    data_info: _common_pb2.DataInfo
    chunk: _common_pb2.DataChunk
    def __init__(self, data_info: _Optional[_Union[_common_pb2.DataInfo, _Mapping]] = ..., chunk: _Optional[_Union[_common_pb2.DataChunk, _Mapping]] = ...) -> None: ...

class DataNotification(_message.Message):
    __slots__ = ("task_id", "data_id", "data_name", "ip_address", "port", "hash")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_NAME_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    data_id: int
    data_name: str
    ip_address: str
    port: int
    hash: str
    def __init__(self, task_id: _Optional[int] = ..., data_id: _Optional[int] = ..., data_name: _Optional[str] = ..., ip_address: _Optional[str] = ..., port: _Optional[int] = ..., hash: _Optional[str] = ...) -> None: ...
