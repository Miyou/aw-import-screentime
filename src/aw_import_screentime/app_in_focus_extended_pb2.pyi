from typing import Any

from google.protobuf.message import Message


class AppInFocusEvent(Message):
    transition_reason: str
    kind: int
    in_foreground: int
    cf_absolute_time: float
    bundle_id: str
    app_version: str
    app_build: str
    platform_flag: int

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def ParseFromString(self, serialized: bytes) -> int: ...
