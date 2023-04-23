from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Message:
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: bytes
