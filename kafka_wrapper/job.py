from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence, Union


@dataclass(frozen=True)
class Job:
    id: Optional[str] = None
    timestamp: Optional[int] = None
    topic: Optional[str] = None
    func: Optional[Callable[..., Any]] = None
    args: Optional[Sequence[Any]] = None
    kwargs: Optional[Dict[str, Any]] = None
    timeout: Optional[Union[float, int]] = None
    key: Optional[bytes] = None
    partition: Optional[Union[float, int]] = None
