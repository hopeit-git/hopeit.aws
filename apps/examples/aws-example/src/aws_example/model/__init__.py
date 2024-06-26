"""
Data model for aws-example test application
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional

from hopeit.dataobjects import dataclass, dataobject, field


class StatusType(str, Enum):
    NEW = "NEW"
    LOADED = "LOADED"
    SUBMITTED = "SUBMITTED"
    PROCESSED = "PROCESSED"


@dataobject
@dataclass
class Status:
    """Status change"""

    ts: datetime
    type: StatusType


@dataobject
@dataclass
class User:
    """User information"""

    id: str
    name: str


@dataobject(event_ts="status.ts")
@dataclass
class Something:
    """Example Something event"""

    id: str
    user: User
    status: Optional[Status] = None
    history: List[Status] = field(default_factory=list)


@dataobject
@dataclass
class SomethingParams:
    """Params to create and save Something"""

    id: str
    user: str


@dataobject
@dataclass
class SomethingNotFound:
    """Item not found in datastore"""

    path: str
    id: str
