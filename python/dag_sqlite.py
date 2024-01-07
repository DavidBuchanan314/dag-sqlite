from typing import Dict, Any
from enum import Enum

class DsTypes(Enum):
	NULL = 0
	BOOLEAN = 1
	INTEGER = 2
	STRING = 3
	BYTES = 4
	LIST = 5
	MAP = 6
	LINK = 7

DsObj = None | bool | int | str | bytes | bytearray | list | Dict[str, Any]