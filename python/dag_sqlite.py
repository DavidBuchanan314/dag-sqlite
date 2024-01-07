from typing import Dict, Any
from enum import Enum

class DsTypes(Enum):
	NULL = 0
	BOOLEAN = 1
	INTEGER = 2
	NEGATIVE_INTEGER = 3
	STRING = 4
	BYTES = 5
	LIST = 6
	MAP = 7
	LINK = 8

DsObj = None | bool | int | str | bytes | bytearray | list | Dict[str, Any]