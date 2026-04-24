from enum import Enum

class ReadMethod(Enum):
    FULL_LOAD = "full_load"
    INCREMENTAL = "incremental"
    
class WriteMethod(Enum):
    APPEND = "append"
    FULL_LOAD = "full_load"
    INCREMENTAL = "incremental"
    