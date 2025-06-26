from enum import Enum

class Status(Enum):
    IN_PROGRESS = "in_progress"
    SAVING = "saving"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"
