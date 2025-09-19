# models.py
from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Any

class JobStatus(Enum):
    """
    An enumeration for the status of a Job.
    Using an Enum prevents typos with strings and makes state transitions explicit.
    """
    QUEUED = auto()
    RUNNING = auto()
    CANCELLED = auto()
    COMPLETED = auto()
    COMPLETED_WITH_ERRORS = auto()
    POST_PROCESSING = auto()
    PROCESSED = auto() # Indicates post-processing is complete

@dataclass
class Job:
    """
    A structured data class representing a single job in the queue.
    Using a dataclass provides type hints, auto-generated __init__, and a
    single, clear definition for what constitutes a "Job".
    """
    # Core Attributes
    id: str = field(default_factory=lambda: f"Job_{uuid.uuid4().hex[:8]}")
    status: JobStatus = JobStatus.QUEUED

    # Job Type Specific Attributes
    job_type: str = "copy"  # "copy" or "mhl_verify"
    mhl_file: Optional[str] = None
    target_dir: Optional[str] = None
    
    # Copy Job Parameters
    sources: list[str] = field(default_factory=list)
    destinations: list[str] = field(default_factory=list)
    resolved_dests: dict[str, list[str]] = field(default_factory=dict)
    checksum_method: str = "xxHash (Fast)"
    verification_mode: str = "full"
    
    # Behavior Flags
    eject_on_completion: bool = False
    skip_existing: bool = True
    resume_partial: bool = True
    defer_post_process: bool = False

    # Data & Reporting
    metadata: dict[str, Any] = field(default_factory=dict)
    report: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Serializes the Job object to a dictionary for JSON storage."""
        state = self.__dict__.copy()
        state['status'] = self.status.name  # Store enum by its string name
        return state

    @staticmethod
    def from_dict(state: dict) -> Job:
        """Deserializes a dictionary back into a Job object."""
        try:
            state['status'] = JobStatus[state.get('status', 'QUEUED')]
        except KeyError:
            # Handle legacy or invalid status strings gracefully
            state['status'] = JobStatus.QUEUED
        
        # Create an empty Job instance and update its state
        job = Job()
        job.__dict__.update(state)
        return job