from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Options:
    send_interval: float = 1.0
    edit_interval: float = 1.0
    delete_interval: float = 1.0
    action_interval: float = 5.0
    typing_enabled: bool = True
    backoff_base: float = 1.0
    backoff_max: float = 30.0
    backoff_jitter: float = 0.2
    max_attempts: int = 5
    stream_ttl: float = 5.0
    machine_ttl: float = 30.0
    shutdown_timeout: float = 10.0
    raise_on_failure: bool = True
    timings_capacity: int = 1024
