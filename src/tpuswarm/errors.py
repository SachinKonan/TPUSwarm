"""TPUSwarm exceptions."""


class SwarmError(RuntimeError):
    """Base class for TPUSwarm failures."""


class TaskNotFoundError(SwarmError):
    pass


class WorkflowNotFoundError(SwarmError):
    pass


class BarrierConflictError(SwarmError):
    pass


class LeadershipLostError(SwarmError):
    pass


class BackendUnavailableError(SwarmError):
    pass
