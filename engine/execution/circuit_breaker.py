"""External panic-file circuit breaker for immediate execution demotion.

The dependency surface is intentionally tiny so an operator or supervisor can
force paper-mode execution even when higher-level APIs or orchestration paths
are degraded.
"""

import os
from engine.execution.execution_mode import set_execution_mode, set_execution_armed

PANIC_FILE = os.environ.get("PANIC_FILE", "panic.flag")

def check_circuit_breaker():
    """Demote execution when the configured panic file is present.

    Returns
    -------
    bool
        ``True`` when ``PANIC_FILE`` exists and execution was forcibly disarmed;
        otherwise ``False``.

    Notes
    -----
    The check is intentionally file-based and fail-closed so a local panic
    signal can override more complex runtime controls.

    Side Effects
    ------------
    When tripped, writes execution control state by calling
    ``set_execution_armed(0)`` and ``set_execution_mode("paper")``.
    """
    # This is intentionally simple and externalized: a panic file can demote
    # execution immediately even if higher-level services are impaired.
    if os.path.exists(PANIC_FILE):
        set_execution_armed(0, actor="circuit_breaker", reason="panic_file")
        set_execution_mode("paper", actor="circuit_breaker", reason="panic_file")
        return True
    return False
