"""Bounded Kafka Connect readiness checks used by the Docker integration probe."""

import time
from urllib.error import HTTPError


def wait_for_connector(fetch_status, name, *, attempts=75, pause=time.sleep):
    if attempts < 1:
        raise ValueError("attempts must be positive")
    last = "no status received"
    for index in range(attempts):
        try:
            status = fetch_status()
        except HTTPError as error:
            if error.code != 404:
                raise
            last = "HTTP 404 from status endpoint"
        else:
            connector_state = status["connector"]["state"]
            task_states = [task["state"] for task in status["tasks"]]
            last = f"connector={connector_state}, tasks={task_states}"
            if connector_state in {"FAILED", "PAUSED", "STOPPED"} or any(
                state in {"FAILED", "PAUSED", "STOPPED"} for state in task_states
            ):
                raise RuntimeError(f"Kafka Connect {name} cannot start: {last}")
            if connector_state == "RUNNING" and task_states and all(
                state == "RUNNING" for state in task_states
            ):
                return {"connector": connector_state, "task_states": task_states}
        if index + 1 < attempts:
            pause(2)
    raise TimeoutError(f"Kafka Connect {name} not ready after {attempts} attempts: {last}")
