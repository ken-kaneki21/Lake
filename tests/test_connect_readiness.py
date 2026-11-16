import unittest
from unittest.mock import Mock
from urllib.error import HTTPError

from tests.connect_readiness import wait_for_connector


def response(connector, *tasks):
    return {"connector": {"state": connector}, "tasks": [{"state": state} for state in tasks]}


def http_error(code):
    return HTTPError("http://connect/status", code, "probe", {}, None)


class ConnectReadinessTest(unittest.TestCase):
    def test_status_can_trail_registration(self):
        fetch = Mock(side_effect=[http_error(404), response("RUNNING"), response("RUNNING", "RUNNING")])
        pause = Mock()
        self.assertEqual(
            wait_for_connector(fetch, "pg-app", attempts=3, pause=pause),
            {"connector": "RUNNING", "task_states": ["RUNNING"]},
        )
        self.assertEqual(pause.call_count, 2)

    def test_missing_status_is_bounded(self):
        fetch, pause = Mock(side_effect=http_error(404)), Mock()
        with self.assertRaisesRegex(TimeoutError, "after 2 attempts: HTTP 404"):
            wait_for_connector(fetch, "pg-app", attempts=2, pause=pause)
        self.assertEqual(fetch.call_count, 2)
        pause.assert_called_once_with(2)

    def test_permission_failure_stops_immediately(self):
        fetch, pause = Mock(side_effect=http_error(403)), Mock()
        with self.assertRaises(HTTPError) as raised:
            wait_for_connector(fetch, "pg-app", pause=pause)
        self.assertEqual(raised.exception.code, 403)
        fetch.assert_called_once()
        pause.assert_not_called()

    def test_terminal_connector_or_task_failure_stops_immediately(self):
        for status in (response("FAILED"), response("RESTARTING", "FAILED"), response("PAUSED")):
            with self.subTest(status=status):
                fetch, pause = Mock(return_value=status), Mock()
                with self.assertRaisesRegex(RuntimeError, "cannot start"):
                    wait_for_connector(fetch, "pg-app", pause=pause)
                fetch.assert_called_once()
                pause.assert_not_called()
