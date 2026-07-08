import threading
from types import SimpleNamespace

import pytest

from snakemake.scheduling.job_scheduler import JobScheduler, ScheduleResult


class DummyExecutor:
    def __init__(self):
        self.shutdown_called = False

    def shutdown(self):
        self.shutdown_called = True


@pytest.mark.parametrize(
    ("has_errors", "expected"),
    [
        (False, ScheduleResult.SUCCESS),
        (True, ScheduleResult.FAILED),
    ],
)
def test_scheduler_returns_explicit_result(has_errors, expected):
    dag = SimpleNamespace(
        queue_input_jobs=False,
        ready_jobs=(),
        has_unfinished_queue_input_jobs=lambda: False,
        needrun_jobs=lambda: (),
        finished=lambda job: False,
    )
    scheduler = JobScheduler.__new__(JobScheduler)
    scheduler.workflow = SimpleNamespace(
        dag=dag,
        remote_execution_settings=SimpleNamespace(immediate_submit=False),
    )
    scheduler._open_jobs = threading.Semaphore(1)
    scheduler._lock = threading.Lock()
    scheduler._finish_jobs = lambda: None
    scheduler._error_jobs = lambda: None
    scheduler.running = set()
    scheduler.failed = set()
    scheduler._errors = has_errors
    scheduler._executor_error = None
    scheduler._user_kill = None
    scheduler.keepgoing = False
    scheduler._executor = DummyExecutor()

    result = scheduler.schedule()

    assert result is expected
    assert scheduler._executor.shutdown_called
