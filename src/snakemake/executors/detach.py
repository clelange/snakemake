from typing import Protocol

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo


class DetachExecutor(Protocol):
    """Experimental executor hooks required by detach/attach."""

    def detach(self) -> None: ...

    def attach_job(self, job_info: SubmittedJobInfo) -> None: ...


def supports_detach_attach(executor: object) -> bool:
    return all(
        callable(getattr(executor, method, None)) for method in ("detach", "attach_job")
    )
