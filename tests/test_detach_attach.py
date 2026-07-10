import json
from contextlib import contextmanager
from pathlib import Path

import pytest

from snakemake import api
from snakemake.dag import DAG
from snakemake.persistence import DetachedRunState
from snakemake.persistence.file import FilePersistence
from snakemake.settings import types as settings
from snakemake_interface_common.exceptions import ApiError, WorkflowError
from snakemake_interface_executor_plugins.executors.base import (
    AbstractExecutor,
    SubmittedJobInfo,
)
from snakemake_interface_executor_plugins.registry import ExecutorPluginRegistry
from snakemake_interface_executor_plugins.registry.plugin import Plugin
from snakemake_interface_executor_plugins.settings import CommonSettings

FAKE_EXECUTOR = "fake-detach"
UNSUPPORTED_EXECUTOR = "fake-no-detach"


class FakeDetachExecutor(AbstractExecutor):
    submitted = []
    attached = []
    cancelled = False
    detach_calls = 0
    shutdown_called = False
    attach_behavior = "success"
    persist_jobids = True
    detach_raises = False

    @classmethod
    def reset(cls):
        cls.submitted = []
        cls.attached = []
        cls.cancelled = False
        cls.detach_calls = 0
        cls.shutdown_called = False
        cls.attach_behavior = "success"
        cls.persist_jobids = True
        cls.detach_raises = False

    @staticmethod
    def write_outputs(job):
        jobs = job.jobs if job.is_group() else (job,)
        for member in jobs:
            for output in member.output:
                path = Path(str(output))
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("completed\n")

    def run_job(self, job):
        external_jobid = f"external-{job.jobid}"
        self.__class__.submitted.append(external_jobid)
        if self.__class__.persist_jobids:
            job.register(external_jobid=external_jobid)
        job_info = SubmittedJobInfo(job=job, external_jobid=external_jobid)
        self.report_job_submission(job_info)
        if self.workflow.execution_settings.attach:
            self.write_outputs(job)
            self.report_job_success(job_info)

    def attach_job(self, job_info: SubmittedJobInfo):
        if self.__class__.attach_behavior == "raise" or (
            self.__class__.attach_behavior == "raise_after_first"
            and self.__class__.attached
        ):
            raise WorkflowError("simulated attach setup failure")
        self.__class__.attached.append(job_info.external_jobid)
        if self.__class__.attach_behavior == "failed":
            self.report_job_error(job_info, msg="simulated external failure")
        else:
            self.write_outputs(job_info.job)
            self.report_job_success(job_info)

    def detach(self):
        self.__class__.detach_calls += 1
        if self.__class__.detach_raises:
            raise WorkflowError("simulated detach failure")

    def shutdown(self):
        self.__class__.shutdown_called = True

    def cancel(self):
        self.__class__.cancelled = True

    def handle_job_success(self, job):
        pass

    def handle_job_error(self, job):
        pass


class UnsupportedDetachExecutor(AbstractExecutor):
    def run_job(self, job):
        pass

    def shutdown(self):
        pass

    def cancel(self):
        pass

    def handle_job_success(self, job):
        pass

    def handle_job_error(self, job):
        pass


@contextmanager
def fake_executor_plugin():
    registry = ExecutorPluginRegistry()
    old_plugin = registry.plugins.get(FAKE_EXECUTOR)
    registry.plugins[FAKE_EXECUTOR] = Plugin(
        _name=FAKE_EXECUTOR,
        executor=FakeDetachExecutor,
        common_settings=CommonSettings(
            non_local_exec=True,
            implies_no_shared_fs=False,
            job_deploy_sources=False,
            auto_deploy_default_storage_provider=False,
        ),
        _executor_settings_cls=None,
    )
    try:
        yield
    finally:
        if old_plugin is None:
            registry.plugins.pop(FAKE_EXECUTOR, None)
        else:
            registry.plugins[FAKE_EXECUTOR] = old_plugin


@contextmanager
def unsupported_executor_plugin():
    registry = ExecutorPluginRegistry()
    old_plugin = registry.plugins.get(UNSUPPORTED_EXECUTOR)
    registry.plugins[UNSUPPORTED_EXECUTOR] = Plugin(
        _name=UNSUPPORTED_EXECUTOR,
        executor=UnsupportedDetachExecutor,
        common_settings=CommonSettings(
            non_local_exec=True,
            implies_no_shared_fs=False,
            job_deploy_sources=False,
            auto_deploy_default_storage_provider=False,
        ),
        _executor_settings_cls=None,
    )
    try:
        yield
    finally:
        if old_plugin is None:
            registry.plugins.pop(UNSUPPORTED_EXECUTOR, None)
        else:
            registry.plugins[UNSUPPORTED_EXECUTOR] = old_plugin


def write_snakefile(workdir: Path):
    (workdir / "Snakefile").write_text("""
rule all:
    output:
        "out.txt"
    shell:
            "echo should-not-run > {output}"
""")


def execute(
    workdir: Path,
    *,
    detach: bool = False,
    attach: bool = False,
    lock: bool = True,
    ignore_incomplete: bool = False,
    force_incomplete: bool = False,
    executor: str = FAKE_EXECUTOR,
    nodes: int = 1,
):
    with api.SnakemakeApi() as snakemake_api:
        workflow_api = snakemake_api.workflow(
            resource_settings=settings.ResourceSettings(nodes=nodes),
            snakefile=workdir / "Snakefile",
            workdir=workdir,
        )
        dag_api = workflow_api.dag(
            settings.DAGSettings(force_incomplete=force_incomplete)
        )
        dag_api.execute_workflow(
            executor=executor,
            execution_settings=settings.ExecutionSettings(
                detach=detach,
                attach=attach,
                lock=lock,
                ignore_incomplete=ignore_incomplete,
            ),
        )


def detached_state(workdir: Path):
    record = json.loads((workdir / ".snakemake" / "detached-run.json").read_text())
    return DetachedRunState(record["state"])


def test_detach_persists_external_job_and_does_not_cancel(tmp_path):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)

    assert FakeDetachExecutor.submitted == ["external-0"]
    assert FakeDetachExecutor.detach_calls == 1
    assert not FakeDetachExecutor.cancelled
    assert (tmp_path / ".snakemake" / "detached-run.json").exists()
    assert detached_state(tmp_path) is DetachedRunState.DETACHED
    assert not (tmp_path / "out.txt").exists()


def test_attach_adopts_external_job_without_resubmitting(tmp_path):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        execute(tmp_path, attach=True)

    assert FakeDetachExecutor.submitted == ["external-0"]
    assert FakeDetachExecutor.attached == ["external-0"]
    assert (tmp_path / "out.txt").read_text() == "completed\n"
    assert not (tmp_path / ".snakemake" / "detached-run.json").exists()


def test_detach_rejects_local_executor(tmp_path):
    write_snakefile(tmp_path)

    with api.SnakemakeApi() as snakemake_api:
        workflow_api = snakemake_api.workflow(
            resource_settings=settings.ResourceSettings(cores=1),
            snakefile=tmp_path / "Snakefile",
            workdir=tmp_path,
        )
        dag_api = workflow_api.dag()
        with pytest.raises(ApiError, match="non-local executor"):
            dag_api.execute_workflow(
                execution_settings=settings.ExecutionSettings(detach=True)
            )


def test_detach_rejects_executor_without_both_hooks(tmp_path):
    write_snakefile(tmp_path)

    with (
        unsupported_executor_plugin(),
        pytest.raises(ApiError, match="does not support experimental detach/attach"),
    ):
        execute(tmp_path, detach=True, executor=UNSUPPORTED_EXECUTOR)


def test_attach_rejects_missing_session(tmp_path):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()

    with (
        fake_executor_plugin(),
        pytest.raises(WorkflowError, match="No detached Snakemake execution"),
    ):
        execute(tmp_path, attach=True)


def test_attach_adopts_job_that_completed_while_detached(tmp_path):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        (tmp_path / "out.txt").write_text("completed externally\n")
        execute(tmp_path, attach=True)

    assert len(FakeDetachExecutor.submitted) == 1
    assert len(FakeDetachExecutor.attached) == 1
    assert not (tmp_path / ".snakemake" / "detached-run.json").exists()


def test_attach_adopts_group_as_one_execution_unit(tmp_path):
    (tmp_path / "Snakefile").write_text("""
rule b:
    input: "a.txt"
    output: "b.txt"
    group: "g"
    shell: "cat {input} > {output}"

rule a:
    output: "a.txt"
    group: "g"
    shell: "echo a > {output}"
""")
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        execute(tmp_path, attach=True)

    assert len(FakeDetachExecutor.submitted) == 1
    assert len(FakeDetachExecutor.attached) == 1
    assert (tmp_path / "a.txt").exists()
    assert (tmp_path / "b.txt").exists()


def test_detach_defers_conventional_local_target_rule(tmp_path):
    (tmp_path / "Snakefile").write_text("""
rule all:
    input: "remote.txt"

rule remote:
    output: "remote.txt"
    shell: "echo should-not-run > {output}"
""")
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        execute(tmp_path, attach=True)

    assert len(FakeDetachExecutor.submitted) == 1
    assert len(FakeDetachExecutor.attached) == 1
    assert (tmp_path / "remote.txt").exists()


def test_detach_defers_independent_local_job(tmp_path):
    (tmp_path / "Snakefile").write_text("""
localrules: local_job

rule all:
    input: "remote.txt", "local.txt"

rule remote:
    output: "remote.txt"
    shell: "echo should-not-run > {output}"

rule local_job:
    output: "local.txt"
    shell: "echo local > {output}"
""")
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        assert not (tmp_path / "local.txt").exists()
        execute(tmp_path, attach=True)

    assert (tmp_path / "remote.txt").exists()
    assert (tmp_path / "local.txt").read_text().strip() == "local"


def test_detach_rejects_when_only_local_jobs_are_ready(tmp_path):
    (tmp_path / "Snakefile").write_text("""
localrules: local_job

rule local_job:
    output: "local.txt"
    shell: "echo local > {output}"
""")
    FakeDetachExecutor.reset()

    with (
        fake_executor_plugin(),
        pytest.raises(WorkflowError, match="No runnable non-local jobs"),
    ):
        execute(tmp_path, detach=True)

    assert not (tmp_path / ".snakemake" / "detached-run.json").exists()


def test_attach_completes_jobs_not_submitted_before_detach(tmp_path):
    (tmp_path / "Snakefile").write_text("""
rule all:
    input: "one.txt", "two.txt"

rule one:
    output: "one.txt"
    shell: "echo should-not-run > {output}"

rule two:
    output: "two.txt"
    shell: "echo should-not-run > {output}"
""")
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True, nodes=1)
        assert len(FakeDetachExecutor.submitted) == 1
        record_path = tmp_path / ".snakemake" / "detached-run.json"
        record = json.loads(record_path.read_text())
        record["state"] = DetachedRunState.DETACHING.value
        record_path.write_text(json.dumps(record))
        execute(tmp_path, attach=True, nodes=1)

    assert len(FakeDetachExecutor.submitted) == 2
    assert len(FakeDetachExecutor.attached) == 1
    assert (tmp_path / "one.txt").exists()
    assert (tmp_path / "two.txt").exists()


def test_attach_failure_persists_terminal_state(tmp_path):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        FakeDetachExecutor.attach_behavior = "failed"
        with pytest.raises(WorkflowError, match="did not complete successfully"):
            execute(tmp_path, attach=True)
        assert detached_state(tmp_path) is DetachedRunState.FAILED
        with pytest.raises(WorkflowError, match="has already failed"):
            execute(tmp_path, attach=True)

        FakeDetachExecutor.attach_behavior = "success"
        execute(tmp_path, detach=True, force_incomplete=True)
        assert detached_state(tmp_path) is DetachedRunState.DETACHED


def test_attach_setup_failure_leaves_jobs_retryable(tmp_path):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        FakeDetachExecutor.attach_behavior = "raise"
        with pytest.raises(WorkflowError, match="left running"):
            execute(tmp_path, attach=True)

    assert detached_state(tmp_path) is DetachedRunState.DETACHED
    assert FakeDetachExecutor.detach_calls == 2
    assert not FakeDetachExecutor.cancelled


def test_partial_attach_setup_failure_can_be_retried(tmp_path):
    (tmp_path / "Snakefile").write_text("""
rule all:
    input: "one.txt", "two.txt"

rule one:
    output: "one.txt"
    shell: "echo should-not-run > {output}"

rule two:
    output: "two.txt"
    shell: "echo should-not-run > {output}"
""")
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True, nodes=2)
        FakeDetachExecutor.attach_behavior = "raise_after_first"
        with pytest.raises(WorkflowError, match="left running"):
            execute(tmp_path, attach=True, nodes=2)
        assert len(FakeDetachExecutor.attached) == 1
        assert detached_state(tmp_path) is DetachedRunState.DETACHED

        FakeDetachExecutor.attach_behavior = "success"
        execute(tmp_path, attach=True, nodes=2)

    assert len(FakeDetachExecutor.submitted) == 2
    assert not FakeDetachExecutor.cancelled
    assert not (tmp_path / ".snakemake" / "detached-run.json").exists()


def test_detach_missing_external_id_cancels_and_remains_recoverable(tmp_path):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()
    FakeDetachExecutor.persist_jobids = False

    with (
        fake_executor_plugin(),
        pytest.raises(WorkflowError, match="did not persist external job IDs"),
    ):
        execute(tmp_path, detach=True)

    assert FakeDetachExecutor.cancelled
    assert detached_state(tmp_path) is DetachedRunState.DETACHING


def test_detach_hook_failure_cancels_and_remains_recoverable(tmp_path):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()
    FakeDetachExecutor.detach_raises = True

    with (
        fake_executor_plugin(),
        pytest.raises(WorkflowError, match="simulated detach failure"),
    ):
        execute(tmp_path, detach=True)

    assert FakeDetachExecutor.cancelled
    assert detached_state(tmp_path) is DetachedRunState.DETACHING


def test_attach_rejects_executor_mismatch(tmp_path):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        record_path = tmp_path / ".snakemake" / "detached-run.json"
        record = json.loads(record_path.read_text())
        record["executor"] = "different-executor"
        record_path.write_text(json.dumps(record))
        with pytest.raises(WorkflowError, match="current executor"):
            execute(tmp_path, attach=True)


def test_attach_rejects_inconsistent_group_external_ids(tmp_path):
    (tmp_path / "Snakefile").write_text("""
rule b:
    input: "a.txt"
    output: "b.txt"
    group: "g"
    shell: "cat {input} > {output}"

rule a:
    output: "a.txt"
    group: "g"
    shell: "echo a > {output}"
""")
    FakeDetachExecutor.reset()

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        persistence = FilePersistence(dag=None, path=tmp_path / ".snakemake")
        persistence._mark_incomplete("a.txt", "different-external-id")
        with pytest.raises(WorkflowError, match="Multiple different external jobids"):
            execute(tmp_path, attach=True)


def test_attach_clears_session_before_workdir_cleanup(tmp_path, monkeypatch):
    write_snakefile(tmp_path)
    FakeDetachExecutor.reset()
    cleanup_records = []
    original_cleanup = DAG.cleanup_workdir

    def cleanup_workdir(dag):
        cleanup_records.append(dag.workflow.persistence.detached_run())
        original_cleanup(dag)

    monkeypatch.setattr(DAG, "cleanup_workdir", cleanup_workdir)

    with fake_executor_plugin():
        execute(tmp_path, detach=True)
        execute(tmp_path, attach=True)

    assert cleanup_records == [None]


@pytest.mark.parametrize(
    "execution_kwargs, message",
    [
        ({"lock": False}, "require workflow directory locking"),
        ({"ignore_incomplete": True}, "cannot be combined"),
        ({"attach": True, "ignore_incomplete": True}, "cannot be combined"),
        ({"attach": True, "force_incomplete": True}, "rerun-incomplete"),
    ],
)
def test_detach_attach_rejects_incompatible_settings(
    tmp_path, execution_kwargs, message
):
    write_snakefile(tmp_path)

    with fake_executor_plugin(), pytest.raises(ApiError, match=message):
        execute(
            tmp_path,
            detach=not execution_kwargs.get("attach", False),
            **execution_kwargs,
        )


def test_detach_and_attach_are_mutually_exclusive(tmp_path):
    write_snakefile(tmp_path)

    with fake_executor_plugin(), pytest.raises(ApiError, match="at the same time"):
        execute(tmp_path, detach=True, attach=True)
