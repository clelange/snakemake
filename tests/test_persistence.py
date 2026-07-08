import json
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snakemake.persistence import DetachedRunState
from snakemake.persistence.db import DbPersistence
from snakemake.persistence.file import FilePersistence
from snakemake_interface_common.exceptions import WorkflowError


class TestCleanupContainers:
    @patch("snakemake.deployment.singularity.Image", autospec=True, create=True)
    def test_one_unrequired_container_gets_removed(self, mock_img):
        snakecode = """rule all:
    input:
        "foo.txt"

rule foo:
    output:
        "foo.txt"
    container:
        "docker://quay.io/mbhall88/rasusa:0.7.0"
    shell:
        "rasusa --help &> {output}"
"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmpdirpath = Path(tmpdirname)
            singularity_dir = tmpdirpath / ".snakemake/singularity"
            singularity_dir.mkdir(parents=True)
            snakefile = tmpdirpath / "Snakefile"
            snakefile.write_text(snakecode)

            unrequired_img_path = (
                singularity_dir / "32077ccc4d05977ef8b94ee6f74073fd.simg"
            )
            unrequired_img_path.touch()
            assert unrequired_img_path.exists()

            required_img_path = (
                singularity_dir / "0581af3d3099c1cc8cf0088c8efe1439.simg"
            )
            required_img_path.touch()
            assert required_img_path.exists()
            mock_img.path = str(required_img_path)
            container_imgs = {"docker://quay.io/mbhall88/rasusa:0.7.0": mock_img}
            mock_dag = MagicMock()
            mock_dag.container_imgs = container_imgs
            persistence = FilePersistence(dag=mock_dag)
            persistence.container_img_path = str(singularity_dir)

            persistence.cleanup_containers()

            assert required_img_path.exists()
            assert not unrequired_img_path.exists()


class TestDropIOCache:
    def test_drop_iocache_missing_file_is_noop(self):
        """Regression for race when parallel workers concurrently drop the iocache.

        The previous check-then-remove pattern raised FileNotFoundError if a
        sibling worker deleted the file between os.path.exists() and os.remove().
        drop_iocache() must tolerate the file already being gone.
        """
        with tempfile.TemporaryDirectory() as tmpdirname:
            persistence = FilePersistence(
                dag=MagicMock(), path=Path(tmpdirname) / ".snakemake"
            )
            # iocache file does not exist; drop_iocache must not raise.
            assert not Path(persistence._iocache_filename).exists()
            persistence.drop_iocache()

    def test_drop_iocache_removes_existing_file(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            persistence = FilePersistence(
                dag=MagicMock(), path=Path(tmpdirname) / ".snakemake"
            )
            iocache_file = Path(persistence._iocache_filename)
            iocache_file.write_bytes(b"placeholder")
            assert iocache_file.exists()

            persistence.drop_iocache()
            assert not iocache_file.exists()


@pytest.mark.parametrize("persistence_cls", [FilePersistence, DbPersistence])
class TestDetachedRunPersistence:
    def test_create_read_and_delete_detached_run(self, persistence_cls):
        with tempfile.TemporaryDirectory() as tmpdirname:
            persistence = persistence_cls(
                dag=None, path=Path(tmpdirname) / ".snakemake"
            )

            record = persistence.begin_detached_run(
                executor="cluster-generic", message="waiting for attach"
            )

            assert record.state is DetachedRunState.DETACHING
            assert record.executor == "cluster-generic"
            assert record.namespace == str(persistence.path.absolute())
            assert record.message == "waiting for attach"
            assert record.created_at == record.updated_at

            persisted = persistence.detached_run()
            assert persisted is not None
            assert persisted.executor == "cluster-generic"
            assert persisted.message == "waiting for attach"

            detached = persistence.transition_detached_run(
                DetachedRunState.DETACHED,
                message="ready for attach",
                expected_state=DetachedRunState.DETACHING,
            )
            assert detached.state is DetachedRunState.DETACHED
            assert detached.message == "ready for attach"
            assert detached.updated_at >= detached.created_at

            assert persistence.delete_detached_run()
            assert persistence.detached_run() is None
            assert not persistence.delete_detached_run()

    def test_create_detached_run_rejects_existing_active_record(self, persistence_cls):
        with tempfile.TemporaryDirectory() as tmpdirname:
            persistence = persistence_cls(
                dag=None, path=Path(tmpdirname) / ".snakemake"
            )
            persistence.begin_detached_run(executor="cluster-generic")

            with pytest.raises(WorkflowError, match="already registered"):
                persistence.begin_detached_run(executor="cluster-generic")

    def test_failed_record_can_be_replaced(self, persistence_cls):
        with tempfile.TemporaryDirectory() as tmpdirname:
            persistence = persistence_cls(
                dag=None, path=Path(tmpdirname) / ".snakemake"
            )
            persistence.begin_detached_run(executor="cluster-generic")
            persistence.transition_detached_run(
                DetachedRunState.FAILED,
                message="external job failed",
                expected_state=DetachedRunState.DETACHING,
            )

            replacement = persistence.begin_detached_run(executor="slurm")

            assert replacement.state is DetachedRunState.DETACHING
            assert replacement.executor == "slurm"
            assert replacement.message is None

    def test_transition_rejects_unexpected_state(self, persistence_cls):
        with tempfile.TemporaryDirectory() as tmpdirname:
            persistence = persistence_cls(
                dag=None, path=Path(tmpdirname) / ".snakemake"
            )
            persistence.begin_detached_run(executor="cluster-generic")

            with pytest.raises(WorkflowError, match="expected 'detached'"):
                persistence.transition_detached_run(
                    DetachedRunState.FAILED,
                    expected_state=DetachedRunState.DETACHED,
                )

    def test_concurrent_begin_has_one_winner(self, persistence_cls):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = Path(tmpdirname) / ".snakemake"
            persistence = [persistence_cls(dag=None, path=path) for _ in range(2)]
            barrier = threading.Barrier(2)

            def begin(index):
                barrier.wait()
                try:
                    persistence[index].begin_detached_run(executor=f"executor-{index}")
                    return True
                except WorkflowError:
                    return False

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(begin, range(2)))

            assert sorted(results) == [False, True]


def test_file_detached_run_rejects_corrupted_record(tmp_path):
    persistence = FilePersistence(dag=None, path=tmp_path / ".snakemake")
    Path(persistence._detached_run_path).write_text("{")

    with pytest.raises(WorkflowError, match="is corrupted"):
        persistence.detached_run()


def test_file_detached_run_rejects_namespace_mismatch(tmp_path):
    persistence = FilePersistence(dag=None, path=tmp_path / ".snakemake")
    persistence.begin_detached_run(executor="cluster-generic")
    record_path = Path(persistence._detached_run_path)
    record = json.loads(record_path.read_text())
    record["namespace"] = str(tmp_path / "somewhere-else" / ".snakemake")
    record_path.write_text(json.dumps(record))

    with pytest.raises(WorkflowError, match="belongs to persistence namespace"):
        persistence.detached_run()
