__authors__ = ["K.D. Murray"]
__copyright__ = "Copyright 2024, Johannes Köster"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"


from snakemake.settings.types import Batch

import pytest


def test_parse_batch():
    from snakemake.cli import parse_batch

    assert parse_batch("aggregate=1/2") == Batch("aggregate", 1, 2)


@pytest.mark.parametrize(
    ("flag", "attribute"), [("--detach", "detach"), ("--attach", "attach")]
)
def test_parse_detach_attach(flag, attribute):
    from snakemake.cli import get_argument_parser

    args = get_argument_parser().parse_args([flag])

    assert getattr(args, attribute)


def test_detach_attach_are_mutually_exclusive_in_cli():
    from snakemake.cli import get_argument_parser

    with pytest.raises(SystemExit):
        get_argument_parser().parse_args(["--detach", "--attach"])


def test_detach_attach_are_marked_experimental_in_help():
    from snakemake.cli import get_argument_parser

    help_text = get_argument_parser().format_help()

    assert "--detach" in help_text
    assert "--attach" in help_text
    assert help_text.count("Experimental:") >= 2
