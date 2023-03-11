"""Verify the library top-level functionality."""
import pyproject


def test_version():
    """Verify we have updated the package version."""
    assert pyproject.__version__ == "23.3.1.dev0"
