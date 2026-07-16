"""Smoke test: every core module must import cleanly.

Catches whole classes of bugs (missing imports, references to deleted
functions, unguarded platform-specific imports) before anything runs.
crexi_watcher_app is excluded: it needs tkinter, which headless CI may lack.
"""
import importlib

import pytest

MODULES = [
    "comps_pipeline",
    "db_utils",
    "crexi.census_pop",
    "crexi_watcher",
    "rank_reports",
    "app",
]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name):
    importlib.import_module(name)
