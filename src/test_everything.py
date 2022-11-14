import pytest
import solacc
import subprocess
import os
from click.testing import CliRunner

def results_matches_expected(basedir):
    results_dir = os.path.join(basedir, "results")
    expected_dir = os.path.join(basedir, "expected")
    process = subprocess.Popen(["diff", "-qr", results_dir, expected_dir])
    exit_code = process.wait()
    return not exit_code

def test_generate_smoke01():
    runner = CliRunner()
    basedir = "src/tests/smoke01"
    result = runner.invoke(solacc.cli, ['--config', os.path.join(basedir, 'smoke01.json'), 'generate', '--prefix', 'True'])
    expected_result_str = """Generating notebooks for 
... 00_okta_collector
... 01_bronze_sample
... 02_dlt_edges
... 03_create_views
... 04_extract_same_as_edges
... 05_analytics_01_impact
... 06_analytics_02_investigation
... RUNME
"""
    assert result.output == expected_result_str
    assert results_matches_expected(basedir)

def test_generate_smoke02():
    runner = CliRunner()
    basedir = "src/tests/smoke02_corelight"
    result = runner.invoke(solacc.cli, ['--config', os.path.join(basedir, 'smoke02.json'), 'generate'])
    expected_result_str = """Generating notebooks for 
... dlt_edges
... create_views
"""
    assert result.output == expected_result_str
    assert results_matches_expected(basedir)
