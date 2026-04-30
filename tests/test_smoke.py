"""
Smoke tests — verify the app starts and key routes respond.
No CSV upload needed; these run in CI without a real dataset.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from app.advanced_analysis_app import app as flask_app


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as c:
        yield c


def test_upload_page_loads(client):
    """GET / should return the upload page with HTTP 200."""
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"Upload" in resp.data or b"upload" in resp.data


def test_analyze_missing_file_returns_404(client):
    """Requesting analysis for a non-existent file_id should return 404."""
    resp = client.get("/analyze/nonexistent-file-id")
    assert resp.status_code == 404


def test_report_csv_missing_file_returns_404(client):
    """Requesting CSV report for a non-existent file_id should return 404."""
    resp = client.get("/report/csv/nonexistent-file-id")
    assert resp.status_code == 404


def test_report_full_missing_file_returns_404(client):
    """Requesting full report for a non-existent file_id should return 404."""
    resp = client.get("/report/full/nonexistent-file-id")
    assert resp.status_code == 404


def test_magnet_detector_imports():
    """MagnetQuenchDetector must be importable."""
    from src.detection.magnet_detector import MagnetQuenchDetector
    detector = MagnetQuenchDetector(use_persistence=False)
    assert detector is not None
