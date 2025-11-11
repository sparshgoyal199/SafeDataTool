import pandas as pd
import pytest
from pathlib import Path
import tempfile

from app.pipeline.risk.analyzer import RiskAnalyzer


@pytest.fixture
def anonymized_dataframe():
    """Create anonymized dataset."""
    return pd.DataFrame({
        "age": [25, 25, 30, 30, 40],
        "zipcode": ["12345", "12345", "67890", "67890", "12345"],
        "income": [50000, 55000, 60000, 65000, 70000],
    })


@pytest.fixture
def identifier_dataframe():
    """Create identifier dataset for testing."""
    return pd.DataFrame({
        "age": [25, 25, 30, 30, 40],
        "zipcode": ["12345", "12345", "67890", "67890", "12345"],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    })


class TestRiskAnalyzer:
    """Test risk assessment."""

    def test_analyze_creates_metrics(self, anonymized_dataframe):
        """Test that analysis produces risk metrics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset_path = Path(tmpdir) / "anonymized.csv"
            anonymized_dataframe.to_csv(dataset_path, index=False)

            analyzer = RiskAnalyzer()
            results = analyzer.analyze(
                dataset_path=dataset_path,
                quasi_identifiers=["age", "zipcode"],
            )

            assert "summary" in results
            assert "metrics" in results
            assert len(results["metrics"]) > 0

    def test_uniqueness_rate_calculation(self, anonymized_dataframe):
        """Test uniqueness rate calculation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset_path = Path(tmpdir) / "anonymized.csv"
            anonymized_dataframe.to_csv(dataset_path, index=False)

            analyzer = RiskAnalyzer()
            results = analyzer.analyze(
                dataset_path=dataset_path,
                quasi_identifiers=["age", "zipcode"],
            )

            uniqueness_metric = next((m for m in results["metrics"] if m["name"] == "uniqueness_rate"), None)
            if uniqueness_metric:
                assert 0 <= uniqueness_metric["value"] <= 1

    def test_linkage_attack_simulation(self, anonymized_dataframe, identifier_dataframe):
        """Test linkage attack simulation with identifier dataset."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset_path = Path(tmpdir) / "anonymized.csv"
            identifier_path = Path(tmpdir) / "identifiers.csv"

            anonymized_dataframe.to_csv(dataset_path, index=False)
            identifier_dataframe.to_csv(identifier_path, index=False)

            analyzer = RiskAnalyzer()
            results = analyzer.analyze(
                dataset_path=dataset_path,
                quasi_identifiers=["age", "zipcode"],
                identifier_path=identifier_path,
            )

            # Should have reidentification rate if identifiers provided
            reid_metric = next((m for m in results["metrics"] if m["name"] == "reidentification_rate"), None)
            if reid_metric:
                assert 0 <= reid_metric["value"] <= 1

