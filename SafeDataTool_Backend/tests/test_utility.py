import pandas as pd
import pytest
from pathlib import Path
import tempfile

from app.pipeline.utility.evaluator import UtilityEvaluator


@pytest.fixture
def original_dataframe():
    """Create original dataset."""
    return pd.DataFrame({
        "age": [25, 30, 35, 40, 45],
        "income": [50000, 60000, 70000, 80000, 90000],
        "category": ["A", "B", "A", "B", "A"],
    })


@pytest.fixture
def protected_dataframe():
    """Create protected dataset (with some suppression)."""
    return pd.DataFrame({
        "age": [25, 30, 35, "*", 45],
        "income": [51000, 61000, 71000, "*", 91000],  # Slightly noisy
        "category": ["A", "B", "A", "*", "A"],
    })


class TestUtilityEvaluator:
    """Test utility evaluation."""

    def test_evaluate_creates_metrics(self, original_dataframe, protected_dataframe):
        """Test that evaluation produces metrics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            original_path = Path(tmpdir) / "original.csv"
            protected_path = Path(tmpdir) / "protected.csv"

            original_dataframe.to_csv(original_path, index=False)
            protected_dataframe.to_csv(protected_path, index=False)

            evaluator = UtilityEvaluator()
            results = evaluator.evaluate(original_path, protected_path)

            assert "summary" in results
            assert "metrics" in results
            assert len(results["metrics"]) > 0

    def test_completeness_metric(self, original_dataframe, protected_dataframe):
        """Test data completeness calculation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            original_path = Path(tmpdir) / "original.csv"
            protected_path = Path(tmpdir) / "protected.csv"

            original_dataframe.to_csv(original_path, index=False)
            protected_dataframe.to_csv(protected_path, index=False)

            evaluator = UtilityEvaluator()
            results = evaluator.evaluate(original_path, protected_path)

            completeness_metric = next((m for m in results["metrics"] if m["name"] == "data_completeness"), None)
            assert completeness_metric is not None
            assert 0 <= completeness_metric["value"] <= 1

    def test_mean_preservation_metric(self, original_dataframe, protected_dataframe):
        """Test mean preservation calculation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            original_path = Path(tmpdir) / "original.csv"
            protected_path = Path(tmpdir) / "protected.csv"

            original_dataframe.to_csv(original_path, index=False)
            protected_dataframe.to_csv(protected_path, index=False)

            evaluator = UtilityEvaluator()
            results = evaluator.evaluate(original_path, protected_path)

            mean_metric = next((m for m in results["metrics"] if m["name"] == "mean_preservation"), None)
            if mean_metric:
                assert 0 <= mean_metric["value"] <= 1

