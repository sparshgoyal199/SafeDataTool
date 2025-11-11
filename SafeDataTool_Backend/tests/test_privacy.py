import pandas as pd
import pytest
from pathlib import Path

from app.pipeline.privacy.transformers import KAnonymityTransformer, DifferentialPrivacyTransformer


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        "age": [25, 30, 25, 40, 30, 25],
        "zipcode": ["12345", "12345", "67890", "12345", "67890", "12345"],
        "income": [50000, 60000, 55000, 70000, 65000, 52000],
    })


class TestKAnonymityTransformer:
    """Test k-anonymity transformation."""

    def test_k_anonymity_suppression(self, sample_dataframe):
        """Test that k-anonymity suppresses rows in groups smaller than k."""
        transformer = KAnonymityTransformer()
        quasi_identifiers = ["age", "zipcode"]
        k = 3

        result = transformer.transform(sample_dataframe, quasi_identifiers, k)

        # Check that suppressed values are replaced with "*"
        assert "*" in result["age"].values or "*" in result["zipcode"].values

    def test_k_anonymity_preserves_large_groups(self, sample_dataframe):
        """Test that groups with k or more members are preserved."""
        transformer = KAnonymityTransformer()
        quasi_identifiers = ["age", "zipcode"]
        k = 2  # Lower k to preserve more data

        result = transformer.transform(sample_dataframe, quasi_identifiers, k)

        # Should have some non-suppressed values
        assert not all(result["age"] == "*")


class TestDifferentialPrivacyTransformer:
    """Test differential privacy transformation."""

    def test_dp_adds_noise_to_numeric(self, sample_dataframe):
        """Test that DP adds noise to numeric quasi-identifiers."""
        transformer = DifferentialPrivacyTransformer()
        quasi_identifiers = ["age", "income"]
        epsilon = 1.0

        result = transformer.transform(sample_dataframe, quasi_identifiers, epsilon)

        # Values should be different (noise added)
        assert not result["age"].equals(sample_dataframe["age"])
        assert not result["income"].equals(sample_dataframe["income"])

    def test_dp_preserves_data_type(self, sample_dataframe):
        """Test that DP preserves data types."""
        transformer = DifferentialPrivacyTransformer()
        quasi_identifiers = ["age", "income"]
        epsilon = 1.0

        result = transformer.transform(sample_dataframe, quasi_identifiers, epsilon)

        assert pd.api.types.is_numeric_dtype(result["age"])
        assert pd.api.types.is_numeric_dtype(result["income"])

