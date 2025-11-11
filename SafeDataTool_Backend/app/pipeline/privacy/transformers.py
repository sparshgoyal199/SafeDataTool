import numpy as np
import pandas as pd
from pathlib import Path
from typing import Any


class PrivacyTransformer:
    """Applies privacy-enhancing transformations to datasets."""

    def __init__(self):
        self.kanon_transformer = KAnonymityTransformer()
        self.dp_transformer = DifferentialPrivacyTransformer()

    def transform(
        self,
        dataset_path: Path,
        technique: str,
        quasi_identifiers: list[str],
        parameters: dict[str, Any],
        output_dir: Path,
    ) -> Path:
        """
        Transform dataset using specified privacy technique.

        Args:
            dataset_path: Path to original dataset
            technique: Privacy technique name ('k_anonymity', 'differential_privacy', etc.)
            quasi_identifiers: Columns to protect
            parameters: Technique-specific parameters
            output_dir: Directory to save protected dataset

        Returns:
            Path to protected dataset
        """
        df = pd.read_csv(dataset_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        if technique == "k_anonymity":
            k = parameters.get("k", 5)
            protected_df = self.kanon_transformer.transform(df, quasi_identifiers, k)
        elif technique == "differential_privacy":
            epsilon = parameters.get("epsilon", 1.0)
            protected_df = self.dp_transformer.transform(df, quasi_identifiers, epsilon)
        else:
            raise ValueError(f"Unknown privacy technique: {technique}")

        output_path = output_dir / f"protected_{dataset_path.name}"
        protected_df.to_csv(output_path, index=False)
        return output_path


class KAnonymityTransformer:
    """Implements k-anonymity through generalization and suppression."""

    def transform(self, df: pd.DataFrame, quasi_identifiers: list[str], k: int) -> pd.DataFrame:
        """
        Apply k-anonymity transformation.

        Simple implementation: suppress rows that don't meet k-anonymity.
        """
        protected_df = df.copy()

        # Count occurrences of each quasi-identifier combination
        qid_subset = protected_df[quasi_identifiers]
        group_sizes = qid_subset.groupby(quasi_identifiers).size()

        # Mark rows in groups smaller than k for suppression
        mask = qid_subset.apply(
            lambda row: group_sizes[tuple(row.values)] >= k,
            axis=1,
        )

        # Suppress by replacing with "*"
        suppressed_df = protected_df.copy()
        for col in quasi_identifiers:
            suppressed_df.loc[~mask, col] = "*"

        return suppressed_df


class DifferentialPrivacyTransformer:
    """Implements differential privacy through noise addition."""

    def transform(self, df: pd.DataFrame, quasi_identifiers: list[str], epsilon: float) -> pd.DataFrame:
        """
        Apply differential privacy by adding Laplace noise to numeric quasi-identifiers.
        """
        protected_df = df.copy()

        for col in quasi_identifiers:
            if pd.api.types.is_numeric_dtype(df[col]):
                # Calculate sensitivity (assuming range-based)
                col_range = df[col].max() - df[col].min()
                sensitivity = col_range if col_range > 0 else 1.0

                # Add Laplace noise
                scale = sensitivity / epsilon
                noise = np.random.laplace(0, scale, size=len(df))
                protected_df[col] = df[col] + noise

        return protected_df

