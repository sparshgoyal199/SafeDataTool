import pandas as pd
from pathlib import Path
from typing import Any

from scipy import stats


class UtilityEvaluator:
    """Evaluates utility preservation in protected datasets."""

    def evaluate(self, original_path: Path, protected_path: Path) -> dict[str, Any]:
        """
        Compare original and protected datasets to measure utility loss.

        Returns:
            Dictionary with utility metrics and summary
        """
        original_df = pd.read_csv(original_path)
        protected_df = pd.read_csv(protected_path)

        metrics = []
        summary = {}

        # Basic statistics comparison
        numeric_cols = original_df.select_dtypes(include=["number"]).columns

        if len(numeric_cols) > 0:
            # Mean preservation
            mean_diff = 0.0
            for col in numeric_cols:
                if col in protected_df.columns:
                    orig_mean = original_df[col].mean()
                    prot_mean = protected_df[col].mean()
                    if orig_mean != 0:
                        mean_diff += abs(orig_mean - prot_mean) / abs(orig_mean)
            mean_diff /= len(numeric_cols)

            metrics.append(
                {
                    "name": "mean_preservation",
                    "value": 1.0 - mean_diff,  # Higher is better
                    "label": "Mean value preservation (1.0 = perfect)",
                    "details": {"mean_difference": mean_diff},
                }
            )
            summary["mean_preservation"] = 1.0 - mean_diff

            # Distribution similarity (Kolmogorov-Smirnov test)
            ks_scores = []
            for col in numeric_cols:
                if col in protected_df.columns:
                    try:
                        ks_stat, _ = stats.ks_2samp(original_df[col].dropna(), protected_df[col].dropna())
                        ks_scores.append(ks_stat)
                    except Exception:
                        pass

            if ks_scores:
                avg_ks = sum(ks_scores) / len(ks_scores)
                metrics.append(
                    {
                        "name": "distribution_similarity",
                        "value": 1.0 - avg_ks,  # Lower KS = more similar
                        "label": "Distribution similarity (1.0 = identical)",
                        "details": {"avg_ks_statistic": avg_ks},
                    }
                )
                summary["distribution_similarity"] = 1.0 - avg_ks

        # Data completeness (non-suppressed rows)
        total_cells = len(original_df) * len(original_df.columns)
        suppressed_cells = (protected_df == "*").sum().sum()
        completeness = 1.0 - (suppressed_cells / total_cells) if total_cells > 0 else 1.0

        metrics.append(
            {
                "name": "data_completeness",
                "value": completeness,
                "label": "Proportion of non-suppressed data",
                "details": {"suppressed_cells": int(suppressed_cells), "total_cells": total_cells},
            }
        )
        summary["data_completeness"] = completeness

        return {
            "summary": summary,
            "metrics": metrics,
        }

