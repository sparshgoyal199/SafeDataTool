from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass
class UtilityMetricResult:
    name: str
    value: float
    label: str
    details: dict[str, float] | None = None


class UtilityEvaluator:
    """Compare original and protected datasets to quantify utility loss."""

    def __init__(self, quasi_identifiers: Iterable[str] | None = None):
        self.quasi_identifiers = list(quasi_identifiers or [])

    def evaluate(self, original_df: pd.DataFrame, protected_df: pd.DataFrame) -> list[UtilityMetricResult]:
        metrics: list[UtilityMetricResult] = []

        metrics.append(self._rowcount_delta(original_df, protected_df))
        metrics.extend(self._numeric_aggregate_deltas(original_df, protected_df))
        metrics.extend(self._categorical_overlap(original_df, protected_df))

        return metrics

    def _rowcount_delta(self, original: pd.DataFrame, protected: pd.DataFrame) -> UtilityMetricResult:
        delta = len(protected) - len(original)
        rate = delta / len(original) if len(original) else 0.0
        return UtilityMetricResult(
            name="rowcount_delta",
            value=float(rate),
            label="Relative change in row count",
            details={
                "original_rows": int(len(original)),
                "protected_rows": int(len(protected)),
                "delta": int(delta),
            },
        )

    def _numeric_aggregate_deltas(
        self,
        original: pd.DataFrame,
        protected: pd.DataFrame,
    ) -> list[UtilityMetricResult]:
        metrics: list[UtilityMetricResult] = []
        numeric_columns = original.select_dtypes(include=[np.number]).columns

        for column in numeric_columns:
            orig_series = original[column].dropna()
            prot_series = protected[column].dropna()
            if orig_series.empty or prot_series.empty:
                continue

            orig_mean = orig_series.mean()
            prot_mean = prot_series.mean()
            mean_delta = float(np.abs(orig_mean - prot_mean))
            rel_delta = float(mean_delta / np.abs(orig_mean)) if orig_mean else mean_delta

            orig_std = orig_series.std(ddof=0)
            prot_std = prot_series.std(ddof=0)
            std_delta = float(np.abs(orig_std - prot_std))

            metrics.append(
                UtilityMetricResult(
                    name=f"{column}_mean_delta",
                    value=rel_delta,
                    label=f"Relative mean difference for {column}",
                    details={
                        "orig_mean": float(orig_mean),
                        "prot_mean": float(prot_mean),
                        "abs_delta": mean_delta,
                    },
                )
            )

            metrics.append(
                UtilityMetricResult(
                    name=f"{column}_std_delta",
                    value=std_delta,
                    label=f"Standard deviation difference for {column}",
                    details={
                        "orig_std": float(orig_std),
                        "prot_std": float(prot_std),
                    },
                )
            )

        return metrics

    def _categorical_overlap(
        self,
        original: pd.DataFrame,
        protected: pd.DataFrame,
    ) -> list[UtilityMetricResult]:
        metrics: list[UtilityMetricResult] = []
        categorical_columns = original.select_dtypes(include=["object", "category"]).columns

        for column in categorical_columns:
            orig_counts = (original[column].fillna("MISSING").value_counts(normalize=True)).to_dict()
            prot_counts = (protected[column].fillna("MISSING").value_counts(normalize=True)).to_dict()

            categories = set(orig_counts) | set(prot_counts)
            overlap = sum(min(orig_counts.get(cat, 0.0), prot_counts.get(cat, 0.0)) for cat in categories)

            metrics.append(
                UtilityMetricResult(
                    name=f"{column}_overlap",
                    value=float(overlap),
                    label=f"Categorical distribution overlap for {column}",
                    details={"unique_categories": float(len(categories))},
                )
            )

        return metrics

