from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np
import pandas as pd


@dataclass
class RiskMetricResult:
    name: str
    value: float
    label: str
    details: dict[str, float] | None = None


class RiskAnalyzer:
    """Compute re-identification risk metrics by simulating linkage attacks."""

    def __init__(self, quasi_identifiers: Iterable[str]):
        self.quasi_identifiers = list(quasi_identifiers)

    def evaluate(
        self,
        anonymised_df: pd.DataFrame,
        identifier_df: Optional[pd.DataFrame] = None,
    ) -> list[RiskMetricResult]:
        if not self.quasi_identifiers:
            raise ValueError("At least one quasi identifier is required for risk analysis.")

        metrics: list[RiskMetricResult] = []

        metrics.append(self._uniques_rate(anonymised_df))
        metrics.append(self._k_map(anonymised_df))

        if identifier_df is not None:
            metrics.append(self._linkage_success_rate(anonymised_df, identifier_df))

        return metrics

    def _uniques_rate(self, df: pd.DataFrame) -> RiskMetricResult:
        grouped = df.groupby(self.quasi_identifiers)
        group_sizes = grouped.size()
        unique_count = (group_sizes == 1).sum()
        total = len(df)
        uniques_rate = unique_count / total if total else 0.0
        return RiskMetricResult(
            name="uniques_rate",
            value=float(uniques_rate),
            label="Proportion of unique records on quasi-identifiers",
            details={
                "unique_records": int(unique_count),
                "total_records": int(total),
            },
        )

    def _k_map(self, df: pd.DataFrame) -> RiskMetricResult:
        grouped = df.groupby(self.quasi_identifiers)
        group_sizes = grouped.size()
        mean_k = float(group_sizes.mean()) if len(group_sizes) else 0.0
        min_k = int(group_sizes.min()) if len(group_sizes) else 0
        return RiskMetricResult(
            name="k_map",
            value=mean_k,
            label="Average equivalence class size (k-map)",
            details={"min_k": min_k},
        )

    def _linkage_success_rate(
        self,
        anonymised_df: pd.DataFrame,
        identifier_df: pd.DataFrame,
    ) -> RiskMetricResult:
        joined = anonymised_df.merge(
            identifier_df,
            on=self.quasi_identifiers,
            how="left",
            suffixes=("_anonymised", "_identifier"),
            indicator=True,
        )
        matched = (joined["_merge"] == "both").sum()
        total = len(anonymised_df)
        success_rate = matched / total if total else 0.0

        return RiskMetricResult(
            name="linkage_success_rate",
            value=float(success_rate),
            label="Linkage attack success rate",
            details={
                "matched_records": int(matched),
                "total_records": int(total),
            },
        )

