from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd


@dataclass
class TransformationResult:
    protected: pd.DataFrame
    technique: str
    parameters: dict[str, Any]
    info: dict[str, Any]


class PrivacyTransformer:
    """Apply privacy-enhancing transformations to a dataset."""

    def __init__(
        self,
        technique: str,
        quasi_identifiers: Iterable[str],
        parameters: dict[str, Any] | None = None,
    ):
        self.technique = technique.lower()
        self.quasi_identifiers = list(quasi_identifiers)
        self.parameters = parameters or {}

    def apply(self, df: pd.DataFrame) -> TransformationResult:
        if self.technique in {"none", "identity"}:
            return TransformationResult(
                protected=df.copy(deep=True),
                technique="identity",
                parameters=self.parameters,
                info={"message": "No transformation applied"},
            )

        if self.technique in {"k_anonymity", "k-anonymity"}:
            protected, info = self._apply_k_anonymity(df)
            return TransformationResult(
                protected=protected,
                technique="k_anonymity",
                parameters=self.parameters,
                info=info,
            )

        if self.technique in {"differential_privacy", "dp"}:
            protected, info = self._apply_differential_privacy(df)
            return TransformationResult(
                protected=protected,
                technique="differential_privacy",
                parameters=self.parameters,
                info=info,
            )

        raise ValueError(f"Unsupported privacy technique: {self.technique}")

    # --------------------------------------------------------------------- #
    # k-anonymity via suppression + optional numeric generalisation
    # --------------------------------------------------------------------- #
    def _apply_k_anonymity(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
        if not self.quasi_identifiers:
            raise ValueError("k-anonymity requires quasi-identifiers.")

        k = int(self.parameters.get("k", 5))
        suppression_value = self.parameters.get("suppression_value", "SUPPRESSED")
        generalise_numeric = bool(self.parameters.get("generalise_numeric", True))
        bin_count = int(self.parameters.get("bin_count", 5))

        protected = df.copy(deep=True)

        key_series = protected[self.quasi_identifiers].astype(str).agg("||".join, axis=1)
        counts = key_series.map(key_series.value_counts())
        suppressed_mask = counts < k

        for column in self.quasi_identifiers:
            if generalise_numeric and pd.api.types.is_numeric_dtype(protected[column]):
                protected.loc[:, column] = self._generalise_numeric(
                    protected[column], bin_count=bin_count
                )
            else:
                protected.loc[suppressed_mask, column] = suppression_value

        suppressed_rows = int(suppressed_mask.sum())

        info = {
            "k": k,
            "suppressed_rows": suppressed_rows,
            "total_rows": int(len(df)),
        }

        return protected, info

    @staticmethod
    def _generalise_numeric(series: pd.Series, bin_count: int) -> pd.Series:
        if series.nunique(dropna=True) <= bin_count:
            return series
        binned = pd.cut(series, bins=bin_count, duplicates="drop")
        return binned.astype(str).fillna("MISSING")

    # --------------------------------------------------------------------- #
    # Differential privacy via Laplace mechanism
    # --------------------------------------------------------------------- #
    def _apply_differential_privacy(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
        epsilon = float(self.parameters.get("epsilon", 1.0))
        sensitivity = float(self.parameters.get("sensitivity", 1.0))

        if epsilon <= 0:
            raise ValueError("Epsilon must be positive for differential privacy.")

        scale = sensitivity / epsilon
        protected = df.copy(deep=True)
        numeric_columns = protected.select_dtypes(include=[np.number]).columns

        for column in numeric_columns:
            noise = np.random.laplace(loc=0.0, scale=scale, size=len(protected))
            protected[column] = protected[column] + noise

        info = {
            "epsilon": epsilon,
            "sensitivity": sensitivity,
            "noise_scale": scale,
            "columns": list(map(str, numeric_columns)),
        }

        return protected, info

