import pandas as pd
from pathlib import Path
from typing import Any


class RiskAnalyzer:
    """Analyzes re-identification risk through linkage attack simulation."""

    def __init__(self):
        self.attacker = LinkageAttacker()

    def analyze(
        self,
        dataset_path: Path,
        quasi_identifiers: list[str],
        identifier_path: Path | None = None,
    ) -> dict[str, Any]:
        """
        Perform risk assessment on the dataset.

        Args:
            dataset_path: Path to the anonymized dataset
            quasi_identifiers: List of column names that are quasi-identifiers
            identifier_path: Optional path to true identifier file for testing

        Returns:
            Dictionary with risk metrics and summary
        """
        df = pd.read_csv(dataset_path)

        # Validate quasi-identifiers exist
        missing = [qid for qid in quasi_identifiers if qid not in df.columns]
        if missing:
            raise ValueError(f"Quasi-identifiers not found in dataset: {missing}")

        # Perform linkage attack simulation
        attack_results = self.attacker.simulate_attack(
            anonymized_df=df,
            quasi_identifiers=quasi_identifiers,
            identifier_df=pd.read_csv(identifier_path) if identifier_path and identifier_path.exists() else None,
        )

        # Calculate risk metrics
        metrics = []
        summary = {}

        # k-map analysis
        k_map = attack_results.get("k_map", {})
        unique_combinations = sum(1 for k in k_map.values() if k == 1)
        total_combinations = len(k_map)

        if total_combinations > 0:
            uniqueness_rate = unique_combinations / total_combinations
            metrics.append(
                {
                    "name": "uniqueness_rate",
                    "value": uniqueness_rate,
                    "label": "Proportion of unique quasi-identifier combinations",
                    "details": {"unique": unique_combinations, "total": total_combinations},
                }
            )
            summary["uniqueness_rate"] = uniqueness_rate

        # Average k-value
        if k_map:
            avg_k = sum(k_map.values()) / len(k_map)
            metrics.append(
                {
                    "name": "average_k",
                    "value": avg_k,
                    "label": "Average k-value (higher is better)",
                    "details": {"min_k": min(k_map.values()), "max_k": max(k_map.values())},
                }
            )
            summary["average_k"] = avg_k

        # Re-identification success rate (if identifiers provided)
        if attack_results.get("match_rate") is not None:
            match_rate = attack_results["match_rate"]
            metrics.append(
                {
                    "name": "reidentification_rate",
                    "value": match_rate,
                    "label": "Proportion of records successfully re-identified",
                    "details": attack_results.get("match_details", {}),
                }
            )
            summary["reidentification_rate"] = match_rate

        return {
            "summary": summary,
            "metrics": metrics,
            "k_map": k_map,
        }


class LinkageAttacker:
    """Simulates linkage attacks on anonymized data."""

    def simulate_attack(
        self,
        anonymized_df: pd.DataFrame,
        quasi_identifiers: list[str],
        identifier_df: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        """
        Simulate a linkage attack.

        Args:
            anonymized_df: The anonymized dataset
            quasi_identifiers: Columns to use for linkage
            identifier_df: Optional external dataset with true identifiers

        Returns:
            Attack results including k-map and match statistics
        """
        # Build k-map: count occurrences of each quasi-identifier combination
        qid_subset = anonymized_df[quasi_identifiers]
        k_map = {}
        for _, row in qid_subset.iterrows():
            key = tuple(row.values)
            k_map[key] = k_map.get(key, 0) + 1

        results = {"k_map": {str(k): v for k, v in k_map.items()}}

        # If identifier dataset provided, attempt matching
        if identifier_df is not None:
            matches = 0
            total = len(anonymized_df)

            # Simple exact match on quasi-identifiers
            for _, anon_row in anonymized_df.iterrows():
                qid_values = tuple(anon_row[qid] for qid in quasi_identifiers)
                # Check if this combination exists in identifier dataset
                match_mask = True
                for i, qid in enumerate(quasi_identifiers):
                    if qid in identifier_df.columns:
                        match_mask = match_mask & (identifier_df[qid] == qid_values[i])
                if match_mask.any():
                    matches += 1

            match_rate = matches / total if total > 0 else 0.0
            results["match_rate"] = match_rate
            results["match_details"] = {"matches": matches, "total": total}

        return results

