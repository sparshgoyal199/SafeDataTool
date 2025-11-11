from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from app.config import get_settings
from app.pipeline.privacy import PrivacyTransformer
from app.pipeline.reporting import ReportGenerator
from app.pipeline.risk import RiskAnalyzer, RiskMetricResult
from app.pipeline.utility import UtilityEvaluator, UtilityMetricResult


@dataclass
class PipelineArtifacts:
    protected_dataset_path: Path
    report_html_path: Path
    report_pdf_path: Path | None
    risk_metrics: list[RiskMetricResult]
    privacy_info: dict[str, Any]
    utility_metrics: list[UtilityMetricResult]


class PipelineOrchestrator:
    """Coordinate the risk, privacy, and utility pipeline."""

    def __init__(self, settings=None):
        self.settings = settings or get_settings()
        templates_dir = Path(__file__).resolve().parent / "reporting" / "templates"
        self.report_generator = ReportGenerator(
            templates_dir=templates_dir,
            output_dir=self.settings.reports_dir,
        )

    def run(
        self,
        dataset_path: Path,
        config: Any,
        identifier_path: Path | None = None,
        run_id: str | None = None,
    ) -> PipelineArtifacts:
        original_df = self._load_dataframe(dataset_path)
        identifier_df = self._load_dataframe(identifier_path) if identifier_path else None

        quasi_identifiers = self._extract_quasi_identifiers(config)
        risk_metrics = self._compute_risk(original_df, quasi_identifiers, identifier_df)

        privacy_result = self._apply_privacy(original_df, quasi_identifiers, config)
        protected_df = privacy_result.protected

        utility_metrics = self._compute_utility(original_df, protected_df, quasi_identifiers)

        protected_path = self._persist_protected_dataset(protected_df, run_id)

        report_paths = self._generate_report(
            dataset_path=dataset_path,
            config=config,
            risk_metrics=risk_metrics,
            privacy_info=privacy_result.info,
            utility_metrics=utility_metrics,
            run_id=run_id,
        )

        return PipelineArtifacts(
            protected_dataset_path=protected_path,
            report_html_path=report_paths.html_path,
            report_pdf_path=report_paths.pdf_path,
            risk_metrics=risk_metrics,
            privacy_info=privacy_result.info,
            utility_metrics=utility_metrics,
        )

    def _load_dataframe(self, path: Path | None) -> pd.DataFrame:
        if path is None:
            raise ValueError("Dataset path cannot be None.")
        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}")
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        if path.suffix.lower() in {".parquet", ".pq"}:
            return pd.read_parquet(path)
        raise ValueError(f"Unsupported dataset format: {path.suffix}")

    def _extract_quasi_identifiers(self, config: Any) -> Sequence[str]:
        quasi_identifiers = getattr(config, "quasi_identifiers", None)
        if not quasi_identifiers:
            raise ValueError("Pipeline configuration must include quasi identifiers.")
        return list(quasi_identifiers)

    def _compute_risk(
        self,
        original_df: pd.DataFrame,
        quasi_identifiers: Iterable[str],
        identifier_df: pd.DataFrame | None = None,
    ) -> list[RiskMetricResult]:
        analyzer = RiskAnalyzer(quasi_identifiers)
        return analyzer.evaluate(original_df, identifier_df)

    def _apply_privacy(
        self,
        original_df: pd.DataFrame,
        quasi_identifiers: Sequence[str],
        config: Any,
    ):
        technique = getattr(config, "privacy_technique", "k_anonymity")
        parameters = getattr(config, "privacy_parameters", {}) or {}
        transformer = PrivacyTransformer(
            technique=technique,
            quasi_identifiers=quasi_identifiers,
            parameters=parameters,
        )
        return transformer.apply(original_df)

    def _compute_utility(
        self,
        original_df: pd.DataFrame,
        protected_df: pd.DataFrame,
        quasi_identifiers: Iterable[str],
    ) -> list[UtilityMetricResult]:
        evaluator = UtilityEvaluator(quasi_identifiers=quasi_identifiers)
        return evaluator.evaluate(original_df, protected_df)

    def _persist_protected_dataset(self, protected_df: pd.DataFrame, run_id: str | None) -> Path:
        filename = f"{run_id}_protected.csv" if run_id else datetime.utcnow().strftime(
            "protected_%Y%m%dT%H%M%S.csv"
        )
        output_path = self.settings.processed_data_dir / filename
        protected_df.to_csv(output_path, index=False)
        return output_path

    def _generate_report(
        self,
        dataset_path: Path,
        config: Any,
        risk_metrics: list[RiskMetricResult],
        privacy_info: dict[str, Any],
        utility_metrics: list[UtilityMetricResult],
        run_id: str | None = None,
    ):
        context = self._build_report_context(
            dataset_path=dataset_path,
            config=config,
            risk_metrics=risk_metrics,
            privacy_info=privacy_info,
            utility_metrics=utility_metrics,
            run_id=run_id,
        )
        filename_prefix = run_id or None
        return self.report_generator.render(context=context, filename_prefix=filename_prefix)

    def _build_report_context(
        self,
        dataset_path: Path,
        config: Any,
        risk_metrics: list[RiskMetricResult],
        privacy_info: dict[str, Any],
        utility_metrics: list[UtilityMetricResult],
        run_id: str | None = None,
    ) -> dict[str, Any]:
        recommendations = self._generate_recommendations(risk_metrics, utility_metrics)

        return {
            "title": "SafeData Privacy-Utility Report",
            "generated_at": datetime.utcnow().isoformat(),
            "dataset": {
                "name": dataset_path.name,
                "description": getattr(config, "dataset_description", ""),
            },
            "config": {
                "id": getattr(config, "id", None),
                "name": getattr(config, "name", "Unnamed configuration"),
                "quasi_identifiers": list(getattr(config, "quasi_identifiers", [])),
                "privacy_technique": getattr(config, "privacy_technique", "k_anonymity"),
                "privacy_parameters": getattr(config, "privacy_parameters", {}),
            },
            "run": {
                "id": run_id,
                "status": "completed",
                "started_at": getattr(config, "run_started_at", ""),
                "completed_at": datetime.utcnow().isoformat(),
            },
            "risk_metrics": [metric.__dict__ for metric in risk_metrics],
            "privacy_info": privacy_info,
            "utility_metrics": [metric.__dict__ for metric in utility_metrics],
            "recommendations": recommendations,
        }

    def _generate_recommendations(
        self,
        risk_metrics: list[RiskMetricResult],
        utility_metrics: list[UtilityMetricResult],
    ) -> list[str]:
        recommendations: list[str] = []

        uniques_metric = next(
            (metric for metric in risk_metrics if metric.name == "uniques_rate"), None
        )
        if uniques_metric and uniques_metric.value > 0.1:
            recommendations.append(
                "High proportion of unique records detected — consider increasing k or adding noise."
            )

        linkage_metric = next(
            (metric for metric in risk_metrics if metric.name == "linkage_success_rate"), None
        )
        if linkage_metric and linkage_metric.value > 0.05:
            recommendations.append(
                "Linkage attack success is elevated — review quasi-identifiers and apply stronger privacy controls."
            )

        mean_deltas = [
            metric
            for metric in utility_metrics
            if metric.name.endswith("_mean_delta") and metric.value > 0.2
        ]
        if mean_deltas:
            recommendations.append(
                "Utility degradation detected in numeric means — adjust epsilon or transformation aggressiveness."
            )

        if not recommendations:
            recommendations.append("Risk levels appear acceptable with retained utility.")

        return recommendations

