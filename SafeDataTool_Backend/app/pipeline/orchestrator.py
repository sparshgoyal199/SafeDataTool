from pathlib import Path
from typing import Any

from app.db.models import PipelineConfig
from app.pipeline.privacy.transformers import PrivacyTransformer
from app.pipeline.reporting.generator import ReportGenerator
from app.pipeline.risk.analyzer import RiskAnalyzer
from app.pipeline.utility.evaluator import UtilityEvaluator


class PipelineOrchestrator:
    """Orchestrates the complete SafeData pipeline: risk assessment, privacy enhancement, utility measurement, and reporting."""

    def __init__(self):
        self.risk_analyzer = RiskAnalyzer()
        self.privacy_transformer = PrivacyTransformer()
        self.utility_evaluator = UtilityEvaluator()
        self.report_generator = ReportGenerator()

    def run(
        self,
        dataset_path: Path,
        config: PipelineConfig,
        identifier_path: Path | None = None,
        run_id: str = "",
    ) -> dict[str, Any]:
        """
        Execute the complete pipeline.

        Returns:
            Dictionary containing paths to artifacts and summary metrics.
        """
        # Step 1: Risk Assessment
        risk_results = self.risk_analyzer.analyze(
            dataset_path=dataset_path,
            quasi_identifiers=config.quasi_identifiers,
            identifier_path=identifier_path,
        )

        # Step 2: Privacy Enhancement
        protected_path = self.privacy_transformer.transform(
            dataset_path=dataset_path,
            technique=config.privacy_technique,
            quasi_identifiers=config.quasi_identifiers,
            parameters=config.privacy_parameters,
            output_dir=Path(f"./data/processed/{run_id}"),
        )

        # Step 3: Utility Evaluation
        utility_results = self.utility_evaluator.evaluate(
            original_path=dataset_path,
            protected_path=protected_path,
        )

        # Step 4: Generate Report
        report_paths = self.report_generator.generate(
            run_id=run_id,
            risk_results=risk_results,
            utility_results=utility_results,
            privacy_summary={
                "technique": config.privacy_technique,
                "parameters": config.privacy_parameters,
            },
            output_dir=Path(f"./reports/{run_id}"),
        )

        return {
            "protected_path": str(protected_path),
            "report_html_path": str(report_paths.get("html_path", "")),
            "report_pdf_path": str(report_paths.get("pdf_path", "")),
            "risk_summary": risk_results.get("summary", {}),
            "risk_metrics": risk_results.get("metrics", []),
            "utility_summary": utility_results.get("summary", {}),
            "utility_metrics": utility_results.get("metrics", []),
            "privacy_summary": {
                "technique": config.privacy_technique,
                "parameters": config.privacy_parameters,
            },
        }

