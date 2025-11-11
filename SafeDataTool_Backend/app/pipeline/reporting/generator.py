from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

# Try to import WeasyPrint, but make it optional
# WeasyPrint requires GTK+ runtime libraries on Windows which may not be available
HAS_WEASYPRINT = False
HTML = None
try:
    from weasyprint import HTML
    HAS_WEASYPRINT = True
except (ImportError, OSError, Exception) as e:
    # WeasyPrint requires system libraries (GTK+ on Windows) that may not be available
    # The application will work without PDF generation - HTML reports will still be generated
    HAS_WEASYPRINT = False
    HTML = None


class ReportGenerator:
    """Generates comprehensive Privacy-Utility reports in HTML and PDF formats."""

    def __init__(self):
        template_dir = Path(__file__).parent / "templates"
        self.env = Environment(loader=FileSystemLoader(str(template_dir)))

    def generate(
        self,
        run_id: str,
        risk_results: dict[str, Any],
        utility_results: dict[str, Any],
        privacy_summary: dict[str, Any],
        output_dir: Path,
    ) -> dict[str, str]:
        """
        Generate HTML and PDF reports.

        Returns:
            Dictionary with paths to generated reports
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        # Render HTML template
        template = self.env.get_template("report.html")
        html_content = template.render(
            run_id=run_id,
            risk_summary=risk_results.get("summary", {}),
            risk_metrics=risk_results.get("metrics", []),
            utility_summary=utility_results.get("summary", {}),
            utility_metrics=utility_results.get("metrics", []),
            privacy_summary=privacy_summary,
        )

        # Save HTML
        html_path = output_dir / "report.html"
        html_path.write_text(html_content, encoding="utf-8")

        # Generate PDF if WeasyPrint available
        pdf_path = None
        if HAS_WEASYPRINT and HTML is not None:
            try:
                pdf_path = output_dir / "report.pdf"
                HTML(string=html_content).write_pdf(pdf_path)
            except Exception as e:
                # PDF generation failed, but HTML report is still available
                print(f"Warning: PDF generation failed: {e}")
                pdf_path = None

        return {
            "html_path": str(html_path),
            "pdf_path": str(pdf_path) if pdf_path else None,
        }

