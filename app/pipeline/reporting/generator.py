from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape


@dataclass
class ReportPaths:
    html_path: Path
    pdf_path: Path | None = None


class ReportGenerator:
    """Render privacy-utility reports to HTML and optionally PDF."""

    def __init__(self, templates_dir: Path, output_dir: Path):
        self.templates_dir = templates_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.environment = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            autoescape=select_autoescape(["html", "xml"]),
        )

    def render(
        self,
        context: dict[str, Any],
        template_name: str = "report.html",
        filename_prefix: str | None = None,
    ) -> ReportPaths:
        if filename_prefix is None:
            filename_prefix = datetime.utcnow().strftime("report_%Y%m%dT%H%M%S")

        template = self.environment.get_template(template_name)
        html_content = template.render(**context)

        html_path = self.output_dir / f"{filename_prefix}.html"
        html_path.write_text(html_content, encoding="utf-8")

        pdf_path = self._generate_pdf(html_content, filename_prefix)

        return ReportPaths(html_path=html_path, pdf_path=pdf_path)

    def _generate_pdf(self, html_content: str, filename_prefix: str) -> Path | None:
        try:
            from weasyprint import HTML  # type: ignore
        except Exception:
            return None

        pdf_path = self.output_dir / f"{filename_prefix}.pdf"
        HTML(string=html_content).write_pdf(target=str(pdf_path))
        return pdf_path

