"""
Generates daily email digest.
Delivery: Gmail SMTP or Resend API, with fallback to saving HTML locally.
"""

import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
from rich.console import Console

console = Console()


class DigestBuilder:
    """Builds and delivers daily HTML job digest emails."""

    # Path constants (relative to project root, resolved at runtime)
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent
    _TEMPLATES_DIR = _PROJECT_ROOT / "templates"
    _TEMPLATE_FILE = "digest_email.html"
    _DIGESTS_DIR = _PROJECT_ROOT / "data" / "digests"

    def __init__(self):
        load_dotenv()

        self.email_method = os.getenv("EMAIL_METHOD", "gmail").lower()
        self.gmail_address = os.getenv("GMAIL_ADDRESS", "")
        self.gmail_app_password = os.getenv("GMAIL_APP_PASSWORD", "")
        self.digest_recipient = os.getenv("DIGEST_RECIPIENT", self.gmail_address)
        self.resend_api_key = os.getenv("RESEND_API_KEY", "")
        self.resend_from = os.getenv("RESEND_FROM", "digest@bayareajobhunter.com")

        env = Environment(
            loader=FileSystemLoader(str(self._TEMPLATES_DIR)),
            autoescape=True,
        )
        self.template = env.get_template(self._TEMPLATE_FILE)
        console.print("[dim]DigestBuilder: template loaded.[/dim]")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_description_excerpt(self, description: str, max_chars: int = 300) -> str:
        """Truncate *description* to ~max_chars at a sentence boundary.

        Also strips common markdown formatting so the excerpt is plain text
        suitable for an HTML email body.
        """
        if not description:
            return ""

        # Strip markdown formatting
        import re
        text = description

        # Remove markdown headings
        text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
        # Remove bold / italic markers
        text = re.sub(r"\*{1,3}(.*?)\*{1,3}", r"\1", text)
        text = re.sub(r"_{1,3}(.*?)_{1,3}", r"\1", text)
        # Remove inline code
        text = re.sub(r"`{1,3}[^`]*`{1,3}", "", text)
        # Remove markdown links but keep label text
        text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
        # Remove bare URLs
        text = re.sub(r"https?://\S+", "", text)
        # Collapse bullet / list markers
        text = re.sub(r"^\s*[-*+]\s+", "", text, flags=re.MULTILINE)
        text = re.sub(r"^\s*\d+\.\s+", "", text, flags=re.MULTILINE)
        # Collapse excess whitespace
        text = re.sub(r"\n{2,}", " ", text)
        text = re.sub(r"\s+", " ", text).strip()

        if len(text) <= max_chars:
            return text

        # Truncate at a sentence boundary within the limit
        truncated = text[:max_chars]
        last_period = max(
            truncated.rfind(". "),
            truncated.rfind("! "),
            truncated.rfind("? "),
        )
        if last_period != -1 and last_period > max_chars // 2:
            return truncated[: last_period + 1].strip()

        # Fall back to word boundary
        last_space = truncated.rfind(" ")
        if last_space != -1:
            return truncated[:last_space].strip() + "\u2026"

        return truncated.strip() + "\u2026"

    def _get_score_color(self, score: int) -> str:
        """Return a hex color string for a given relevance score."""
        if score >= 80:
            return "#22c55e"   # green
        elif score >= 60:
            return "#eab308"   # yellow
        elif score >= 40:
            return "#f97316"   # orange
        else:
            return "#ef4444"   # red

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build_digest(self, jobs_df: pd.DataFrame) -> tuple[str, str]:
        """Build the HTML email from *jobs_df*.

        Returns
        -------
        (subject, html_body)
        """
        if jobs_df is None or jobs_df.empty:
            jobs_df = pd.DataFrame()

        # ---- Sort: score desc, then posting_age asc (freshest first) ----
        if not jobs_df.empty:
            sort_cols = []
            ascending = []
            if "score" in jobs_df.columns:
                sort_cols.append("score")
                ascending.append(False)
            if "posting_age" in jobs_df.columns:
                sort_cols.append("posting_age")
                ascending.append(True)
            if sort_cols:
                jobs_df = jobs_df.sort_values(sort_cols, ascending=ascending)

        # Take top 25
        top_jobs_df = jobs_df.head(25)

        run_date = datetime.now().strftime("%B %d, %Y")
        generation_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

        total_new = len(jobs_df)
        high_scoring_count = (
            int((jobs_df["score"] >= 75).sum())
            if "score" in jobs_df.columns and not jobs_df.empty
            else 0
        )
        total_jobs_in_db = int(os.getenv("TOTAL_JOBS_IN_DB", total_new))

        # ---- Prepare per-job context dicts ----
        jobs_context = []
        for _, row in top_jobs_df.iterrows():
            job = row.to_dict()

            # Score
            score = int(job.get("score", 0))
            score_color = self._get_score_color(score)

            # Comp range: convert raw values to "k" integers if they look
            # like full dollar amounts (e.g. 150000 -> 150).
            def _to_k(val):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                val = float(val)
                if val == 0:
                    return None
                return int(round(val / 1000)) if val >= 1000 else int(val)

            comp_min = _to_k(job.get("comp_min"))
            comp_max = _to_k(job.get("comp_max"))

            # Description excerpt
            description_excerpt = self._get_description_excerpt(
                job.get("description", "") or "", max_chars=300
            )

            # Matched keywords from score_breakdown JSON
            matched_keywords: list[str] = []
            score_breakdown_raw = job.get("score_breakdown")
            if score_breakdown_raw:
                try:
                    if isinstance(score_breakdown_raw, str):
                        score_breakdown = json.loads(score_breakdown_raw)
                    else:
                        score_breakdown = score_breakdown_raw
                    matched_keywords = (
                        score_breakdown.get("matched_keywords", [])
                        or score_breakdown.get("keywords", [])
                        or []
                    )
                except (json.JSONDecodeError, AttributeError, TypeError):
                    pass

            # all_sources: may be a list, JSON string, or comma-separated str
            all_sources_raw = job.get("all_sources")
            all_sources: list[str] = []
            if all_sources_raw:
                if isinstance(all_sources_raw, list):
                    all_sources = all_sources_raw
                elif isinstance(all_sources_raw, str):
                    try:
                        parsed = json.loads(all_sources_raw)
                        all_sources = parsed if isinstance(parsed, list) else [str(parsed)]
                    except (json.JSONDecodeError, ValueError):
                        all_sources = [s.strip() for s in all_sources_raw.split(",") if s.strip()]

            jobs_context.append(
                {
                    "title": job.get("title", "Untitled Role"),
                    "company": job.get("company", "Unknown Company"),
                    "company_tier": job.get("company_tier", ""),
                    "location": job.get("location", "Bay Area, CA"),
                    "score": score,
                    "score_color": score_color,
                    "comp_min": comp_min,
                    "comp_max": comp_max,
                    "posting_age": job.get("posting_age"),
                    "job_url": job.get("job_url", "#"),
                    "source": job.get("source", ""),
                    "all_sources": all_sources,
                    "matched_keywords": matched_keywords,
                    "description_excerpt": description_excerpt,
                }
            )

        html_body = self.template.render(
            subject=f"\U0001f3af {total_new} New Roles - {run_date} | Bay Area Job Hunter",
            run_date=run_date,
            total_new=total_new,
            high_scoring_count=high_scoring_count,
            total_jobs_in_db=total_jobs_in_db,
            generation_timestamp=generation_timestamp,
            jobs=jobs_context,
        )

        subject = f"\U0001f3af {total_new} New Roles - {run_date} | Bay Area Job Hunter"
        return subject, html_body

    # ------------------------------------------------------------------
    # Delivery
    # ------------------------------------------------------------------

    def send_email(self, subject: str, html_body: str) -> bool:
        """Send the digest email via the configured method.

        Returns True on success, False on failure.
        """
        method = self.email_method

        if method == "gmail":
            return self._send_via_gmail(subject, html_body)
        elif method == "resend":
            return self._send_via_resend(subject, html_body)
        else:
            console.print(
                f"[yellow]DigestBuilder: unknown EMAIL_METHOD '{method}'. "
                "Skipping send.[/yellow]"
            )
            return False

    def _send_via_gmail(self, subject: str, html_body: str) -> bool:
        """Send via Gmail SMTP using an App Password."""
        if not self.gmail_address or not self.gmail_app_password:
            console.print(
                "[red]DigestBuilder (Gmail): GMAIL_ADDRESS or GMAIL_APP_PASSWORD "
                "not set. Cannot send.[/red]"
            )
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.gmail_address
            msg["To"] = self.digest_recipient

            # Plain-text fallback
            plain_text = (
                f"{subject}\n\n"
                "This email requires an HTML-capable client.\n"
                "Please view it in a modern email application."
            )
            msg.attach(MIMEText(plain_text, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(self.gmail_address, self.gmail_app_password)
                server.sendmail(
                    self.gmail_address,
                    self.digest_recipient,
                    msg.as_string(),
                )

            console.print(
                f"[green]DigestBuilder: digest sent via Gmail to "
                f"{self.digest_recipient}[/green]"
            )
            return True

        except smtplib.SMTPAuthenticationError:
            console.print(
                "[red]DigestBuilder (Gmail): authentication failed. "
                "Check GMAIL_ADDRESS and GMAIL_APP_PASSWORD.[/red]"
            )
        except smtplib.SMTPException as exc:
            console.print(f"[red]DigestBuilder (Gmail): SMTP error: {exc}[/red]")
        except Exception as exc:
            console.print(f"[red]DigestBuilder (Gmail): unexpected error: {exc}[/red]")

        return False

    def _send_via_resend(self, subject: str, html_body: str) -> bool:
        """Send via the Resend transactional email API."""
        if not self.resend_api_key:
            console.print(
                "[red]DigestBuilder (Resend): RESEND_API_KEY not set. "
                "Cannot send.[/red]"
            )
            return False

        try:
            response = requests.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {self.resend_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": self.resend_from,
                    "to": [self.digest_recipient],
                    "subject": subject,
                    "html": html_body,
                },
                timeout=30,
            )
            response.raise_for_status()

            console.print(
                f"[green]DigestBuilder: digest sent via Resend to "
                f"{self.digest_recipient}[/green]"
            )
            return True

        except requests.exceptions.HTTPError as exc:
            console.print(
                f"[red]DigestBuilder (Resend): HTTP {exc.response.status_code} "
                f"- {exc.response.text}[/red]"
            )
        except requests.exceptions.ConnectionError:
            console.print(
                "[red]DigestBuilder (Resend): connection error. "
                "Check network/API endpoint.[/red]"
            )
        except requests.exceptions.Timeout:
            console.print("[red]DigestBuilder (Resend): request timed out.[/red]")
        except Exception as exc:
            console.print(f"[red]DigestBuilder (Resend): unexpected error: {exc}[/red]")

        return False

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    def save_digest(self, html_body: str, date_str: str = None) -> Path:
        """Save *html_body* to data/digests/digest_YYYY-MM-DD.html.

        Creates the directory if it does not exist.
        Returns the Path of the saved file.
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        self._DIGESTS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = self._DIGESTS_DIR / f"digest_{date_str}.html"

        output_path.write_text(html_body, encoding="utf-8")
        console.print(f"[dim]DigestBuilder: digest saved to {output_path}[/dim]")
        return output_path

    def send_test_email(self) -> bool:
        """Send a simple test email to verify delivery config is working."""
        subject = "✅ Bay Area Job Hunter — Test Email"
        html_body = """
        <html><body style="font-family:sans-serif;padding:20px;">
        <h2>✅ Email delivery is working!</h2>
        <p>Your Bay Area Job Hunter is configured correctly.</p>
        <p>You'll receive daily digests at this address.</p>
        </body></html>
        """
        return self.send_email(subject, html_body)

    # ------------------------------------------------------------------
    # Orchestrate
    # ------------------------------------------------------------------

    def deliver(self, jobs_df: pd.DataFrame) -> bool:
        """Build, send (best-effort), and always save the digest.

        Returns True if the email was sent successfully, False otherwise.
        The HTML backup is always written regardless of send outcome.
        """
        console.print("[bold]DigestBuilder: building digest...[/bold]")
        subject, html_body = self.build_digest(jobs_df)
        console.print(f"[dim]Subject: {subject}[/dim]")

        sent = self.send_email(subject, html_body)
        if not sent:
            console.print(
                "[yellow]DigestBuilder: email delivery failed or skipped. "
                "Falling back to local save.[/yellow]"
            )

        date_str = datetime.now().strftime("%Y-%m-%d")
        self.save_digest(html_body, date_str=date_str)

        return sent
