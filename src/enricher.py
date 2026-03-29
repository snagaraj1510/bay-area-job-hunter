import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
from rich.console import Console

console = Console()


class JobEnricher:
    def __init__(self, companies_config_path="config/companies.yaml"):
        self.companies_config = self._load_yaml(companies_config_path)
        self._tier_map = self._build_tier_map()

    def _load_yaml(self, path: str) -> dict:
        config_path = Path(path)
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _build_tier_map(self) -> list[tuple[str, str]]:
        """Build an ordered list of (company_lower, tier_label) pairs for matching."""
        entries = []
        tier_keys = {
            "tier_1": "Tier 1",
            "tier_2": "Tier 2",
            "tier_3": "Tier 3",
            "tier_4": "Tier 4",
            "blocklist": "Blocklist",
        }
        for key, label in tier_keys.items():
            companies = self.companies_config.get(key, [])
            if isinstance(companies, list):
                for company in companies:
                    entries.append((company.lower(), label))
        return entries

    def _get_company_tier(self, company: str) -> str:
        """Return tier label for a company using case-insensitive partial matching."""
        if not company:
            return "Tier 4"

        company_lower = company.lower().strip()

        for known_company, label in self._tier_map:
            if known_company in company_lower or company_lower in known_company:
                return label

        return "Tier 4"

    def _get_posting_age(self, date_posted) -> str:
        """Return a human-readable age string for a posting date."""
        # Handle None / NaN / NaT
        if date_posted is None:
            return "Unknown"
        try:
            if pd.isnull(date_posted):
                return "Unknown"
        except (TypeError, ValueError):
            pass

        try:
            if isinstance(date_posted, str):
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        posted_dt = datetime.strptime(date_posted, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    return "Unknown"
            elif isinstance(date_posted, datetime):
                posted_dt = date_posted
            elif hasattr(date_posted, "to_pydatetime"):
                posted_dt = date_posted.to_pydatetime()
            else:
                return "Unknown"
        except Exception:
            return "Unknown"

        # Strip timezone info for comparison with utcnow
        if hasattr(posted_dt, "tzinfo") and posted_dt.tzinfo is not None:
            posted_dt = posted_dt.replace(tzinfo=None)

        now = datetime.utcnow()
        delta = now - posted_dt
        total_seconds = delta.total_seconds()

        if total_seconds < 0:
            return "Unknown"

        hours = total_seconds / 3600
        days = total_seconds / 86400

        if hours < 1:
            minutes = int(total_seconds / 60)
            if minutes <= 1:
                return "Just now"
            return f"{minutes} minutes ago"
        elif hours < 2:
            return "1 hour ago"
        elif hours < 24:
            return f"{int(hours)} hours ago"
        elif days < 2:
            return "1 day ago"
        else:
            return f"{int(days)} days ago"

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add enrichment columns to a scored jobs DataFrame."""
        df = df.copy()

        # Ensure job_url column exists
        if "job_url" not in df.columns:
            df["job_url"] = ""

        # Add company_tier
        df["company_tier"] = df.get("company", pd.Series([""] * len(df))).apply(
            lambda c: self._get_company_tier(c if isinstance(c, str) else "")
        )

        # Add posting_age
        if "date_posted" in df.columns:
            df["posting_age"] = df["date_posted"].apply(self._get_posting_age)
        else:
            df["posting_age"] = "Unknown"

        # Log enrichment summary
        console.print("\n[bold cyan]Enrichment Summary[/bold cyan]")
        console.print(f"  Total jobs enriched : {len(df)}")

        tier_counts = df["company_tier"].value_counts()
        console.print("\n[bold]Company Tier Breakdown:[/bold]")
        for tier in ["Tier 1", "Tier 2", "Tier 3", "Tier 4", "Blocklist"]:
            count = tier_counts.get(tier, 0)
            bar = "#" * count
            console.print(f"  {tier:<10}: {count:>4}  {bar}")

        age_counts = df["posting_age"].value_counts()
        console.print("\n[bold]Posting Age Distribution:[/bold]")
        for age, count in age_counts.items():
            console.print(f"  {age:<20}: {count}")

        console.print()

        return df
