"""
Hard filters that remove jobs before scoring.
All filters should be configurable via config/search_queries.yaml.

CRITICAL GUARDRAILS:
- Never auto-apply to any job
- Never submit any personal information anywhere
- Never interact with any ATS or application system
- This tool is READ-ONLY: scrape, filter, score, display
"""

import re

import pandas as pd
from rich.console import Console

BAY_AREA_CITIES = [
    "san francisco", "sf", "south san francisco", "daly city",
    "san mateo", "redwood city", "menlo park", "palo alto",
    "mountain view", "sunnyvale", "santa clara", "san jose",
    "cupertino", "milpitas", "fremont", "oakland", "berkeley",
    "emeryville", "walnut creek", "pleasanton", "foster city",
    "burlingame", "san bruno", "san carlos",
]
BAY_AREA_PATTERNS = ["bay area", "sf bay", "san francisco bay area"]

SENIORITY_EXCLUDE = [
    r"\bintern\b", r"\binternship\b",
    r"\bjunior\b", r"\bjr\b", r"\bjr\.\b",
    r"\bentry.level\b", r"\bentry level\b",
    r"\bdirector\b", r"\bvp\b", r"\bvice.president\b",
    r"\bchief\b(?!.*staff)",
    r"\bhead of\b",
    r"\bfellow\b",
    r"(?<!chief of )\bstaff\b",
]

COMPANY_BLOCKLIST = [
    "robert half", "randstad", "adecco", "manpower", "kelly services",
    "hays", "kforce", "insight global", "teksystems", "apex systems",
    "jobvite", "jobot", "cybercoders", "manpowergroup",
]

TITLE_BLOCKLIST_PATTERNS = [
    r"\bengineering\b", r"\bengineer\b",
    r"\bdata scientist\b",
    r"\bdesign\b(?!.*strategy)",
    r"\bmarketing\b(?!.*strategy|.*operations)",
    r"\bnursing\b", r"\bclinical\b",
    r"\bsales rep\b", r"\baccount executive\b",
    r"\brecruiter\b", r"\brecruiting\b",
    r"\bcustomer (support|service)\b",
]


class JobFilter:
    def __init__(self):
        self.console = Console()

        self._seniority_patterns = [
            re.compile(p, re.IGNORECASE) for p in SENIORITY_EXCLUDE
        ]
        self._title_blocklist_patterns = [
            re.compile(p, re.IGNORECASE) for p in TITLE_BLOCKLIST_PATTERNS
        ]

    def _is_bay_area(self, location: str) -> bool:
        if not isinstance(location, str):
            return False

        loc_lower = location.lower()

        for city in BAY_AREA_CITIES:
            if city in loc_lower:
                return True

        for pattern in BAY_AREA_PATTERNS:
            if pattern in loc_lower:
                return True

        if "remote" in loc_lower:
            if "bay area" in loc_lower or ", ca" in loc_lower or "california" in loc_lower:
                return True

        return False

    def _is_excluded_seniority(self, title: str) -> bool:
        if not isinstance(title, str):
            return False

        for pattern in self._seniority_patterns:
            if pattern.search(title):
                return True

        return False

    def _is_blocked_company(self, company: str) -> bool:
        if not isinstance(company, str):
            return False

        company_lower = company.lower()

        for blocked in COMPANY_BLOCKLIST:
            if blocked in company_lower:
                return True

        return False

    def _is_blocked_title(self, title: str) -> bool:
        if not isinstance(title, str):
            return False

        for pattern in self._title_blocklist_patterns:
            if pattern.search(title):
                return True

        return False

    def filter_jobs(self, df: pd.DataFrame) -> pd.DataFrame:
        initial_count = len(df)
        self.console.print(f"[bold]Starting filters with {initial_count} jobs[/bold]")

        # 1. Remove rows with missing title or company
        df = df.dropna(subset=["title", "company"])
        removed = initial_count - len(df)
        if removed:
            self.console.print(
                f"  [yellow]Missing title/company:[/yellow] removed {removed}, {len(df)} remaining"
            )

        # 2. Apply location filter (keep Bay Area only)
        before = len(df)
        if "location" in df.columns:
            df = df[df["location"].apply(self._is_bay_area)]
        else:
            self.console.print("  [dim]No 'location' column found; skipping location filter[/dim]")
        removed = before - len(df)
        self.console.print(
            f"  [yellow]Location filter:[/yellow] removed {removed}, {len(df)} remaining"
        )

        # 3. Apply seniority exclusion
        before = len(df)
        df = df[~df["title"].apply(self._is_excluded_seniority)]
        removed = before - len(df)
        self.console.print(
            f"  [yellow]Seniority exclusion:[/yellow] removed {removed}, {len(df)} remaining"
        )

        # 4. Apply company blocklist
        before = len(df)
        df = df[~df["company"].apply(self._is_blocked_company)]
        removed = before - len(df)
        self.console.print(
            f"  [yellow]Company blocklist:[/yellow] removed {removed}, {len(df)} remaining"
        )

        # 5. Apply title blocklist
        before = len(df)
        df = df[~df["title"].apply(self._is_blocked_title)]
        removed = before - len(df)
        self.console.print(
            f"  [yellow]Title blocklist:[/yellow] removed {removed}, {len(df)} remaining"
        )

        total_removed = initial_count - len(df)
        self.console.print(
            f"[bold green]Filtering complete:[/bold green] {total_removed} jobs removed total, "
            f"{len(df)} jobs remaining"
        )

        return df.reset_index(drop=True)
