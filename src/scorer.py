import re
import json
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
from rich.console import Console

console = Console()

WORD_TO_NUM = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}

STRONG_MATCH_PATTERNS = [
    "strategy & operations",
    "strategy and operations",
    "business operations",
    "revenue operations",
    "strategic finance",
    "fp&a",
    "fpa",
    "chief of staff",
    "corporate strategy",
    "sales strategy",
    "go-to-market",
    "go to market",
    "bizops",
    "biz ops",
    "revops",
    "rev ops",
    "pricing strategy",
    "deal strategy",
    "merchant strategy",
    "gtm",
]

PARTIAL_MATCH_PATTERNS = [
    "operations",
    "strategy",
    "finance",
    "analyst",
    "associate",
    "manager",
]


class JobScorer:
    def __init__(
        self,
        scoring_config_path="config/scoring.yaml",
        companies_config_path="config/companies.yaml",
        candidate_profile_path="config/candidate_profile.yaml",
    ):
        self.scoring_config = self._load_yaml(scoring_config_path)
        self.companies_config = self._load_yaml(companies_config_path)
        self.candidate_profile = self._load_yaml(candidate_profile_path)
        self.company_tier_lookup = self._build_company_tier_lookup()
        self._build_keyword_sets()

    def _load_yaml(self, path: str) -> dict:
        config_path = Path(path)
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _build_keyword_sets(self):
        """Flatten candidate_profile.yaml competencies into scoring keyword sets."""
        competencies = self.candidate_profile.get("candidate", {}).get("core_competencies", {})
        background = self.candidate_profile.get("candidate", {}).get("background_signals", [])
        tech_skills = self.candidate_profile.get("candidate", {}).get("technical_skills", {})

        # All competency keywords (strategy, finance, ops, customer/growth)
        self.profile_keywords = []
        for category_keywords in competencies.values():
            self.profile_keywords.extend(category_keywords)

        # Background signals valued by employers (MBA, consulting, FP&A, etc.)
        self.background_signals = background

        # Technical tools
        self.tech_keywords = []
        for tool_list in tech_skills.values():
            self.tech_keywords.extend(tool_list)

    def _build_company_tier_lookup(self) -> dict:
        lookup = {}
        tier_map = {
            "tier_1": "tier_1",
            "tier_2": "tier_2",
            "tier_3": "tier_3",
        }
        for key, tier_name in tier_map.items():
            companies = self.companies_config.get(key, [])
            if isinstance(companies, list):
                for company in companies:
                    lookup[company.lower()] = tier_name
        # Also add blocklisted companies
        for company in self.companies_config.get("blocklist", []):
            lookup[company.lower()] = "blocklist"
        return lookup

    def _get_company_tier_score(self, tier_name: str) -> int:
        tier_scores = {
            "tier_1": 15,
            "tier_2": 12,
            "tier_3": 8,
            "tier_4": 4,
            "blocklist": 0,
        }
        return tier_scores.get(tier_name, 4)

    def _score_title(self, title: str) -> tuple[int, list]:
        if not title:
            return 0, []

        title_lower = title.lower()
        matched_keywords = []

        # Check reference role patterns from config first (30 pts)
        reference_patterns = self.scoring_config.get("reference_role_patterns", [])
        for pattern in reference_patterns:
            if pattern.lower() in title_lower:
                matched_keywords.append(pattern)
                return 30, matched_keywords

        # Check strong match patterns (20 pts)
        for pattern in STRONG_MATCH_PATTERNS:
            if pattern.lower() in title_lower:
                matched_keywords.append(pattern)
                return 20, matched_keywords

        # Check partial match patterns (10 pts)
        for pattern in PARTIAL_MATCH_PATTERNS:
            if pattern.lower() in title_lower:
                matched_keywords.append(pattern)
                return 10, matched_keywords

        return 0, matched_keywords

    def _score_description(self, description: str) -> tuple[int, list]:
        if not description:
            return 0, []

        desc_lower = description.lower()
        score = 0
        matched_keywords = []

        # ── Tier 1: Background signals (5 pts each, cap at 10) ──────────────
        # MBA mention
        if re.search(r"\bmba\b", desc_lower):
            score += 5
            matched_keywords.append("MBA")

        # Consulting / IB / PE background
        consulting_patterns = [
            r"\bconsulting\b",
            r"\binvestment banking\b",
            r"\bprivate equity\b",
            r"\bpe firm\b",
        ]
        for pat in consulting_patterns:
            if re.search(pat, desc_lower):
                score += 5
                matched_keywords.append(pat.replace(r"\b", "").replace("\\", ""))
                break

        # ── Tier 2: Core competency keywords from candidate profile (2 pts each, cap at 10) ──
        profile_hit_count = 0
        for keyword in self.profile_keywords:
            if profile_hit_count >= 5:
                break
            # Use word-boundary matching for short keywords, substring for phrases
            if " " in keyword:
                if keyword.lower() in desc_lower:
                    score += 2
                    matched_keywords.append(keyword)
                    profile_hit_count += 1
            else:
                if re.search(r"\b" + re.escape(keyword.lower()) + r"\b", desc_lower):
                    score += 2
                    matched_keywords.append(keyword)
                    profile_hit_count += 1

        # ── Tier 3: Technical skill matches from candidate profile (1 pt each, cap at 5) ──
        tech_hit_count = 0
        for skill in self.tech_keywords:
            if tech_hit_count >= 5:
                break
            if re.search(r"\b" + re.escape(skill.lower()) + r"\b", desc_lower):
                score += 1
                matched_keywords.append(skill)
                tech_hit_count += 1

        capped_score = min(score, 25)
        return capped_score, matched_keywords

    def _parse_yoe(self, description: str) -> int | None:
        if not description:
            return None

        desc_lower = description.lower()

        # Replace word numbers with digits for unified parsing
        for word, num in WORD_TO_NUM.items():
            desc_lower = re.sub(r"\b" + word + r"\b", str(num), desc_lower)

        found_years = []

        # "3+ years" or "3+ years of experience"
        matches = re.findall(r"(\d+)\s*\+\s*years?", desc_lower)
        found_years.extend(int(m) for m in matches)

        # "3-5 years" or "3 to 5 years"
        matches = re.findall(r"(\d+)\s*(?:-|to)\s*\d+\s*years?", desc_lower)
        found_years.extend(int(m) for m in matches)

        # "minimum 3 years" or "at least 3 years"
        matches = re.findall(
            r"(?:minimum|at least|min\.?)\s*(\d+)\s*years?", desc_lower
        )
        found_years.extend(int(m) for m in matches)

        # plain "3 years of experience" or "3 years experience"
        matches = re.findall(r"(\d+)\s*years?\s+(?:of\s+)?experience", desc_lower)
        found_years.extend(int(m) for m in matches)

        if found_years:
            return min(found_years)

        return None

    def _score_yoe(self, description: str) -> int:
        yoe = self._parse_yoe(description)

        if yoe is None:
            return 0

        # Pull sweet spot config if available
        sweet_spot = self.scoring_config.get("yoe_sweet_spot", {})
        min_sweet = sweet_spot.get("min", 3)
        max_sweet = sweet_spot.get("max", 6)

        if min_sweet <= yoe <= max_sweet:
            return 15
        elif (min_sweet - 1 <= yoe < min_sweet) or (max_sweet < yoe <= max_sweet + 2):
            return 10
        elif (max(0, min_sweet - 2) <= yoe < min_sweet - 1) or (
            max_sweet + 2 < yoe <= max_sweet + 4
        ):
            return 5
        else:
            return 0

    def _score_company(self, company: str) -> int:
        if not company:
            return 4

        company_lower = company.lower().strip()

        # Exact lookup
        if company_lower in self.company_tier_lookup:
            tier = self.company_tier_lookup[company_lower]
            return self._get_company_tier_score(tier)

        # Partial match (e.g. "DoorDash, Inc." should match "doordash")
        for known_company, tier in self.company_tier_lookup.items():
            if known_company in company_lower or company_lower in known_company:
                return self._get_company_tier_score(tier)

        # Default: unknown company
        return 4

    def _score_comp(self, comp_min, comp_max) -> int:
        ideal_min = self.scoring_config.get("comp_ideal_min", 100000)
        ideal_max = self.scoring_config.get("comp_ideal_max", 250000)

        has_min = comp_min is not None and not (
            isinstance(comp_min, float) and pd.isna(comp_min)
        )
        has_max = comp_max is not None and not (
            isinstance(comp_max, float) and pd.isna(comp_max)
        )

        if not has_min and not has_max:
            return 0

        try:
            if has_min:
                comp_min = float(comp_min)
            if has_max:
                comp_max = float(comp_max)
        except (ValueError, TypeError):
            return 0

        # Normalize: values under 1000 are likely expressed in thousands
        if has_min and comp_min < 1000:
            comp_min *= 1000
        if has_max and comp_max < 1000:
            comp_max *= 1000

        if has_min and has_max:
            in_range = comp_min >= ideal_min and comp_max <= ideal_max
            overlaps = comp_min <= ideal_max and comp_max >= ideal_min
            if in_range or overlaps:
                return 10
            else:
                return 5
        elif has_min or has_max:
            val = comp_min if has_min else comp_max
            if ideal_min <= val <= ideal_max:
                return 10
            else:
                return 5

        return 0

    def _score_freshness(self, date_posted) -> int:
        if date_posted is None or (
            isinstance(date_posted, float) and pd.isna(date_posted)
        ):
            return 0

        try:
            if isinstance(date_posted, str):
                # Try common ISO formats
                for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        posted_dt = datetime.strptime(date_posted, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    return 0
            elif isinstance(date_posted, datetime):
                posted_dt = date_posted
            elif hasattr(date_posted, "to_pydatetime"):
                posted_dt = date_posted.to_pydatetime()
            else:
                return 0
        except Exception:
            return 0

        now = datetime.now()
        # If posted_dt is timezone-aware, strip tz for comparison
        if hasattr(posted_dt, "tzinfo") and posted_dt.tzinfo is not None:
            posted_dt = posted_dt.replace(tzinfo=None)

        hours_ago = (now - posted_dt).total_seconds() / 3600

        freshness_config = self.scoring_config.get("freshness", {})
        freshness_brackets = [
            {"max_hours": freshness_config.get("max_hours_full_points", 6), "score": 5},
            {"max_hours": freshness_config.get("max_hours_most_points", 24), "score": 4},
            {"max_hours": freshness_config.get("max_hours_some_points", 72), "score": 3},
            {"max_hours": freshness_config.get("max_hours_few_points", 168), "score": 1},
        ]

        for bracket in sorted(freshness_brackets, key=lambda b: b["max_hours"]):
            if hours_ago <= bracket["max_hours"]:
                return bracket["score"]

        return 0

    def score_jobs(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        scores = []
        breakdowns = []

        for _, row in df.iterrows():
            title = row.get("title", "") or ""
            description = row.get("description", "") or ""
            company = row.get("company", "") or ""
            comp_min = row.get("comp_min", None)
            comp_max = row.get("comp_max", None)
            date_posted = row.get("date_posted", None)

            title_score, title_keywords = self._score_title(title)
            desc_score, desc_keywords = self._score_description(description)
            yoe_score = self._score_yoe(description)
            company_score = self._score_company(company)
            comp_score = self._score_comp(comp_min, comp_max)
            freshness_score = self._score_freshness(date_posted)

            total = (
                title_score
                + desc_score
                + yoe_score
                + company_score
                + comp_score
                + freshness_score
            )
            total = min(total, 100)

            breakdown = {
                "title_score": title_score,
                "title_keywords": title_keywords,
                "description_score": desc_score,
                "description_keywords": desc_keywords,
                "yoe_score": yoe_score,
                "company_score": company_score,
                "comp_score": comp_score,
                "freshness_score": freshness_score,
                "total": total,
            }

            scores.append(total)
            breakdowns.append(json.dumps(breakdown))

        df["score"] = scores
        df["score_breakdown"] = breakdowns

        # Log score distribution summary
        score_series = pd.Series(scores)
        console.print("\n[bold cyan]Score Distribution Summary[/bold cyan]")
        console.print(f"  Total jobs scored : {len(score_series)}")
        console.print(f"  Mean score        : {score_series.mean():.1f}")
        console.print(f"  Median score      : {score_series.median():.1f}")
        console.print(f"  Min score         : {score_series.min()}")
        console.print(f"  Max score         : {score_series.max()}")

        bins = [0, 20, 40, 60, 80, 101]
        labels = ["0-19", "20-39", "40-59", "60-79", "80-100"]
        buckets = pd.cut(score_series, bins=bins, labels=labels, right=False)
        console.print("\n[bold]Score Buckets:[/bold]")
        for label, count in buckets.value_counts().sort_index().items():
            bar = "#" * count
            console.print(f"  {label:>7}: {count:>4}  {bar}")
        console.print()

        return df
