"""
Tests for src.scorer.JobScorer.

Run from the project root with:
    pytest tests/test_scorer.py

The scorer reads YAML configs from config/scoring.yaml and config/companies.yaml
at instantiation time, so tests that care about company tier scores rely on the
real config files being present (which they are in this repo).
"""

import json
import pytest
import pandas as pd
from datetime import datetime, timedelta

from src.scorer import JobScorer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def scorer():
    """Shared scorer instance (reads real config once per test module)."""
    return JobScorer(
        scoring_config_path="config/scoring.yaml",
        companies_config_path="config/companies.yaml",
    )


# ---------------------------------------------------------------------------
# Reference role tests (should each score >= 75)
# ---------------------------------------------------------------------------

REFERENCE_ROLES = [
    {
        "title": "Operations and Analytics Associate",
        "company": "Roblox",
        "location": "San Mateo, CA",
        "description": (
            "Join our strategy and operations team. You will own cross-functional "
            "projects, build financial models in Excel and SQL, drive go-to-market "
            "planning, and present insights to executive leadership and the board. "
            "3-5 years of experience in consulting or investment banking preferred. "
            "MBA a plus. Strategic planning and stakeholder management required."
        ),
        "comp_min": 120000,
        "comp_max": 160000,
        "date_posted": (datetime.utcnow() - timedelta(hours=2)).strftime("%Y-%m-%d"),
    },
    {
        "title": "Senior Analyst, Sales Strategy & Operations",
        "company": "Rubrik",
        "location": "Palo Alto, CA",
        "description": (
            "Drive sales strategy and operations initiatives across the GTM org. "
            "You will build financial models, manage cross-functional stakeholders, "
            "and own strategic planning processes. 3-5 years of experience required. "
            "Prior consulting or investment banking background strongly preferred."
        ),
        "comp_min": 130000,
        "comp_max": 170000,
        "date_posted": (datetime.utcnow() - timedelta(hours=10)).strftime("%Y-%m-%d"),
    },
    {
        "title": "Senior Associate, Merchant Strategy & Operations",
        "company": "DoorDash",
        "location": "San Francisco, CA",
        "description": (
            "Own the merchant strategy and operations roadmap. Work cross-functionally "
            "with product, finance, and sales. Build SQL models and Excel dashboards. "
            "Present to senior leadership and the board. 3+ years of experience preferred. "
            "MBA preferred. Consulting or private equity background a plus. "
            "GTM strategy and strategic planning experience valued."
        ),
        "comp_min": 125000,
        "comp_max": 165000,
        "date_posted": (datetime.utcnow() - timedelta(hours=5)).strftime("%Y-%m-%d"),
    },
    {
        "title": "Strategic Finance Senior Associate",
        "company": "Rippling",
        "location": "San Francisco, CA",
        "description": (
            "Join our strategic finance team to own FP&A and corporate strategy work. "
            "You will build long-range plans, drive cross-functional alignment, and "
            "create executive presentations for the board. "
            "4-6 years of experience in investment banking or consulting preferred. "
            "Financial modeling and SQL skills required."
        ),
        "comp_min": 140000,
        "comp_max": 180000,
        "date_posted": (datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%d"),
    },
]


@pytest.mark.parametrize("role", REFERENCE_ROLES, ids=[r["title"] for r in REFERENCE_ROLES])
def test_reference_roles_score_above_75(scorer, role):
    """Each reference role must achieve the 'highlight' threshold of 75+."""
    df = pd.DataFrame([role])
    result = scorer.score_jobs(df)
    score = result.loc[0, "score"]
    breakdown = json.loads(result.loc[0, "score_breakdown"])
    assert score >= 75, (
        f"Expected score >= 75 for '{role['title']}' at {role['company']}, "
        f"got {score}. Breakdown: {breakdown}"
    )


# ---------------------------------------------------------------------------
# Low-relevance roles should score below 40
# ---------------------------------------------------------------------------

LOW_RELEVANCE_ROLES = [
    {
        "title": "Customer Support Specialist",
        "company": "Unknown Startup",
        "location": "Austin, TX",
        "description": "Answer customer tickets and resolve billing issues.",
        "comp_min": None,
        "comp_max": None,
        "date_posted": (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d"),
    },
    {
        "title": "Junior Marketing Coordinator",
        "company": "Generic Agency",
        "location": "Remote",
        "description": "Create social media content and manage email campaigns.",
        "comp_min": 45000,
        "comp_max": 55000,
        "date_posted": (datetime.utcnow() - timedelta(days=20)).strftime("%Y-%m-%d"),
    },
]


@pytest.mark.parametrize("role", LOW_RELEVANCE_ROLES, ids=[r["title"] for r in LOW_RELEVANCE_ROLES])
def test_low_relevance_roles_score_below_40(scorer, role):
    df = pd.DataFrame([role])
    result = scorer.score_jobs(df)
    score = result.loc[0, "score"]
    assert score < 40, f"Expected score < 40 for '{role['title']}', got {score}"


# ---------------------------------------------------------------------------
# Title scoring unit tests
# ---------------------------------------------------------------------------

class TestTitleScoring:
    def setup_method(self):
        self.scorer = JobScorer(
            scoring_config_path="config/scoring.yaml",
            companies_config_path="config/companies.yaml",
        )

    def test_strategy_and_operations_scores_20(self):
        score, keywords = self.scorer._score_title("Strategy & Operations Associate")
        assert score == 20
        assert any("strategy" in kw.lower() and "operations" in kw.lower() for kw in keywords)

    def test_strategic_finance_scores_20(self):
        score, keywords = self.scorer._score_title("Strategic Finance Manager")
        assert score == 20

    def test_chief_of_staff_scores_20(self):
        score, keywords = self.scorer._score_title("Chief of Staff")
        assert score == 20

    def test_analyst_scores_10(self):
        score, _ = self.scorer._score_title("Business Analyst")
        assert score == 10

    def test_associate_scores_10(self):
        score, _ = self.scorer._score_title("Associate")
        assert score == 10

    def test_unrelated_title_scores_0(self):
        score, keywords = self.scorer._score_title("Hair Stylist")
        assert score == 0
        assert keywords == []

    def test_empty_title_scores_0(self):
        score, keywords = self.scorer._score_title("")
        assert score == 0


# ---------------------------------------------------------------------------
# Description keyword scoring unit tests
# ---------------------------------------------------------------------------

class TestDescriptionScoring:
    def setup_method(self):
        self.scorer = JobScorer(
            scoring_config_path="config/scoring.yaml",
            companies_config_path="config/companies.yaml",
        )

    def test_mba_mention_adds_points(self):
        score, keywords = self.scorer._score_description("MBA preferred.")
        assert score >= 5
        assert "MBA" in keywords

    def test_consulting_mention_adds_points(self):
        score, keywords = self.scorer._score_description(
            "Background in consulting or investment banking preferred."
        )
        assert score >= 5

    def test_sql_mention_adds_points(self):
        score, _ = self.scorer._score_description("Strong SQL skills required.")
        assert score >= 3

    def test_cross_functional_adds_points(self):
        score, _ = self.scorer._score_description(
            "Drive cross-functional alignment across engineering and sales."
        )
        assert score >= 3

    def test_empty_description_scores_0(self):
        score, keywords = self.scorer._score_description("")
        assert score == 0
        assert keywords == []

    def test_description_score_capped_at_25(self):
        rich_desc = (
            "MBA required. Consulting and investment banking background. "
            "Financial modeling, Excel, SQL. Cross-functional stakeholder management. "
            "Board-level executive presentations. GTM strategy. Strategic planning."
        )
        score, _ = self.scorer._score_description(rich_desc)
        assert score <= 25


# ---------------------------------------------------------------------------
# YOE parsing unit tests
# ---------------------------------------------------------------------------

class TestYOEParsing:
    def setup_method(self):
        self.scorer = JobScorer(
            scoring_config_path="config/scoring.yaml",
            companies_config_path="config/companies.yaml",
        )

    def test_parse_plus_format(self):
        assert self.scorer._parse_yoe("3+ years of experience") == 3

    def test_parse_range_format(self):
        assert self.scorer._parse_yoe("3-5 years experience") == 3

    def test_parse_to_range_format(self):
        assert self.scorer._parse_yoe("2 to 4 years of experience") == 2

    def test_parse_minimum_format(self):
        assert self.scorer._parse_yoe("Minimum 4 years required") == 4

    def test_parse_at_least_format(self):
        assert self.scorer._parse_yoe("At least 3 years of experience") == 3

    def test_parse_word_number(self):
        """Word numbers (e.g. 'three') should be converted before parsing."""
        result = self.scorer._parse_yoe("Three+ years of experience")
        assert result == 3

    def test_parse_no_yoe_returns_none(self):
        assert self.scorer._parse_yoe("No experience requirement mentioned.") is None

    def test_parse_empty_returns_none(self):
        assert self.scorer._parse_yoe("") is None

    def test_yoe_sweet_spot_scores_15(self):
        """3-6 years is the sweet spot and should return 15 points."""
        score = self.scorer._score_yoe("Requires 4+ years of experience")
        assert score == 15

    def test_yoe_outside_sweet_spot_scores_less(self):
        score = self.scorer._score_yoe("Requires 10+ years of experience")
        assert score < 15


# ---------------------------------------------------------------------------
# Company tier scoring unit tests
# ---------------------------------------------------------------------------

class TestCompanyTierScoring:
    def setup_method(self):
        self.scorer = JobScorer(
            scoring_config_path="config/scoring.yaml",
            companies_config_path="config/companies.yaml",
        )

    def test_tier1_company_scores_15(self):
        assert self.scorer._score_company("Google") == 15

    def test_tier2_company_scores_12(self):
        """DoorDash, Rubrik, Roblox, and Rippling are tier_2 (12 pts)."""
        for company in ("DoorDash", "Rubrik", "Roblox", "Rippling"):
            assert self.scorer._score_company(company) == 12, f"Expected 12 for {company}"

    def test_tier3_company_scores_8(self):
        assert self.scorer._score_company("HubSpot") == 8

    def test_unknown_company_scores_4(self):
        assert self.scorer._score_company("Random Startup XYZ") == 4

    def test_partial_company_name_matches(self):
        """'DoorDash, Inc.' should still resolve to tier_2."""
        score = self.scorer._score_company("DoorDash, Inc.")
        assert score == 12


# ---------------------------------------------------------------------------
# Comp range scoring unit tests
# ---------------------------------------------------------------------------

class TestCompScoring:
    def setup_method(self):
        self.scorer = JobScorer(
            scoring_config_path="config/scoring.yaml",
            companies_config_path="config/companies.yaml",
        )

    def test_in_range_scores_10(self):
        assert self.scorer._score_comp(120000, 180000) == 10

    def test_values_expressed_in_thousands_normalized(self):
        """Values < 1000 should be treated as thousands (e.g. 120 -> 120,000)."""
        assert self.scorer._score_comp(120, 180) == 10

    def test_no_comp_data_scores_0(self):
        assert self.scorer._score_comp(None, None) == 0

    def test_out_of_range_scores_5(self):
        """Very high comp that does not overlap ideal range still scores 5."""
        assert self.scorer._score_comp(300000, 500000) == 5

    def test_score_jobs_adds_score_column(self):
        df = pd.DataFrame([{
            "title": "Operations Associate",
            "company": "Stripe",
            "location": "San Francisco, CA",
            "description": "3+ years of experience in strategy and operations.",
            "comp_min": 130000,
            "comp_max": 160000,
            "date_posted": datetime.utcnow().strftime("%Y-%m-%d"),
        }])
        result = self.scorer.score_jobs(df)
        assert "score" in result.columns
        assert "score_breakdown" in result.columns
        assert result.loc[0, "score"] > 0
