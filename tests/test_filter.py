"""
Tests for src.filter.JobFilter.

Run from the project root with:
    pytest tests/test_filter.py
"""

import pytest
import pandas as pd
from src.filter import JobFilter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_df(**kwargs) -> pd.DataFrame:
    """Build a single-row DataFrame.  Any column can be overridden via kwargs."""
    defaults = {
        "title": "Operations Associate",
        "company": "Acme Corp",
        "location": "San Francisco, CA",
    }
    defaults.update(kwargs)
    return pd.DataFrame([defaults])


# ---------------------------------------------------------------------------
# Seniority exclusion tests
# ---------------------------------------------------------------------------

class TestSeniorityExclusion:
    def setup_method(self):
        self.jf = JobFilter()

    def test_junior_is_excluded(self):
        assert self.jf._is_excluded_seniority("Junior Operations Analyst") is True

    def test_intern_is_excluded(self):
        assert self.jf._is_excluded_seniority("Operations Intern") is True

    def test_internship_is_excluded(self):
        assert self.jf._is_excluded_seniority("Strategy Internship") is True

    def test_vp_is_excluded(self):
        assert self.jf._is_excluded_seniority("VP of Finance") is True

    def test_director_is_excluded(self):
        assert self.jf._is_excluded_seniority("Director of Operations") is True

    def test_head_of_is_excluded(self):
        assert self.jf._is_excluded_seniority("Head of Strategy") is True

    def test_chief_without_staff_is_excluded(self):
        assert self.jf._is_excluded_seniority("Chief Revenue Officer") is True

    def test_chief_of_staff_is_NOT_excluded(self):
        """'Chief of Staff' must pass seniority because of the lookahead guard."""
        assert self.jf._is_excluded_seniority("Chief of Staff") is False

    def test_senior_chief_of_staff_is_NOT_excluded(self):
        assert self.jf._is_excluded_seniority("Senior Chief of Staff, GTM") is False

    def test_jr_abbreviation_is_excluded(self):
        assert self.jf._is_excluded_seniority("Jr. Business Analyst") is True

    def test_non_seniority_title_passes(self):
        assert self.jf._is_excluded_seniority("Strategy & Operations Associate") is False


# ---------------------------------------------------------------------------
# Bay Area location tests
# ---------------------------------------------------------------------------

class TestBayAreaLocation:
    def setup_method(self):
        self.jf = JobFilter()

    def test_san_francisco_passes(self):
        assert self.jf._is_bay_area("San Francisco, CA") is True

    def test_menlo_park_passes(self):
        assert self.jf._is_bay_area("Menlo Park, CA") is True

    def test_san_jose_passes(self):
        assert self.jf._is_bay_area("San Jose, CA") is True

    def test_new_york_fails(self):
        assert self.jf._is_bay_area("New York, NY") is False

    def test_remote_with_bay_area_passes(self):
        assert self.jf._is_bay_area("Remote - Bay Area preferred") is True

    def test_remote_with_ca_passes(self):
        assert self.jf._is_bay_area("Remote, CA") is True

    def test_remote_with_california_passes(self):
        assert self.jf._is_bay_area("Remote (California)") is True

    def test_plain_remote_fails(self):
        """Remote without any Bay Area or CA qualifier should be excluded."""
        assert self.jf._is_bay_area("Remote") is False

    def test_non_string_returns_false(self):
        assert self.jf._is_bay_area(None) is False
        assert self.jf._is_bay_area(42) is False


# ---------------------------------------------------------------------------
# Company blocklist tests
# ---------------------------------------------------------------------------

class TestCompanyBlocklist:
    def setup_method(self):
        self.jf = JobFilter()

    def test_robert_half_is_blocked(self):
        assert self.jf._is_blocked_company("Robert Half") is True

    def test_insight_global_is_blocked(self):
        assert self.jf._is_blocked_company("Insight Global") is True

    def test_jobot_is_blocked(self):
        assert self.jf._is_blocked_company("Jobot") is True

    def test_legitimate_company_passes(self):
        assert self.jf._is_blocked_company("DoorDash") is False

    def test_non_string_returns_false(self):
        assert self.jf._is_blocked_company(None) is False


# ---------------------------------------------------------------------------
# Title blocklist tests
# ---------------------------------------------------------------------------

class TestTitleBlocklist:
    def setup_method(self):
        self.jf = JobFilter()

    def test_engineer_is_blocked(self):
        assert self.jf._is_blocked_title("Software Engineer") is True

    def test_engineering_is_blocked(self):
        assert self.jf._is_blocked_title("Engineering Manager") is True

    def test_data_scientist_is_blocked(self):
        assert self.jf._is_blocked_title("Senior Data Scientist") is True

    def test_recruiter_is_blocked(self):
        assert self.jf._is_blocked_title("Technical Recruiter") is True

    def test_account_executive_is_blocked(self):
        assert self.jf._is_blocked_title("Account Executive") is True

    def test_customer_support_is_blocked(self):
        assert self.jf._is_blocked_title("Customer Support Specialist") is True

    def test_marketing_alone_is_blocked(self):
        assert self.jf._is_blocked_title("Marketing Manager") is True

    def test_marketing_strategy_is_NOT_blocked(self):
        assert self.jf._is_blocked_title("Marketing Strategy Associate") is False

    def test_marketing_operations_is_NOT_blocked(self):
        assert self.jf._is_blocked_title("Marketing Operations Lead") is False

    def test_design_alone_is_blocked(self):
        assert self.jf._is_blocked_title("Product Design Lead") is True

    def test_design_strategy_is_NOT_blocked(self):
        assert self.jf._is_blocked_title("Design Strategy Analyst") is False

    def test_operations_associate_passes(self):
        assert self.jf._is_blocked_title("Operations Associate") is False


# ---------------------------------------------------------------------------
# Integration-style filter_jobs tests
# ---------------------------------------------------------------------------

class TestFilterJobs:
    def setup_method(self):
        self.jf = JobFilter()

    def _base_jobs(self) -> pd.DataFrame:
        """Return a small DataFrame with a variety of jobs to filter."""
        return pd.DataFrame([
            {
                "title": "Strategy & Operations Associate",
                "company": "Rippling",
                "location": "San Francisco, CA",
            },
            {
                "title": "Junior Analyst",
                "company": "Acme Corp",
                "location": "San Francisco, CA",
            },
            {
                "title": "Senior Software Engineer",
                "company": "Stripe",
                "location": "San Francisco, CA",
            },
            {
                "title": "Chief of Staff",
                "company": "Roblox",
                "location": "San Mateo, CA",
            },
            {
                "title": "Operations Analyst",
                "company": "Robert Half",
                "location": "Menlo Park, CA",
            },
            {
                "title": "Business Operations Manager",
                "company": "DoorDash",
                "location": "New York, NY",
            },
            {
                "title": "Revenue Operations Associate",
                "company": "Rubrik",
                "location": "Remote, CA",
            },
        ])

    def test_filter_removes_junior(self):
        df = self._base_jobs()
        result = self.jf.filter_jobs(df)
        assert "Junior Analyst" not in result["title"].values

    def test_filter_removes_engineer(self):
        df = self._base_jobs()
        result = self.jf.filter_jobs(df)
        assert "Senior Software Engineer" not in result["title"].values

    def test_filter_removes_blocked_company(self):
        df = self._base_jobs()
        result = self.jf.filter_jobs(df)
        assert "Robert Half" not in result["company"].values

    def test_filter_removes_non_bay_area(self):
        df = self._base_jobs()
        result = self.jf.filter_jobs(df)
        assert "New York, NY" not in result["location"].values

    def test_filter_keeps_chief_of_staff(self):
        df = self._base_jobs()
        result = self.jf.filter_jobs(df)
        assert "Chief of Staff" in result["title"].values

    def test_filter_keeps_remote_ca(self):
        df = self._base_jobs()
        result = self.jf.filter_jobs(df)
        assert "Remote, CA" in result["location"].values

    def test_filter_keeps_strategy_operations(self):
        df = self._base_jobs()
        result = self.jf.filter_jobs(df)
        assert "Strategy & Operations Associate" in result["title"].values

    def test_filter_drops_rows_missing_title(self):
        df = pd.DataFrame([
            {"title": None, "company": "Acme", "location": "San Francisco, CA"},
            {"title": "Operations Associate", "company": "Acme", "location": "San Francisco, CA"},
        ])
        result = self.jf.filter_jobs(df)
        assert len(result) == 1

    def test_filter_returns_reset_index(self):
        df = self._base_jobs()
        result = self.jf.filter_jobs(df)
        assert list(result.index) == list(range(len(result)))
