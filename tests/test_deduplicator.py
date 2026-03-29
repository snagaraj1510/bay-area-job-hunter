"""
Tests for src.deduplicator.Deduplicator.

Run from the project root with:
    pytest tests/test_deduplicator.py
"""

import pytest
import pandas as pd

from src.deduplicator import Deduplicator
from src.storage import generate_job_id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_job(**kwargs) -> dict:
    """Return a job dict with sensible defaults, overridable via kwargs."""
    defaults = {
        "title": "Operations Associate",
        "company": "Acme Corp",
        "location": "San Francisco, CA",
        "description": "A typical operations role.",
        "job_url": "https://jobs.example.com/1",
        "comp_min": None,
        "comp_max": None,
    }
    defaults.update(kwargs)
    return defaults


# ---------------------------------------------------------------------------
# Fuzzy title / company matching
# ---------------------------------------------------------------------------

class TestFuzzyMatching:
    def setup_method(self):
        self.dedup = Deduplicator(threshold=85)

    def test_sr_analyst_matches_senior_analyst(self):
        """'Sr. Analyst' and 'Senior Analyst' are fuzzy duplicates."""
        df = pd.DataFrame([
            make_job(title="Sr. Analyst", company="Rubrik", location="Palo Alto, CA",
                     job_url="https://jobs.example.com/sr"),
            make_job(title="Senior Analyst", company="Rubrik", location="Palo Alto, CA",
                     job_url="https://jobs.example.com/senior"),
        ])
        result = self.dedup.deduplicate(df)
        assert len(result) == 1

    def test_doordash_variants_are_deduplicated(self):
        """'DoorDash', 'DoorDash USA', and 'DoorDash, Inc.' are the same company."""
        df = pd.DataFrame([
            make_job(title="Strategy Associate", company="DoorDash",
                     location="San Francisco, CA", job_url="https://a.com/1"),
            make_job(title="Strategy Associate", company="DoorDash USA",
                     location="San Francisco, CA", job_url="https://b.com/2"),
            make_job(title="Strategy Associate", company="DoorDash, Inc.",
                     location="San Francisco, CA", job_url="https://c.com/3"),
        ])
        result = self.dedup.deduplicate(df)
        assert len(result) == 1

    def test_completely_different_jobs_are_kept(self):
        """Two clearly different jobs must NOT be collapsed into one."""
        df = pd.DataFrame([
            make_job(title="Operations Associate", company="Rippling",
                     location="San Francisco, CA", job_url="https://a.com/1"),
            make_job(title="Strategic Finance Manager", company="DoorDash",
                     location="Palo Alto, CA", job_url="https://b.com/2"),
        ])
        result = self.dedup.deduplicate(df)
        assert len(result) == 2

    def test_same_title_different_companies_are_kept(self):
        """Same title but sufficiently different companies should NOT be deduplicated.

        'Stripe' vs 'Palantir Technologies' produces a fuzzy key ratio well below
        the 85-point threshold, so both records must survive deduplication.
        """
        df = pd.DataFrame([
            make_job(title="Operations Associate", company="Stripe",
                     location="San Francisco, CA", job_url="https://stripe.com/1"),
            make_job(title="Operations Associate", company="Palantir Technologies",
                     location="Palo Alto, CA", job_url="https://palantir.com/1"),
        ])
        result = self.dedup.deduplicate(df)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# More-complete record is kept
# ---------------------------------------------------------------------------

class TestCompletenessKeeping:
    def setup_method(self):
        self.dedup = Deduplicator(threshold=85)

    def test_record_with_description_preferred_over_empty(self):
        """When deduplicating, the row with a richer description should be kept."""
        sparse = make_job(
            title="Operations Associate", company="Acme Corp",
            location="San Francisco, CA", description="",
            job_url="https://sparse.com/1",
        )
        rich = make_job(
            title="Operations Associate", company="Acme Corp",
            location="San Francisco, CA",
            description="A very detailed job description with lots of content about the role.",
            job_url="https://rich.com/2",
        )
        df = pd.DataFrame([sparse, rich])
        result = self.dedup.deduplicate(df)
        assert len(result) == 1
        assert result.loc[0, "description"] == rich["description"]

    def test_record_with_comp_data_preferred(self):
        """Row with comp data is more complete and should be kept as representative."""
        no_comp = make_job(
            title="Senior Analyst", company="Rubrik",
            location="Palo Alto, CA", comp_min=None, comp_max=None,
            job_url="https://a.com/1",
        )
        with_comp = make_job(
            title="Senior Analyst", company="Rubrik",
            location="Palo Alto, CA", comp_min=130000, comp_max=160000,
            job_url="https://b.com/2",
        )
        df = pd.DataFrame([no_comp, with_comp])
        result = self.dedup.deduplicate(df)
        assert len(result) == 1
        assert result.loc[0, "comp_min"] == 130000


# ---------------------------------------------------------------------------
# Exact duplicate removal
# ---------------------------------------------------------------------------

class TestExactDeduplication:
    def setup_method(self):
        self.dedup = Deduplicator(threshold=85)

    def test_exact_duplicates_are_collapsed(self):
        row = make_job(title="Revenue Ops Manager", company="Rippling",
                       location="San Francisco, CA")
        df = pd.DataFrame([row, row.copy(), row.copy()])
        result = self.dedup.deduplicate(df)
        assert len(result) == 1

    def test_case_insensitive_exact_dedup(self):
        """Titles that differ only in case should be treated as exact duplicates."""
        df = pd.DataFrame([
            make_job(title="Operations Associate", company="Acme Corp",
                     location="San Francisco, CA"),
            make_job(title="OPERATIONS ASSOCIATE", company="Acme Corp",
                     location="San Francisco, CA"),
        ])
        result = self.dedup.deduplicate(df)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# seen_ids filtering
# ---------------------------------------------------------------------------

class TestSeenIdsFilter:
    def setup_method(self):
        self.dedup = Deduplicator(threshold=85)

    def test_seen_job_is_excluded(self):
        """A job whose generated ID is in seen_ids must be removed from output."""
        job = make_job(title="Chief of Staff", company="Roblox",
                       location="San Mateo, CA")
        job_id = generate_job_id(job["title"], job["company"], job["location"])
        df = pd.DataFrame([job])
        result = self.dedup.deduplicate(df, seen_ids={job_id})
        assert len(result) == 0

    def test_unseen_job_is_kept(self):
        job = make_job(title="Chief of Staff", company="Roblox",
                       location="San Mateo, CA")
        df = pd.DataFrame([job])
        result = self.dedup.deduplicate(df, seen_ids={"some-other-id-that-does-not-match"})
        assert len(result) == 1

    def test_partial_seen_filtering(self):
        """Only the job matching seen_ids is removed; others remain."""
        job_a = make_job(title="Operations Associate", company="Stripe",
                         location="San Francisco, CA", job_url="https://a.com")
        job_b = make_job(title="Strategic Finance Analyst", company="Airbnb",
                         location="San Francisco, CA", job_url="https://b.com")
        id_a = generate_job_id(job_a["title"], job_a["company"], job_a["location"])
        df = pd.DataFrame([job_a, job_b])
        result = self.dedup.deduplicate(df, seen_ids={id_a})
        assert len(result) == 1
        assert result.loc[0, "company"] == "Airbnb"


# ---------------------------------------------------------------------------
# all_sources URL aggregation
# ---------------------------------------------------------------------------

class TestAllSourcesAggregation:
    def setup_method(self):
        self.dedup = Deduplicator(threshold=85)

    def test_all_sources_aggregates_urls_from_duplicates(self):
        """all_sources must contain both URLs when two variants are merged."""
        df = pd.DataFrame([
            make_job(title="Sr. Analyst", company="Rubrik",
                     location="Palo Alto, CA", job_url="https://linkedin.com/job/1"),
            make_job(title="Senior Analyst", company="Rubrik",
                     location="Palo Alto, CA", job_url="https://greenhouse.com/job/2"),
        ])
        result = self.dedup.deduplicate(df)
        assert len(result) == 1
        all_sources = result.loc[0, "all_sources"]
        assert "linkedin.com" in all_sources
        assert "greenhouse.com" in all_sources

    def test_all_sources_deduplicates_same_url(self):
        """When duplicates share the same URL, it should appear only once."""
        shared_url = "https://jobs.example.com/42"
        df = pd.DataFrame([
            make_job(title="Operations Associate", company="Acme",
                     location="San Francisco, CA", job_url=shared_url),
            make_job(title="Operations Associate", company="Acme",
                     location="San Francisco, CA", job_url=shared_url),
        ])
        result = self.dedup.deduplicate(df)
        assert len(result) == 1
        sources = result.loc[0, "all_sources"]
        assert sources.count(shared_url) == 1

    def test_empty_dataframe_returns_empty(self):
        result = self.dedup.deduplicate(pd.DataFrame())
        assert result.empty

    def test_internal_columns_are_removed(self):
        """Temporary internal columns (_norm_title, _city, etc.) must not leak into output."""
        df = pd.DataFrame([make_job()])
        result = self.dedup.deduplicate(df)
        internal = [c for c in result.columns if c.startswith("_")]
        assert internal == [], f"Internal columns leaked: {internal}"
