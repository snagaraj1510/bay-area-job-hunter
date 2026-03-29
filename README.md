# Bay Area Job Hunter

> **Why I built this:** Manually sifting through job boards every day is slow, repetitive, and easy to let slip. I wanted a system that would do the legwork for me — scraping across multiple boards, filtering out noise, and surfacing only the roles most aligned to my background in FP&A, Strategy, and BizOps. So I built a daily pipeline that scores and ranks every new posting against my actual skillset and delivers a prioritized digest to my inbox each morning.

A Python CLI that scrapes Bay Area job postings daily across 5 job boards, scores each role against a candidate profile, and delivers a ranked HTML digest by email. Runs automatically via GitHub Actions — no laptop required.

## Pipeline

```
Scrape (JobSpy) → Deduplicate (fuzzy match) → Filter (seniority/location/blocklist)
    → Score (0–100 rubric) → Enrich (company tier, posting age) → Digest (email)
```

## Features

- **Multi-board scraping** — Indeed, LinkedIn, ZipRecruiter, Google Jobs, Glassdoor via [python-jobspy](https://github.com/Bunsly/JobSpy)
- **Fuzzy deduplication** — rapidfuzz cross-source dedup (catches "Sr. Analyst" vs "Senior Analyst", "DoorDash Inc." vs "DoorDash")
- **Smart filtering** — hard filters for Bay Area location, seniority level, job type, and company blocklist
- **Profile-driven scoring** — 0–100 rubric using a `candidate_profile.yaml` (gitignored) that maps your actual skills and competencies to job description keywords
- **Company tier scoring** — configurable tier list weights Tier 1 (Google, Stripe, Anthropic) vs Tier 4 unknowns
- **HTML email digest** — top 15–25 roles ranked by score, with comp range, freshness, matched keywords, and source badges
- **SQLite persistence** — tracks seen jobs, avoids re-sending duplicates, stores full score breakdowns
- **CLI** — `run`, `scrape`, `score`, `digest`, `export`, `stats`, `mark` commands

## Scoring Rubric

| Dimension | Max Points | What it measures |
|---|---|---|
| Title match | 30 | Role type alignment (Strategy & Ops, FP&A, BizOps, etc.) |
| Description keywords | 25 | Profile competencies found in JD (MBA signal, consulting/IB, GTM, modeling, etc.) |
| YOE fit | 15 | Years of experience required vs. candidate sweet spot |
| Company quality | 15 | Tier 1–4 company list |
| Comp range | 10 | Whether stated comp overlaps target range |
| Posting freshness | 5 | Posted today > this week |

## Quick Start

### 1. Install

```bash
git clone https://github.com/snagaraj1510/bay-area-job-hunter.git
cd bay-area-job-hunter
uv sync   # or: pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Fill in: GMAIL_ADDRESS, GMAIL_APP_PASSWORD, DIGEST_RECIPIENT
```

Create your `config/candidate_profile.yaml` (gitignored — see structure in `config/` for other YAML examples) with your skills, competencies, and background signals.

Edit `config/companies.yaml` to add your target companies to the tier list.

### 3. Run

```bash
# First run — 7-day backfill
python -m src.main run --backfill

# Daily run
python -m src.main run

# Other commands
python -m src.main scrape          # scrape only, no digest
python -m src.main stats           # view score distribution
python -m src.main export --min-score 70 --output top_jobs.csv
python -m src.main mark <job_id> --status applied
python -m src.main test-email      # verify delivery
```

## GitHub Actions (Automated)

The workflow runs daily at 7am PT. Add these secrets to your repo:

| Secret | Value |
|---|---|
| `ANTHROPIC_API_KEY` | Optional — for Phase 2 LLM scoring |
| `GMAIL_ADDRESS` | Sending Gmail address |
| `GMAIL_APP_PASSWORD` | Gmail App Password (not your main password) |
| `DIGEST_RECIPIENT` | Where to send the digest |

## Configuration Files

| File | Purpose |
|---|---|
| `config/search_queries.yaml` | Search terms, locations, job board settings |
| `config/scoring.yaml` | Scoring weights, thresholds, freshness brackets |
| `config/companies.yaml` | Company tier list and blocklist |
| `config/candidate_profile.yaml` | **Gitignored** — your personal skills and competencies |

## Tech Stack

- **Scraping:** [python-jobspy](https://github.com/Bunsly/JobSpy)
- **Deduplication:** [rapidfuzz](https://github.com/maxbachmann/RapidFuzz)
- **Storage:** SQLite (zero infra)
- **CLI:** [Click](https://click.palletsprojects.com/) + [Rich](https://github.com/Textualize/rich)
- **Email:** Gmail SMTP or [Resend](https://resend.com/)
- **Templates:** Jinja2

## Safety

This tool is **read-only**. It never auto-applies to jobs, submits personal data to any ATS, or interacts with application forms. All data is stored locally in SQLite only.
