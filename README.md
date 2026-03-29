# Bay Area Job Hunter

> **Why I built this:** Manually sifting through job boards every day is slow, repetitive, and easy to let slip. I wanted a system that would do the legwork for me — scraping across multiple boards, filtering out noise, and surfacing only the roles most aligned to my background in FP&A, Strategy, and BizOps. So I built a daily pipeline that scores and ranks every new posting against my actual skillset and delivers a prioritized digest to my inbox each morning.

A Python CLI that scrapes Bay Area job postings daily across 5 job boards, scores each role against a candidate profile, and delivers a ranked HTML digest by email. Runs automatically via GitHub Actions — no laptop required.

## Pipeline

```
Scrape (JobSpy) → Deduplicate (fuzzy match) → Filter (seniority/location/blocklist)
    → Score (0–100) → Enrich (company tier, posting age) → Digest (email)
```

## Scoring — Two Modes

The scorer auto-selects based on whether `ANTHROPIC_API_KEY` is set:

### Claude Agent Scorer (default when API key is present)

Uses a Claude agent with tool use to reason semantically about job fit. The agent runs a tool loop:

1. **`lookup_candidate_profile`** — retrieves candidate skills, experience, and background signals
2. **`lookup_company_tier`** — looks up company quality tier and score
3. **`submit_score`** — terminal tool; ends the loop and returns a structured score

The agent reasons holistically — it can recognize that "consulting or IB background preferred" matches an FP&A + MBA profile even without exact keyword overlap. Each scored job includes a natural-language `reasoning` field and `key_matches`/`key_gaps` lists surfaced in the digest.

### Rule-Based Scorer (free fallback)

Keyword matching against a `candidate_profile.yaml` using a 3-tier system:
- **Tier 1** (5 pts): Background signals — MBA, consulting/IB/PE
- **Tier 2** (2 pts each): Core competency keywords from profile
- **Tier 3** (1 pt each): Technical skill matches

Both scorers use the same 0–100 rubric:

| Dimension | Max | What it measures |
|---|---|---|
| Title match | 30 | Role type alignment (Strategy & Ops, FP&A, BizOps, GTM, etc.) |
| Description / semantic fit | 25 | Requirement overlap with candidate background |
| YOE fit | 15 | Years of experience required vs. candidate sweet spot (3–6 yrs) |
| Company quality | 15 | Tier 1–4 company list |
| Comp range | 10 | Whether stated comp overlaps target range |
| Posting freshness | 5 | Posted today > this week |

## Features

- **Multi-board scraping** — Indeed, LinkedIn, ZipRecruiter, Google Jobs, Glassdoor via [python-jobspy](https://github.com/Bunsly/JobSpy)
- **Fuzzy deduplication** — rapidfuzz cross-source dedup (catches "Sr. Analyst" vs "Senior Analyst", "DoorDash Inc." vs "DoorDash")
- **Smart filtering** — hard filters for Bay Area location, seniority level, job type, and company blocklist
- **Claude agent scorer** — tool-use loop with per-job reasoning, key matches, and key gaps
- **Rule-based fallback** — profile-driven keyword scorer runs free if no API key is set; also used per-job if agent fails
- **HTML email digest** — top 15–25 roles ranked by score, with comp range, freshness, matched keywords, and reasoning
- **SQLite persistence** — tracks seen jobs, avoids re-sending duplicates, stores full score breakdowns
- **CLI** — `run`, `scrape`, `score`, `digest`, `export`, `stats`, `mark` commands

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
# Required: GMAIL_ADDRESS, GMAIL_APP_PASSWORD, DIGEST_RECIPIENT
# Optional: ANTHROPIC_API_KEY  (enables Claude agent scoring — ~$1.20/month for daily runs)
```

Create `config/candidate_profile.yaml` (gitignored) with your skills and competencies. See `config/scoring.yaml` and `config/companies.yaml` for structure reference.

### 3. Run

```bash
# First run — 7-day backfill
python -m src.main run --backfill

# Daily run
python -m src.main run

# Other commands
python -m src.main scrape                              # scrape only, no digest
python -m src.main stats                               # view score distribution
python -m src.main export --min-score 70 --output top_jobs.csv
python -m src.main mark <job_id> --status applied
python -m src.main test-email                          # verify delivery
```

## GitHub Actions (Automated)

The workflow runs daily at 7am PT. Add these secrets to your repo:

| Secret | Value |
|---|---|
| `GMAIL_ADDRESS` | Sending Gmail address |
| `GMAIL_APP_PASSWORD` | Gmail App Password (not your account password) |
| `DIGEST_RECIPIENT` | Where to send the digest |
| `ANTHROPIC_API_KEY` | Optional — enables Claude agent scoring |

## Configuration Files

| File | Purpose |
|---|---|
| `config/search_queries.yaml` | Search terms, locations, job board settings |
| `config/scoring.yaml` | Scoring weights, thresholds, freshness brackets |
| `config/companies.yaml` | Company tier list and blocklist |
| `config/candidate_profile.yaml` | **Gitignored** — your personal skills and competencies |

## Tech Stack

- **Scraping:** [python-jobspy](https://github.com/Bunsly/JobSpy)
- **Agent scoring:** [Anthropic Python SDK](https://github.com/anthropics/anthropic-sdk-python) — Claude Haiku with tool use
- **Deduplication:** [rapidfuzz](https://github.com/maxbachmann/RapidFuzz)
- **Storage:** SQLite (zero infra)
- **CLI:** [Click](https://click.palletsprojects.com/) + [Rich](https://github.com/Textualize/rich)
- **Email:** Gmail SMTP or [Resend](https://resend.com/)
- **Templates:** Jinja2

## File Structure

```
bay-area-job-hunter/
├── config/
│   ├── search_queries.yaml       # Search terms and job board settings
│   ├── scoring.yaml              # Scoring weights and thresholds
│   ├── companies.yaml            # Company tier list and blocklist
│   └── candidate_profile.yaml   # Gitignored — personal skills/profile
├── src/
│   ├── main.py                   # CLI entrypoint + pipeline orchestration
│   ├── scraper.py                # JobSpy wrapper + multi-query orchestration
│   ├── deduplicator.py           # Fuzzy dedup across sources
│   ├── filter.py                 # Seniority, location, keyword filtering
│   ├── llm_scorer.py             # Claude agent scorer (tool-use loop)
│   ├── scorer.py                 # Rule-based keyword scorer (free fallback)
│   ├── enricher.py               # Company metadata, posting age
│   ├── digest.py                 # Email/HTML digest builder
│   └── storage.py                # SQLite persistence layer
├── templates/
│   └── digest_email.html         # Jinja2 email template
└── data/
    └── jobs.db                   # Gitignored — SQLite database
```

## Safety

This tool is **read-only**. It never auto-applies to jobs, submits personal data to any ATS, or interacts with application forms. All data is stored locally in SQLite only.
