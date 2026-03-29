"""
LLM-based job scorer using a Claude agent with tool use.

The agent is given three tools:
  - lookup_candidate_profile  → returns candidate skills/background
  - lookup_company_tier       → returns tier + score for a company name
  - submit_score              → terminal tool; ends the loop and captures the score

Claude decides which tools to call, in what order, then submits a structured
score with reasoning. The deterministic comp_score and freshness_score are
pre-calculated and passed in as context so the LLM focuses on semantic fit.

Falls back to None on any failure — caller should use rule-based scorer instead.
"""

import json
import os
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime
from rich.console import Console

console = Console()

SYSTEM_PROMPT = """You are a precise job fit scorer for a specific candidate.

For each job posting you receive, use your tools to assess fit and submit a score.

Always follow this sequence:
1. Call lookup_candidate_profile to understand the candidate's background
2. Call lookup_company_tier to get the company score
3. Call submit_score with your full assessment

Scoring rubric — be strict and accurate:
- title_score (0–30): How well does the role type match?
    30 = exact match (Strategy & Ops, BizOps, FP&A, GTM, RevOps, Strategic Finance)
    20 = strong match (Operations, Strategy, Finance roles with relevant keywords)
    10 = partial match (Analyst/Associate/Manager in adjacent area)
    0  = no relevant match
- description_score (0–25): How well do job requirements match candidate skills?
    Award points for: MBA preferred (+5), consulting/IB/PE background (+5),
    SQL/financial modeling/Excel (+3 each), cross-functional (+3), board/exec
    presentations (+3), GTM/go-to-market (+3), strategic planning/LRP (+3).
    Cap at 25.
- yoe_score (0–15): Years of experience required vs candidate's ~5 years:
    15 = 3–6 years required
    10 = 2–3 or 5–8 years required
    5  = 1–2 or 8–10 years required
    0  = 10+ years, unspecified, or clearly entry-level
- company_score (0–15): Use lookup_company_tier — it returns the score directly.

The comp_score (0–10) and freshness_score (0–5) are pre-calculated and shown
in the job posting. Include them in submit_score as-is."""


class LLMJobScorer:
    def __init__(
        self,
        candidate_profile_path="config/candidate_profile.yaml",
        companies_config_path="config/companies.yaml",
    ):
        from anthropic import Anthropic
        self.client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        self.candidate_profile = self._load_yaml(candidate_profile_path)
        self.companies_config = self._load_yaml(companies_config_path)
        self.company_tier_lookup = self._build_company_tier_lookup()
        self._candidate_summary = self._build_candidate_summary()
        self._tools = self._define_tools()

    def _load_yaml(self, path: str) -> dict:
        p = Path(path)
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _build_company_tier_lookup(self) -> dict:
        lookup = {}
        for tier in ["tier_1", "tier_2", "tier_3"]:
            for company in self.companies_config.get(tier, []):
                lookup[company.lower()] = tier
        for company in self.companies_config.get("blocklist", []):
            lookup[company.lower()] = "blocklist"
        return lookup

    def _build_candidate_summary(self) -> str:
        """Compact text representation of candidate profile for LLM consumption."""
        c = self.candidate_profile.get("candidate", {})
        lines = [
            f"Candidate: {c.get('name', 'Unknown')}",
            f"Education: {'; '.join(c.get('education', []))}",
            f"Total experience: {c.get('years_experience', '5-7')} years",
            "",
            "Work history:",
        ]
        for exp in c.get("experience", []):
            lines.append(f"  [{exp['company']} | {exp['title']} | {exp['dates']}]")
            for h in exp.get("highlights", [])[:3]:
                lines.append(f"    • {h}")

        lines.append("\nCore competencies (keyword signals):")
        for category, keywords in c.get("core_competencies", {}).items():
            lines.append(f"  {category.replace('_', ' ').title()}: {', '.join(keywords[:10])}")

        lines.append("\nTechnical tools:")
        for cat, skills in c.get("technical_skills", {}).items():
            lines.append(f"  {cat}: {', '.join(skills)}")

        signals = c.get("background_signals", [])
        lines.append(f"\nKey background signals: {', '.join(signals)}")
        return "\n".join(lines)

    def _define_tools(self) -> list:
        return [
            {
                "name": "lookup_candidate_profile",
                "description": (
                    "Retrieve the candidate's full background — work history, skills, "
                    "competencies, and background signals — to compare against this job's requirements."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            },
            {
                "name": "lookup_company_tier",
                "description": (
                    "Look up the quality tier and score for a company. "
                    "Returns tier (tier_1 through tier_4 or blocklist), score (0–15), and description."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "company_name": {
                            "type": "string",
                            "description": "The name of the company to look up",
                        }
                    },
                    "required": ["company_name"],
                },
            },
            {
                "name": "submit_score",
                "description": "Submit the final scored fit assessment for this job. This ends the scoring loop.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "title_score": {
                            "type": "integer",
                            "description": "0–30: how well the role type matches target roles",
                        },
                        "description_score": {
                            "type": "integer",
                            "description": "0–25: how well job requirements match candidate skills and background",
                        },
                        "yoe_score": {
                            "type": "integer",
                            "description": "0–15: how well required years of experience fits the candidate",
                        },
                        "company_score": {
                            "type": "integer",
                            "description": "0–15: company quality score from lookup_company_tier",
                        },
                        "comp_score": {
                            "type": "integer",
                            "description": "0–10: compensation fit — copy the pre-calculated value from the job posting",
                        },
                        "freshness_score": {
                            "type": "integer",
                            "description": "0–5: posting freshness — copy the pre-calculated value from the job posting",
                        },
                        "reasoning": {
                            "type": "string",
                            "description": "2–3 sentence explanation of why this role is or isn't a strong fit",
                        },
                        "key_matches": {
                            "type": "string",
                            "description": "Top 3–5 signals that matched, as a comma-separated string. E.g. 'MBA preferred, SQL required, cross-functional experience'",
                        },
                        "key_gaps": {
                            "type": "string",
                            "description": "Top 1–3 gaps or concerns as a comma-separated string, or empty string if strong fit. E.g. 'requires Salesforce, 8+ years preferred'",
                        },
                    },
                    "required": [
                        "title_score", "description_score", "yoe_score",
                        "company_score", "comp_score", "freshness_score",
                        "reasoning", "key_matches", "key_gaps",
                    ],
                },
            },
        ]

    def _execute_tool(self, tool_name: str, tool_input: dict) -> str:
        if tool_name == "lookup_candidate_profile":
            return self._candidate_summary

        if tool_name == "lookup_company_tier":
            company = tool_input.get("company_name", "").lower().strip()
            tier = "tier_4"
            for known, t in self.company_tier_lookup.items():
                if known in company or company in known:
                    tier = t
                    break
            tier_scores = {"tier_1": 15, "tier_2": 12, "tier_3": 8, "blocklist": 0, "tier_4": 4}
            tier_labels = {
                "tier_1": "Top-tier tech (FAANG, Stripe, Anthropic, OpenAI, etc.)",
                "tier_2": "Strong growth tech (Rubrik, Rippling, DoorDash, Roblox, etc.)",
                "tier_3": "Solid public tech (HubSpot, Okta, Palo Alto Networks, etc.)",
                "tier_4": "Unknown company — default",
                "blocklist": "Staffing agency — excluded from digest",
            }
            return json.dumps({
                "tier": tier,
                "score": tier_scores[tier],
                "description": tier_labels[tier],
            })

        if tool_name == "submit_score":
            return "Score received."

        return f"Unknown tool: {tool_name}"

    def score_job(
        self,
        title: str,
        description: str,
        company: str,
        comp_score: int,
        freshness_score: int,
    ) -> dict | None:
        """
        Run the agent loop for a single job. Returns a score dict or None on failure.
        """
        user_message = (
            f"Score this job posting.\n\n"
            f"Title: {title}\n"
            f"Company: {company}\n"
            f"Pre-calculated comp_score: {comp_score}/10\n"
            f"Pre-calculated freshness_score: {freshness_score}/5\n\n"
            f"Job Description:\n{(description or '')[:3000]}"
        )

        messages = [{"role": "user", "content": user_message}]
        submitted_score = None

        for _ in range(8):  # safety ceiling on turns
            response = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                tools=self._tools,
                messages=messages,
            )

            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = self._execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
                    if block.name == "submit_score":
                        submitted_score = block.input

            if submitted_score:
                break

            if tool_results:
                messages.append({"role": "user", "content": tool_results})
            elif response.stop_reason == "end_turn":
                break

        if not submitted_score:
            return None

        # Normalise key_matches / key_gaps: schema uses comma-separated strings for reliability
        for field in ("key_matches", "key_gaps"):
            val = submitted_score.get(field, "")
            if isinstance(val, list):
                submitted_score[field] = val  # already a list, keep as-is
            elif isinstance(val, str) and val.strip():
                submitted_score[field] = [i.strip() for i in val.split(",") if i.strip()]
            else:
                submitted_score[field] = []

        total = min(
            submitted_score.get("title_score", 0)
            + submitted_score.get("description_score", 0)
            + submitted_score.get("yoe_score", 0)
            + submitted_score.get("company_score", 0)
            + submitted_score.get("comp_score", comp_score)
            + submitted_score.get("freshness_score", freshness_score),
            100,
        )

        return {
            "total": total,
            "title_score": submitted_score.get("title_score", 0),
            "description_score": submitted_score.get("description_score", 0),
            "yoe_score": submitted_score.get("yoe_score", 0),
            "company_score": submitted_score.get("company_score", 0),
            "comp_score": submitted_score.get("comp_score", comp_score),
            "freshness_score": submitted_score.get("freshness_score", freshness_score),
            "reasoning": submitted_score.get("reasoning", ""),
            "key_matches": submitted_score.get("key_matches", []),
            "key_gaps": submitted_score.get("key_gaps", []),
            "scored_by": "llm",
        }

    def score_jobs(self, df: pd.DataFrame) -> pd.DataFrame:
        """Score a DataFrame of jobs, falling back to rule-based scorer on failure."""
        from src.scorer import JobScorer
        import json as _json

        fallback = JobScorer()
        df = df.copy()
        scores, breakdowns = [], []

        total = len(df)
        console.print(f"\n[bold cyan]LLM Scorer[/bold cyan] scoring {total} jobs with Claude agent...")

        for i, (_, row) in enumerate(df.iterrows(), 1):
            title       = row.get("title", "") or ""
            description = row.get("description", "") or ""
            company     = row.get("company", "") or ""
            comp_min    = row.get("comp_min", None)
            comp_max    = row.get("comp_max", None)
            date_posted = row.get("date_posted", None)

            # Pre-calculate deterministic scores
            comp_score      = fallback._score_comp(comp_min, comp_max)
            freshness_score = fallback._score_freshness(date_posted)

            result = None
            try:
                result = self.score_job(title, description, company, comp_score, freshness_score)
            except Exception as e:
                console.print(f"  [yellow]LLM scoring failed for '{title}': {e} — using rule-based fallback[/yellow]")

            if result is None:
                # Fall back to rule-based for this job
                fallback_row = df.iloc[[i - 1]]
                fallback_result = fallback.score_jobs(fallback_row)
                scores.append(int(fallback_result.iloc[0]["score"]))
                breakdowns.append(fallback_result.iloc[0]["score_breakdown"])
                console.print(f"  [{i}/{total}] [dim]{company} — {title[:50]}[/dim] → fallback score")
            else:
                scores.append(result["total"])
                breakdowns.append(_json.dumps(result))
                console.print(
                    f"  [{i}/{total}] [dim]{company} — {title[:50]}[/dim] → "
                    f"[bold]{result['total']}[/bold]  {result['reasoning'][:80]}..."
                )

        df["score"] = scores
        df["score_breakdown"] = breakdowns
        return df
