"""
Cross-source deduplication using fuzzy matching.

Strategy:
1. Normalize: lowercase, strip whitespace, remove special chars from title + company
2. Create composite key: f"{normalized_title}|{normalized_company}|{city}"
3. Use rapidfuzz.fuzz.ratio() with threshold >= 85 to catch variants like:
   - "Sr. Analyst" vs "Senior Analyst"
   - "DoorDash" vs "DoorDash USA" vs "DoorDash, Inc."
4. Keep the version with the most complete data (longest description, has comp info)
5. Track all source URLs so the user can see where the job was posted

Also deduplicate against previously seen jobs (stored in SQLite).
- Jobs seen in last 14 days should be excluded from digest but kept in DB
- Jobs older than 30 days can be purged from DB
"""

import re
import pandas as pd
from rapidfuzz import fuzz
from rich.console import Console

from src.storage import generate_job_id

console = Console()


class Deduplicator:
    """Cross-source job deduplicator using exact-match and fuzzy-match strategies."""

    def __init__(self, threshold: int = 85):
        self.threshold = threshold

    def _normalize(self, text: str) -> str:
        """Lowercase, strip, remove special chars (keep alphanumeric and spaces only),
        and collapse multiple spaces into one."""
        if not isinstance(text, str):
            return ""
        text = text.lower().strip()
        text = re.sub(r"[^a-z0-9 ]", " ", text)
        text = re.sub(r" +", " ", text).strip()
        return text

    def _extract_city(self, location: str) -> str:
        """Extract and normalize the city from a location string.

        Takes everything before the first comma, then normalizes it.
        E.g. "San Francisco, CA" -> "san francisco"
        """
        if not isinstance(location, str):
            return ""
        city = location.split(",")[0]
        return self._normalize(city)

    def _completeness_score(self, row) -> int:
        """Score how complete a job record is.

        +10  if description is present (non-empty)
        +len(description)  for description length (rewards richer content)
        +5   if comp_min is present
        +5   if comp_max is present
        +3   if job_url is present
        """
        score = 0

        description = row.get("description") if hasattr(row, "get") else getattr(row, "description", None)
        if description and isinstance(description, str) and description.strip():
            score += 10
            score += len(description.strip())

        comp_min = row.get("comp_min") if hasattr(row, "get") else getattr(row, "comp_min", None)
        if comp_min is not None and not (isinstance(comp_min, float) and pd.isna(comp_min)):
            score += 5

        comp_max = row.get("comp_max") if hasattr(row, "get") else getattr(row, "comp_max", None)
        if comp_max is not None and not (isinstance(comp_max, float) and pd.isna(comp_max)):
            score += 5

        job_url = row.get("job_url") if hasattr(row, "get") else getattr(row, "job_url", None)
        if job_url and isinstance(job_url, str) and job_url.strip():
            score += 3

        return score

    def deduplicate(self, df: pd.DataFrame, seen_ids: set = None) -> pd.DataFrame:
        """Deduplicate a DataFrame of job listings.

        Steps:
        1. Return early if the DataFrame is empty.
        2. Remove exact duplicates based on normalized title + company + location.
        3. Group fuzzy matches and keep the most complete representative per group.
        4. Collect all source URLs from duplicates into an `all_sources` column.
        5. Optionally filter out jobs whose generated ID appears in seen_ids.
        6. Log stats and return the deduplicated DataFrame.
        """
        if df.empty:
            return df

        total_input = len(df)

        # ------------------------------------------------------------------ #
        # Step 1: build normalized columns used for both exact and fuzzy dedup
        # ------------------------------------------------------------------ #
        df = df.copy()

        # Ensure the columns we need exist; fill missing ones with empty strings
        for col in ("title", "company", "location", "job_url"):
            if col not in df.columns:
                df[col] = ""

        df["_norm_title"] = df["title"].apply(self._normalize)
        df["_norm_company"] = df["company"].apply(self._normalize)
        df["_city"] = df["location"].apply(self._extract_city)

        # ------------------------------------------------------------------ #
        # Step 2: exact deduplication
        # ------------------------------------------------------------------ #
        exact_key = df["_norm_title"] + "|" + df["_norm_company"] + "|" + df["_city"]
        df["_exact_key"] = exact_key

        # Among exact duplicates, keep the row with the highest completeness score
        df["_completeness"] = df.apply(self._completeness_score, axis=1)

        # Sort by completeness descending so drop_duplicates keeps the best row
        df_sorted = df.sort_values("_completeness", ascending=False)
        df_no_exact = df_sorted.drop_duplicates(subset="_exact_key", keep="first").copy()

        exact_dupes_removed = total_input - len(df_no_exact)

        # ------------------------------------------------------------------ #
        # Step 3: fuzzy deduplication (O(n^2), fine for ~500-1000 jobs)
        # ------------------------------------------------------------------ #
        # Build composite keys for fuzzy comparison
        df_no_exact["_fuzzy_key"] = (
            df_no_exact["_norm_title"] + "|" + df_no_exact["_norm_company"] + "|" + df_no_exact["_city"]
        )

        # Reset to a clean positional index for iteration
        rows = df_no_exact.reset_index(drop=True)

        # accepted_indices: indices (in `rows`) of jobs we are keeping
        # group_members[i]: list of indices from `rows` that are duplicates of row i
        accepted_indices: list[int] = []
        group_members: dict[int, list[int]] = {}

        for idx in range(len(rows)):
            current_key = rows.at[idx, "_fuzzy_key"]
            current_company = rows.at[idx, "_norm_company"]
            matched_group = None

            for accepted_idx in accepted_indices:
                accepted_key = rows.at[accepted_idx, "_fuzzy_key"]
                accepted_company = rows.at[accepted_idx, "_norm_company"]
                # Both the composite key AND company name must fuzzy-match
                # Use partial_ratio for company to handle "DoorDash" vs "DoorDash USA"
                key_ratio = fuzz.ratio(current_key, accepted_key)
                company_ratio = fuzz.partial_ratio(current_company, accepted_company)
                if key_ratio >= self.threshold and company_ratio >= self.threshold:
                    matched_group = accepted_idx
                    break

            if matched_group is None:
                # This row starts a new group
                accepted_indices.append(idx)
                group_members[idx] = [idx]
            else:
                # This row is a fuzzy duplicate of an existing group
                group_members[matched_group].append(idx)

                # Promote this row as the group representative if it is more complete
                current_score = rows.at[idx, "_completeness"]
                accepted_score = rows.at[matched_group, "_completeness"]
                if current_score > accepted_score:
                    # Swap: make idx the new representative
                    group_members[idx] = group_members.pop(matched_group)
                    accepted_indices[accepted_indices.index(matched_group)] = idx
                    group_members[idx][group_members[idx].index(idx)] = matched_group

        fuzzy_dupes_removed = len(rows) - len(accepted_indices)

        # ------------------------------------------------------------------ #
        # Step 4: collect all source URLs for each group -> `all_sources`
        # ------------------------------------------------------------------ #
        result_rows = []
        for rep_idx in accepted_indices:
            rep_row = rows.loc[rep_idx].to_dict()
            member_indices = group_members[rep_idx]

            # Gather non-empty job_url values from all group members
            urls = []
            for m_idx in member_indices:
                url = rows.at[m_idx, "job_url"]
                if url and isinstance(url, str) and url.strip():
                    urls.append(url.strip())

            # Also include the representative's own URL if not already present
            rep_url = rep_row.get("job_url", "")
            if rep_url and isinstance(rep_url, str) and rep_url.strip() and rep_url.strip() not in urls:
                urls.insert(0, rep_url.strip())

            rep_row["all_sources"] = ", ".join(dict.fromkeys(urls))  # preserve order, dedupe
            result_rows.append(rep_row)

        result_df = pd.DataFrame(result_rows)

        # ------------------------------------------------------------------ #
        # Step 5: filter out previously seen jobs
        # ------------------------------------------------------------------ #
        seen_removed = 0
        if seen_ids and not result_df.empty:
            result_df["_job_id"] = result_df.apply(
                lambda r: generate_job_id(
                    str(r.get("title", "")),
                    str(r.get("company", "")),
                    str(r.get("location", "")),
                ),
                axis=1,
            )
            mask_seen = result_df["_job_id"].isin(seen_ids)
            seen_removed = mask_seen.sum()
            result_df = result_df[~mask_seen].copy()

        # ------------------------------------------------------------------ #
        # Step 6: clean up internal columns and log stats
        # ------------------------------------------------------------------ #
        internal_cols = ["_norm_title", "_norm_company", "_city", "_exact_key", "_fuzzy_key",
                         "_completeness", "_job_id"]
        drop_cols = [c for c in internal_cols if c in result_df.columns]
        result_df = result_df.drop(columns=drop_cols).reset_index(drop=True)

        final_count = len(result_df)

        console.print(
            f"[bold cyan]Deduplication stats:[/bold cyan] "
            f"input={total_input} | "
            f"exact dupes removed={exact_dupes_removed} | "
            f"fuzzy dupes removed={fuzzy_dupes_removed} | "
            f"seen jobs removed={seen_removed} | "
            f"final count={final_count}"
        )

        return result_df
