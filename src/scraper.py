"""
Uses python-jobspy to run multiple search queries across job boards.

Key behaviors:
- Run each search_term x location combo as a separate scrape_jobs() call
- Use hours_old=24 for daily runs (168 for backfill)
- Aggregate all results into a single DataFrame
- Handle rate limiting gracefully: add 3-5 second delays between queries
- Catch and log errors per-query without crashing the pipeline
"""

import time
import yaml
import pandas as pd
from pathlib import Path
from rich.console import Console
from jobspy import scrape_jobs


console = Console()


class JobScraper:
    def __init__(self, config_path="config/search_queries.yaml"):
        config_file = Path(config_path)
        with config_file.open("r") as f:
            self.config = yaml.safe_load(f)

    def scrape(self, backfill=False, test=False) -> pd.DataFrame:
        search_terms_config = self.config.get("search_terms", {})
        all_search_terms = (
            search_terms_config.get("high_priority", [])
            + search_terms_config.get("medium_priority", [])
            + search_terms_config.get("low_priority", [])
        )

        locations = self.config.get("locations", [])
        sites = self.config.get("sites", [])
        settings = self.config.get("settings", {})

        results_wanted = settings.get("results_per_site", 25)
        hours_old = (
            settings.get("hours_old_backfill", 168)
            if backfill
            else settings.get("hours_old_daily", 24)
        )
        delay_sec = settings.get("delay_between_queries_sec", 4)
        distance_miles = settings.get("distance_miles", 25)

        if test:
            all_search_terms = all_search_terms[:1]
            locations = locations[:1]
            results_wanted = 5

        all_results = []
        total_queries = len(all_search_terms) * len(locations)
        query_num = 0

        for search_term in all_search_terms:
            for location in locations:
                query_num += 1
                console.print(
                    f"[bold cyan]Query {query_num}/{total_queries}:[/bold cyan] "
                    f"'{search_term}' in '{location}'"
                )

                try:
                    df = scrape_jobs(
                        site_name=sites,
                        search_term=search_term,
                        location=location,
                        results_wanted=results_wanted,
                        hours_old=hours_old,
                        country_indeed="USA",
                        job_type="fulltime",
                        distance=distance_miles,
                        linkedin_fetch_description=True,
                        description_format="markdown",
                    )
                    console.print(
                        f"  [green]Found {len(df)} results[/green]"
                    )
                    all_results.append(df)
                except Exception as e:
                    console.print(
                        f"  [red]Error scraping '{search_term}' in '{location}': {e}[/red]"
                    )

                if query_num < total_queries:
                    time.sleep(delay_sec)

        if not all_results:
            console.print("[yellow]No results found across all queries.[/yellow]")
            return pd.DataFrame()

        combined = pd.concat(all_results, ignore_index=True)

        console.print(f"\n[bold green]Scrape complete.[/bold green] Total results: {len(combined)}")
        if "site" in combined.columns:
            site_counts = combined["site"].value_counts()
            console.print("[bold]Results per site:[/bold]")
            for site, count in site_counts.items():
                console.print(f"  {site}: {count}")

        return combined
