"""
CLI entrypoint for Bay Area Job Hunter.
Commands: run, scrape, score, digest, export, stats, mark, test-email, schedule
"""

import sys
import platform
from datetime import datetime

import click
import pandas as pd
from rich.console import Console
from rich.table import Table

from src.scraper import JobScraper
from src.deduplicator import Deduplicator
from src.filter import JobFilter
from src.scorer import JobScorer
from src.enricher import JobEnricher
from src.digest import DigestBuilder
from src.storage import JobStorage, generate_job_id

console = Console()


@click.group()
def cli():
    """Bay Area Job Hunter - daily pipeline for scraping, scoring, and digesting jobs."""
    pass


@cli.command()
@click.option("--backfill", is_flag=True, default=False, help="Use longer lookback window (168h instead of 24h).")
def run(backfill):
    """Run the full daily pipeline: scrape -> deduplicate -> filter -> score -> enrich -> persist -> digest."""
    console.rule("[bold blue]Bay Area Job Hunter - Full Pipeline[/bold blue]")
    start_time = datetime.utcnow()
    errors = []

    # Initialize all modules
    storage = JobStorage()
    scraper = JobScraper()
    deduplicator = Deduplicator()
    job_filter = JobFilter()
    scorer = JobScorer()
    enricher = JobEnricher()
    digest_builder = DigestBuilder()

    # Step 1: Scrape
    console.print("[bold cyan]Step 1/7:[/bold cyan] Scraping jobs...")
    try:
        raw_df = scraper.scrape(backfill=backfill)
        total_scraped = len(raw_df)
        console.print(f"  [green]Scraped {total_scraped} raw jobs.[/green]")
    except Exception as exc:
        console.print(f"  [red]Scraping failed: {exc}[/red]")
        errors.append(str(exc))
        raw_df = pd.DataFrame()
        total_scraped = 0

    # Step 2: Deduplicate
    console.print("[bold cyan]Step 2/7:[/bold cyan] Deduplicating...")
    seen_ids = storage.get_seen_ids(days=168 if backfill else 14)
    try:
        deduped_df = deduplicator.deduplicate(raw_df, seen_ids=seen_ids)
        total_after_dedup = len(deduped_df)
        console.print(f"  [green]{total_after_dedup} jobs after deduplication.[/green]")
    except Exception as exc:
        console.print(f"  [red]Deduplication failed: {exc}[/red]")
        errors.append(str(exc))
        deduped_df = raw_df
        total_after_dedup = len(deduped_df)

    # Step 3: Filter
    console.print("[bold cyan]Step 3/7:[/bold cyan] Filtering...")
    try:
        filtered_df = job_filter.filter(deduped_df)
        total_after_filter = len(filtered_df)
        console.print(f"  [green]{total_after_filter} jobs after filtering.[/green]")
    except Exception as exc:
        console.print(f"  [red]Filtering failed: {exc}[/red]")
        errors.append(str(exc))
        filtered_df = deduped_df
        total_after_filter = len(filtered_df)

    if filtered_df.empty:
        console.print("[yellow]No jobs remain after filtering. Skipping score/enrich/digest steps.[/yellow]")
        storage.log_scrape_run({
            "timestamp": start_time.isoformat(),
            "total_scraped": total_scraped,
            "total_after_dedup": total_after_dedup,
            "total_after_filter": 0,
            "total_sent": 0,
            "errors": errors if errors else None,
        })
        console.rule("[bold blue]Pipeline complete (no jobs to process)[/bold blue]")
        return

    # Step 4: Score
    console.print("[bold cyan]Step 4/7:[/bold cyan] Scoring...")
    try:
        scored_df = scorer.score(filtered_df)
        console.print(f"  [green]Scored {len(scored_df)} jobs.[/green]")
    except Exception as exc:
        console.print(f"  [red]Scoring failed: {exc}[/red]")
        errors.append(str(exc))
        scored_df = filtered_df

    # Step 5: Enrich
    console.print("[bold cyan]Step 5/7:[/bold cyan] Enriching...")
    try:
        enriched_df = enricher.enrich(scored_df)
        console.print(f"  [green]Enriched {len(enriched_df)} jobs.[/green]")
    except Exception as exc:
        console.print(f"  [red]Enrichment failed: {exc}[/red]")
        errors.append(str(exc))
        enriched_df = scored_df

    # Step 6: Generate IDs and persist to SQLite
    console.print("[bold cyan]Step 6/7:[/bold cyan] Persisting to database...")
    try:
        if "id" not in enriched_df.columns:
            enriched_df["id"] = enriched_df.apply(
                lambda row: generate_job_id(
                    str(row.get("title", "")),
                    str(row.get("company", "")),
                    str(row.get("location", "")),
                ),
                axis=1,
            )
        if "date_scraped" not in enriched_df.columns:
            enriched_df["date_scraped"] = datetime.utcnow().date().isoformat()
        storage.save_jobs(enriched_df)
        console.print(f"  [green]Saved {len(enriched_df)} jobs to storage.[/green]")
    except Exception as exc:
        console.print(f"  [red]Persist failed: {exc}[/red]")
        errors.append(str(exc))

    # Step 7: Build and send digest
    console.print("[bold cyan]Step 7/7:[/bold cyan] Sending digest...")
    total_sent = 0
    try:
        import yaml
        from pathlib import Path

        scoring_config_path = Path("config/scoring.yaml")
        min_score = 40
        if scoring_config_path.exists():
            with scoring_config_path.open("r") as f:
                scoring_cfg = yaml.safe_load(f)
            min_score = scoring_cfg.get("thresholds", {}).get("minimum_score_for_digest", 40)

        digest_jobs = storage.get_unsent_jobs(min_score=min_score)
        if digest_jobs.empty:
            console.print("  [yellow]No qualifying jobs for digest.[/yellow]")
        else:
            digest_builder.build_and_send(digest_jobs)
            job_ids = digest_jobs["id"].tolist()
            storage.mark_jobs_sent(job_ids)
            total_sent = len(job_ids)
            console.print(f"  [green]Digest sent with {total_sent} jobs.[/green]")
    except Exception as exc:
        console.print(f"  [red]Digest failed: {exc}[/red]")
        errors.append(str(exc))

    # Log the scrape run
    storage.log_scrape_run({
        "timestamp": start_time.isoformat(),
        "total_scraped": total_scraped,
        "total_after_dedup": total_after_dedup,
        "total_after_filter": total_after_filter,
        "total_sent": total_sent,
        "errors": errors if errors else None,
    })

    console.rule("[bold blue]Pipeline complete[/bold blue]")
    console.print(
        f"[bold]Summary:[/bold] scraped={total_scraped}, "
        f"after_dedup={total_after_dedup}, after_filter={total_after_filter}, "
        f"sent={total_sent}, errors={len(errors)}"
    )


@cli.command()
@click.option("--backfill", is_flag=True, default=False, help="Use longer lookback window.")
@click.option("--test", "test_run", is_flag=True, default=False, help="Run a minimal test scrape.")
def scrape(backfill, test_run):
    """Scrape jobs and save raw results to storage (without scoring or filtering)."""
    console.rule("[bold blue]Scrape Only[/bold blue]")

    storage = JobStorage()
    scraper = JobScraper()

    console.print("[cyan]Scraping jobs...[/cyan]")
    try:
        raw_df = scraper.scrape(backfill=backfill, test=test_run)
        console.print(f"[green]Scraped {len(raw_df)} raw jobs.[/green]")
    except Exception as exc:
        console.print(f"[red]Scraping failed: {exc}[/red]")
        return

    if raw_df.empty:
        console.print("[yellow]No jobs scraped.[/yellow]")
        return

    # Generate IDs and set date_scraped before saving
    if "id" not in raw_df.columns:
        raw_df["id"] = raw_df.apply(
            lambda row: generate_job_id(
                str(row.get("title", "")),
                str(row.get("company", "")),
                str(row.get("location", "")),
            ),
            axis=1,
        )
    if "date_scraped" not in raw_df.columns:
        raw_df["date_scraped"] = datetime.utcnow().date().isoformat()

    storage.save_jobs(raw_df)
    console.print(f"[green]Saved {len(raw_df)} jobs to storage.[/green]")


@cli.command()
def score():
    """Score existing unscored jobs in the database."""
    console.rule("[bold blue]Score Unscored Jobs[/bold blue]")

    storage = JobStorage()
    scorer = JobScorer()

    console.print("[cyan]Loading unscored jobs...[/cyan]")
    import sqlite3
    with sqlite3.connect(storage.db_path) as conn:
        conn.row_factory = sqlite3.Row
        unscored_df = pd.read_sql_query(
            "SELECT * FROM jobs WHERE score IS NULL",
            conn,
        )

    if unscored_df.empty:
        console.print("[yellow]No unscored jobs found.[/yellow]")
        return

    console.print(f"[cyan]Scoring {len(unscored_df)} jobs...[/cyan]")
    try:
        scored_df = scorer.score(unscored_df)
    except Exception as exc:
        console.print(f"[red]Scoring failed: {exc}[/red]")
        return

    storage.save_jobs(scored_df)
    console.print(f"[green]Updated scores for {len(scored_df)} jobs.[/green]")


@cli.command()
@click.option("--min-score", default=None, type=int, help="Minimum score threshold (default from config).")
def digest(min_score):
    """Send the digest email for today's qualifying jobs."""
    console.rule("[bold blue]Send Digest[/bold blue]")

    storage = JobStorage()
    digest_builder = DigestBuilder()

    if min_score is None:
        try:
            import yaml
            from pathlib import Path
            with open("config/scoring.yaml", "r") as f:
                cfg = yaml.safe_load(f)
            min_score = cfg.get("thresholds", {}).get("minimum_score_for_digest", 40)
        except Exception:
            min_score = 40

    console.print(f"[cyan]Loading unsent jobs with score >= {min_score}...[/cyan]")
    jobs_df = storage.get_unsent_jobs(min_score=min_score)

    if jobs_df.empty:
        console.print("[yellow]No qualifying unsent jobs found.[/yellow]")
        return

    console.print(f"[cyan]Building and sending digest for {len(jobs_df)} jobs...[/cyan]")
    try:
        digest_builder.build_and_send(jobs_df)
        job_ids = jobs_df["id"].tolist()
        storage.mark_jobs_sent(job_ids)
        console.print(f"[green]Digest sent. Marked {len(job_ids)} jobs as sent.[/green]")
    except Exception as exc:
        console.print(f"[red]Digest failed: {exc}[/red]")


@cli.command()
@click.option("--output", default="jobs_export.csv", show_default=True, help="Output CSV file path.")
@click.option("--min-score", default=0, show_default=True, type=int, help="Minimum score to include.")
def export(output, min_score):
    """Export jobs to a CSV file."""
    console.rule("[bold blue]Export Jobs to CSV[/bold blue]")

    storage = JobStorage()

    console.print(f"[cyan]Exporting jobs with score >= {min_score}...[/cyan]")
    jobs_df = storage.export_jobs(min_score=min_score)

    if jobs_df.empty:
        console.print("[yellow]No jobs found matching the criteria.[/yellow]")
        return

    jobs_df.to_csv(output, index=False)
    console.print(f"[green]Exported {len(jobs_df)} jobs to {output}.[/green]")


@cli.command()
def stats():
    """Display database statistics."""
    console.rule("[bold blue]Database Stats[/bold blue]")

    storage = JobStorage()
    data = storage.get_stats()

    # Summary table
    summary_table = Table(title="Job Summary", show_header=True, header_style="bold magenta")
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", style="green")
    summary_table.add_row("Total Jobs", str(data["total_jobs"]))
    for status, count in data.get("jobs_by_status", {}).items():
        summary_table.add_row(f"  Status: {status}", str(count))
    console.print(summary_table)

    # Score distribution
    import sqlite3
    try:
        with sqlite3.connect(storage.db_path) as conn:
            score_df = pd.read_sql_query(
                """
                SELECT
                    CASE
                        WHEN score IS NULL THEN 'Unscored'
                        WHEN score < 40 THEN '< 40'
                        WHEN score < 60 THEN '40-59'
                        WHEN score < 75 THEN '60-74'
                        WHEN score < 85 THEN '75-84'
                        ELSE '>= 85'
                    END as range,
                    COUNT(*) as count
                FROM jobs
                GROUP BY range
                ORDER BY range
                """,
                conn,
            )
        if not score_df.empty:
            score_table = Table(title="Score Distribution", show_header=True, header_style="bold magenta")
            score_table.add_column("Score Range", style="cyan")
            score_table.add_column("Count", style="green")
            for _, row in score_df.iterrows():
                score_table.add_row(str(row["range"]), str(row["count"]))
            console.print(score_table)
    except Exception as exc:
        console.print(f"[red]Could not load score distribution: {exc}[/red]")

    # Recent scrape runs
    recent_runs = data.get("recent_scrape_runs", [])
    if recent_runs:
        runs_table = Table(title="Recent Scrape Runs (last 10)", show_header=True, header_style="bold magenta")
        runs_table.add_column("Timestamp", style="cyan")
        runs_table.add_column("Scraped", style="green")
        runs_table.add_column("After Dedup", style="green")
        runs_table.add_column("After Filter", style="green")
        runs_table.add_column("Sent", style="green")
        runs_table.add_column("Errors", style="red")
        for run in recent_runs:
            runs_table.add_row(
                str(run.get("timestamp", "")),
                str(run.get("total_scraped", "")),
                str(run.get("total_after_dedup", "")),
                str(run.get("total_after_filter", "")),
                str(run.get("total_sent", "")),
                str(run.get("errors", "") or ""),
            )
        console.print(runs_table)
    else:
        console.print("[yellow]No scrape runs recorded yet.[/yellow]")


@cli.command()
@click.argument("job_id")
@click.option(
    "--status",
    required=True,
    type=click.Choice(["applied", "saved", "rejected"], case_sensitive=False),
    help="New status to set for the job.",
)
def mark(job_id, status):
    """Mark a job with a given status (applied/saved/rejected)."""
    storage = JobStorage()
    storage.mark_job_status(job_id, status.lower())
    console.print(f"[green]Job {job_id} marked as '{status}'.[/green]")


@cli.command("test-email")
def test_email():
    """Send a test email to verify email delivery is working."""
    console.rule("[bold blue]Test Email Delivery[/bold blue]")

    digest_builder = DigestBuilder()
    console.print("[cyan]Sending test email...[/cyan]")
    try:
        digest_builder.send_test_email()
        console.print("[green]Test email sent successfully.[/green]")
    except Exception as exc:
        console.print(f"[red]Test email failed: {exc}[/red]")


@cli.command()
@click.option("--time", "run_time", default="07:00", show_default=True, help="Time to run daily (HH:MM, 24h format).")
def schedule(run_time):
    """Set up a daily scheduled task for the pipeline."""
    console.rule("[bold blue]Schedule Daily Run[/bold blue]")

    python_exec = sys.executable
    script_path = str(__file__)
    os_name = platform.system()

    if os_name == "Windows":
        project_dir = str(Path(__file__).resolve().parent.parent)
        task_name = "BayAreaJobHunter"
        ps_cmd = (
            f'$action = New-ScheduledTaskAction -Execute "{python_exec}" '
            f'-Argument "-m src.main run" -WorkingDirectory "{project_dir}"; '
            f'$trigger = New-ScheduledTaskTrigger -Daily -At "{run_time}"; '
            f'$settings = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable '
            f'-ExecutionTimeLimit (New-TimeSpan -Hours 2); '
            f'Register-ScheduledTask -TaskName "{task_name}" -Action $action '
            f'-Trigger $trigger -Settings $settings -Force'
        )
        console.print("[cyan]Run this in an [bold]Administrator PowerShell[/bold] to schedule with wake-from-sleep:[/cyan]")
        console.print(f"\n[bold]{ps_cmd}[/bold]\n")
        console.print(
            f"[green]This creates a task '{task_name}' that runs daily at {run_time}, "
            f"waking your PC from sleep if needed.[/green]"
        )
        console.print("[yellow]Note: Wake from sleep works for Sleep (S3) but not Hibernate or Shutdown.[/yellow]")
    else:
        hour, minute = run_time.split(":")
        cron_line = f"{minute} {hour} * * * cd \"{__file__.rsplit('src', 1)[0].rstrip('/')}\" && {python_exec} -m src.main run >> logs/cron.log 2>&1"
        console.print("[cyan]To schedule on Unix/macOS, add the following line to your crontab:[/cyan]")
        console.print(f"\n[bold]{cron_line}[/bold]\n")
        console.print("[cyan]Run [bold]crontab -e[/bold] and paste the line above to enable daily scheduling.[/cyan]")
        console.print(f"[green]The job will run every day at {run_time}.[/green]")


if __name__ == "__main__":
    cli()
