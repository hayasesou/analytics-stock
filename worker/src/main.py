from __future__ import annotations

import argparse

from src.jobs.daily import run_daily
from src.jobs.edge_radar import run_edge_radar
from src.jobs.crypto_marketdata import run_crypto_marketdata
from src.jobs.discord_listener import run_discord_listener
from src.jobs.discord_research_listener import run_discord_research_listener
from src.jobs.executor import run_executor
from src.jobs.agents import run_agents, run_openclaw_evaluation
from src.jobs.ingest_youtube import run_ingest_youtube
from src.jobs.research import run_research
from src.jobs.research_chat import run_research_chat
from src.jobs.scheduler import run_scheduler
from src.jobs.weekly import run_weekly


def main() -> None:
    parser = argparse.ArgumentParser(description="analytics-stock worker")
    parser.add_argument(
        "job",
        choices=[
            "daily",
            "weekly",
            "research",
            "research_chat",
            "agents",
            "scheduler",
            "executor",
            "edge_radar",
            "crypto_marketdata",
            "ingest_youtube",
            "discord_listener",
            "discord_research_listener",
            "openclaw_eval",
        ],
        help="Job type",
    )
    parser.add_argument(
        "--command",
        default=None,
        help="Raw command text (e.g. '/ingest_youtube https://youtube...')",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Direct YouTube URL or video id",
    )
    args = parser.parse_args()

    if args.job == "daily":
        run_id = run_daily()
        print(f"job={args.job} run_id={run_id}")
    elif args.job == "weekly":
        run_id = run_weekly()
        print(f"job={args.job} run_id={run_id}")
    elif args.job == "research":
        run_id = run_research()
        print(f"job={args.job} run_id={run_id}")
    elif args.job == "research_chat":
        summary = run_research_chat()
        print(f"job={args.job} summary={summary}")
    elif args.job == "agents":
        run_agents()
    elif args.job == "executor":
        run_executor()
    elif args.job == "edge_radar":
        summary = run_edge_radar(scope="all")
        print(f"job={args.job} summary={summary}")
    elif args.job == "crypto_marketdata":
        summary = run_crypto_marketdata()
        print(f"job={args.job} summary={summary}")
    elif args.job == "ingest_youtube":
        summary = run_ingest_youtube(command=args.command, url=args.url)
        print(f"job={args.job} summary={summary}")
    elif args.job == "discord_listener":
        run_discord_listener()
    elif args.job == "discord_research_listener":
        run_discord_research_listener()
    elif args.job == "openclaw_eval":
        summary = run_openclaw_evaluation()
        print(f"job={args.job} summary={summary}")
    else:
        run_scheduler()


if __name__ == "__main__":
    main()
