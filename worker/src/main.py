from __future__ import annotations

import argparse

from src.jobs.daily import run_daily
from src.jobs.executor import run_executor
from src.jobs.agents import run_agents
from src.jobs.research import run_research
from src.jobs.scheduler import run_scheduler
from src.jobs.weekly import run_weekly


def main() -> None:
    parser = argparse.ArgumentParser(description="analytics-stock worker")
    parser.add_argument(
        "job",
        choices=["daily", "weekly", "research", "agents", "scheduler", "executor"],
        help="Job type",
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
    elif args.job == "agents":
        run_agents()
    elif args.job == "executor":
        run_executor()
    else:
        run_scheduler()


if __name__ == "__main__":
    main()
