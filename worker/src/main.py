from __future__ import annotations

import argparse

from src.jobs.daily import run_daily
from src.jobs.scheduler import run_scheduler
from src.jobs.weekly import run_weekly


def main() -> None:
    parser = argparse.ArgumentParser(description="analytics-stock worker")
    parser.add_argument("job", choices=["daily", "weekly", "scheduler"], help="Job type")
    args = parser.parse_args()

    if args.job == "daily":
        run_id = run_daily()
        print(f"job={args.job} run_id={run_id}")
    elif args.job == "weekly":
        run_id = run_weekly()
        print(f"job={args.job} run_id={run_id}")
    else:
        run_scheduler()


if __name__ == "__main__":
    main()
