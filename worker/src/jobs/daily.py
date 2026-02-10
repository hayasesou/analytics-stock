from __future__ import annotations

from datetime import datetime
import traceback

from src.config import load_runtime_secrets, load_yaml_config
from src.data.provider import HybridDataProvider
from src.integrations.discord import DiscordNotifier
from src.llm.reporting import generate_event_digest_report
from src.storage.db import NeonRepository
from src.storage.r2 import R2Storage


def run_daily() -> str:
    cfg = load_yaml_config()
    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)
    provider = HybridDataProvider(secrets)
    notifier = DiscordNotifier(secrets.discord_webhook_url)
    r2 = R2Storage(
        endpoint_url=secrets.r2_endpoint,
        access_key_id=secrets.r2_access_key_id,
        secret_access_key=secrets.r2_secret_access_key,
        bucket_evidence=secrets.r2_bucket_evidence,
        bucket_data=secrets.r2_bucket_data,
    )

    run_id = repo.create_run("daily", str(cfg.get("version", "1.1")), metadata={"baseline": True})

    try:
        now = datetime.now()
        events = provider.load_recent_events(now=now, hours=24)

        # Daily event records
        repo.insert_events(run_id, events, security_uuid_map={})

        event_dicts = [
            {
                "event_type": e.event_type,
                "importance": e.importance,
                "event_time": e.event_time.isoformat(),
                "title": e.title,
                "summary": e.summary,
                "source_url": e.source_url,
                "doc_version_id": e.doc_version_id,
            }
            for e in events
        ]

        report = generate_event_digest_report(now, event_dicts)
        report_id = repo.insert_report(run_id, report, security_uuid_map={})

        if r2.available():
            r2.put_json(f"daily/{now.date().isoformat()}/events.json", {"run_id": run_id, "events": event_dicts})
            r2.put_text(
                f"daily/{now.date().isoformat()}/event_digest_{report_id}.md",
                report.body_md,
                evidence=True,
            )

        notifier.send_daily_event_digest(now, event_dicts)

        repo.finish_run(run_id, "success", metadata={"event_count": len(events), "report_id": report_id})
    except Exception as exc:  # noqa: BLE001
        repo.finish_run(
            run_id,
            "failed",
            metadata={"error": str(exc), "trace": traceback.format_exc()[-8000:]},
        )
        raise

    return run_id
