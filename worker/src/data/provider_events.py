from __future__ import annotations

from datetime import datetime, timedelta
import uuid

from src.types import EventItem

SEC_HIGH_FORMS = {"8-K", "6-K"}
SEC_MEDIUM_FORMS = {"10-K", "10-Q", "20-F", "DEF 14A"}
EDINET_HIGH_KEYWORDS = ("臨時報告書", "大量保有", "公開買付", "自己株券買付")
EDINET_MEDIUM_KEYWORDS = ("有価証券報告書", "四半期報告書", "半期報告書", "決算")


class ProviderEventsMixin:
    def load_recent_events(self, now: datetime, hours: int = 24) -> list[EventItem]:
        since = now - timedelta(hours=max(1, int(hours)))
        sec_events = self._load_recent_events_sec(since=since, now=now)
        edinet_events = self._load_recent_events_edinet(since=since, now=now)
        live_events = sec_events + edinet_events
        if not live_events:
            print(
                f"[provider] recent_events source=live count=0 sec={len(sec_events)} edinet={len(edinet_events)}",
                flush=True,
            )
            return []

        deduped: dict[tuple[str, str], EventItem] = {}
        for event in live_events:
            source = str(event.source_url or event.title)
            key = (source, event.event_time.isoformat())
            existing = deduped.get(key)
            if existing is None or event.event_time > existing.event_time:
                deduped[key] = event
        events = sorted(deduped.values(), key=lambda item: item.event_time, reverse=True)
        print(
            f"[provider] recent_events source=live count={len(events)} sec={len(sec_events)} edinet={len(edinet_events)}",
            flush=True,
        )
        return events

    @staticmethod
    def _event_doc_version_id(source: str, event_time: datetime) -> str:
        raw = f"{str(source or '').strip() or 'event'}:{event_time.isoformat()}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))

    @staticmethod
    def _parse_event_time(raw: object) -> datetime | None:
        if isinstance(raw, datetime):
            parsed = raw
        else:
            text = str(raw or "").strip()
            if not text:
                return None
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        parsed = datetime.strptime(text, fmt)  # noqa: DTZ007
                        break
                    except ValueError:
                        continue
                else:
                    return None
        return parsed.astimezone().replace(tzinfo=None) if parsed.tzinfo is not None else parsed

    @staticmethod
    def _importance_from_sec_form(form_type: str) -> str:
        form = str(form_type or "").strip().upper()
        if form in SEC_HIGH_FORMS:
            return "high"
        if form in SEC_MEDIUM_FORMS:
            return "medium"
        return "low"

    @staticmethod
    def _importance_from_edinet_description(description: str) -> str:
        desc = str(description or "").strip()
        if any(keyword in desc for keyword in EDINET_HIGH_KEYWORDS):
            return "high"
        if any(keyword in desc for keyword in EDINET_MEDIUM_KEYWORDS):
            return "medium"
        return "low"

    def _load_recent_events_sec(self, since: datetime, now: datetime) -> list[EventItem]:
        sec_client = self._make_sec_client()
        try:
            rows = sec_client.fetch_current_filings(count=100)
        except Exception as exc:  # noqa: BLE001
            print(f"[provider] recent_events_error source=sec error={exc}", flush=True)
            return []

        events: list[EventItem] = []
        for row in rows:
            event_time = self._parse_event_time(row.get("updated"))
            if event_time is None or event_time < since or event_time > (now + timedelta(hours=2)):
                continue
            form_type = str(row.get("form_type") or "").strip().upper()
            company_name = str(row.get("company_name") or "").strip()
            source_url = str(row.get("source_url") or "").strip() or None
            title = f"{form_type}: {company_name}" if form_type and company_name else str(row.get("title") or "").strip()
            if not title:
                continue
            summary = str(row.get("summary") or "").strip() or "SEC filing update."
            events.append(
                EventItem(
                    event_type="filing",
                    importance=self._importance_from_sec_form(form_type),
                    event_time=event_time,
                    title=title,
                    summary=summary,
                    source_url=source_url,
                    security_id=None,
                    doc_version_id=self._event_doc_version_id(source_url or title, event_time),
                    metadata={"source": "sec", "form_type": form_type or None},
                )
            )
        return events

    def _load_recent_events_edinet(self, since: datetime, now: datetime) -> list[EventItem]:
        client = self._make_edinet_client()
        if not client.available():
            return []
        events: list[EventItem] = []
        for target_date in sorted({since.date(), now.date()}):
            try:
                rows = client.fetch_documents_list(target_date.isoformat())
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[provider] recent_events_error source=edinet date={target_date.isoformat()} error={exc}",
                    flush=True,
                )
                continue
            for row in rows:
                doc_id = str(row.get("docID") or row.get("docId") or "").strip()
                event_time = self._parse_event_time(row.get("submitDateTime") or row.get("submitDate"))
                if event_time is None or event_time < since or event_time > (now + timedelta(hours=2)):
                    continue
                filer_name = str(row.get("filerName") or row.get("submitterName") or "").strip()
                doc_desc = str(row.get("docDescription") or row.get("description") or "").strip()
                if not filer_name and not doc_desc:
                    continue
                title = " - ".join(part for part in [filer_name, doc_desc] if part)
                source_url = f"https://disclosure2.edinet-fsa.go.jp/WEEK0010.aspx?DocID={doc_id}" if doc_id else None
                events.append(
                    EventItem(
                        event_type="filing",
                        importance=self._importance_from_edinet_description(doc_desc),
                        event_time=event_time,
                        title=title,
                        summary=doc_desc or "EDINET filing update.",
                        source_url=source_url,
                        security_id=None,
                        doc_version_id=self._event_doc_version_id(source_url or title, event_time),
                        metadata={
                            "source": "edinet",
                            "doc_id": doc_id or None,
                            "form_code": str(row.get("formCode") or "").strip() or None,
                        },
                    )
                )
        return events
