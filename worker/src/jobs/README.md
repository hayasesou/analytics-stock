# Jobs Map

- `executor.py`: facade。実処理は `executor_runtime.py`, `executor_prechecks.py`, `executor_*_runtime.py`, `executor_*_support.py` に分割済み。入口ファイル自体は budget 内。
- `agents.py`: facade。実処理は `agents_support.py`, `agents_runtime.py`, `agents_evaluation.py` に分割済み。
- `research.py`: facade。実処理は `research_runtime.py`, `research_lifecycle.py`, `research_support.py` に分割済み。
- `weekly.py`: facade。実処理は `weekly_runtime.py`, `weekly_support.py` に分割済み。
- `edge_radar.py`: facade。実処理は `edge_radar_runtime.py`, `edge_radar_support.py` に分割済み。
- `ingest_youtube.py`: facade。実処理は `ingest_youtube_runtime.py`, `ingest_youtube_support.py` に分割済み。
- `research_chat.py`: facade。実処理は `research_chat_runtime.py`, `research_chat_tasks.py`, `research_chat_support.py`, `research_chat_charts.py` に分割済み。
