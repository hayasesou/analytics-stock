# Claude Read Guide

- 先に index/README を読む。巨大ファイルの全量 read は避ける。
- Python は `rg -n "^    def |^def |^class "`、TypeScript は `rg -n "^export async function |^function "` を使って関心範囲を絞る。
- `worker/src/storage/db.py` は facade のみ。実処理は `db_*.py` に分散している。
- `web/lib/repository.ts` は barrel export のみ。実処理は `web/lib/repository/*.ts` を読む。
- 新しい大きめの領域を作るときは、同じ階層に短い README/code map を置く。
