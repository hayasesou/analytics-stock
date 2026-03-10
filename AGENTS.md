# Repo Read Guide

- 巨大ファイルは丸ごと `read` しない。先に `rg -n "^def |^class |^export async function |^function "` でシンボル一覧を取る。
- 300 行超のファイルは `sed -n 'start,endp'` で範囲指定して読む。
- まず読む順序:
  1. `README.md`
  2. `worker/src/storage/README.md` または `web/lib/repository/README.md`
  3. 対象シンボルだけ本体を範囲指定で読む
- `worker/src/storage/db.py` と `web/lib/repository.ts` は facade。詳細実装は隣接ディレクトリの分割ファイルを見る。
