#!/bin/bash
# 中部電力PGの停電データをローカルで取得し、data-cacheブランチに push する。
# cron から実行: */10 * * * * /path/to/scripts/update_chubu_cache.sh >> /tmp/chubu_cache.log 2>&1

set -euo pipefail

REPO_DIR="/Users/kt/src/outage-dashboard"
WORKTREE_DIR="/tmp/chubu-cache-wt"
CACHE_BRANCH="data-cache"
PYTHON="python3"

echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

cd "$REPO_DIR"

# Step 1: データ取得（失敗時はキャッシュを変更せず終了）
if ! "$PYTHON" scripts/fetch_chubu_cache.py; then
    echo "Fetch failed — cache not updated"
    exit 0
fi

# Step 2: data-cache ブランチ用 worktree のセットアップ
git fetch origin "$CACHE_BRANCH" --quiet
if [ ! -d "$WORKTREE_DIR/.git" ]; then
    git worktree add "$WORKTREE_DIR" "$CACHE_BRANCH" 2>/dev/null || true
fi

# Step 3: worktree を最新に合わせてからキャッシュを更新
cd "$WORKTREE_DIR"
git pull origin "$CACHE_BRANCH" --rebase --quiet
cp "$REPO_DIR/cache/chubu.json" cache/chubu.json
git add cache/chubu.json

if git diff --staged --quiet; then
    echo "No changes — push skipped"
    exit 0
fi

git commit -m "Update Chubu cache $(date -u +%Y-%m-%dT%H:%M:%SZ) [local]" \
    --author="cron <cron@local>"
git push origin "$CACHE_BRANCH" --quiet

echo "Cache pushed successfully"
