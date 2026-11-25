#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python -m venv .venv
fi

. .venv/bin/activate
pip -q install -r requirements.txt

if [ ! -f .env ]; then
  cp .env.example .env
  echo "已生成 .env，请填入 DASHSCOPE_API_KEY 和 DASHSCOPE_WORKSPACE_ID 后重新运行。" >&2
  exit 1
fi

exec python server.py
