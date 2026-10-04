#!/usr/bin/env bash
# 用上一正式版引擎生成数据，再由当前引擎打开并检查数据、去重和派生索引。

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_TAG="${YT_UPGRADE_BASE_TAG:-v0.1.5}"
SPANS="${YT_UPGRADE_SPANS:-2000}"
WORK_DIR="$(mktemp -d)"
OLD_ROOT="$WORK_DIR/old"
DATA_DIR="$WORK_DIR/db"
PORT=7879
PID=""

cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

if ! git -C "$ROOT_DIR" rev-parse -q --verify "refs/tags/$BASE_TAG" >/dev/null; then
  echo "缺少升级基线 tag: $BASE_TAG" >&2
  exit 2
fi
if curl -fsS "http://127.0.0.1:$PORT/v1/healthz" >/dev/null 2>&1; then
  echo "端口 $PORT 已被占用，无法运行升级测试" >&2
  exit 2
fi

mkdir -p "$OLD_ROOT"
git -C "$ROOT_DIR" archive "$BASE_TAG" | tar -xf - -C "$OLD_ROOT"

echo "==> 构建 $BASE_TAG 引擎"
cargo build --offline --manifest-path "$OLD_ROOT/yitrace-engine/Cargo.toml" \
  -p yt-engine --example scale_bench --example server_durable --release

echo "==> 用 $BASE_TAG 生成 $SPANS 个 span"
"$OLD_ROOT/yitrace-engine/target/release/examples/scale_bench" \
  --spans "$SPANS" --queries 2 --batch 500 --data-dir "$DATA_DIR" \
  --report "$WORK_DIR/old-report.md" --keep-data

python - "$DATA_DIR" <<'PY'
from pathlib import Path
import struct
import sys

root = Path(sys.argv[1])
expected = {
    "bm25.dat": 4,
    "filter_attrs.dat": 3,
    "trace_rollup.dat": 3,
    "segment_bloom.dat": 1,
}
for name, version in expected.items():
    data = (root / name).read_bytes()
    actual = struct.unpack_from("<I", data, 4)[0]
    assert actual == version, f"{name}: expected v{version}, got v{actual}"
print("upgrade baseline sidecar versions verified")
PY

start_server() {
  local binary="$1"
  local log="$2"
  "$binary" "$DATA_DIR" >"$log" 2>&1 &
  PID=$!
  local attempt
  for attempt in $(seq 1 200); do
    if curl -fsS "http://127.0.0.1:$PORT/v1/healthz" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$PID" 2>/dev/null; then
      echo "server 提前退出" >&2
      cat "$log" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "等待 server 启动超时" >&2
  cat "$log" >&2
  return 1
}

stop_server() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  PID=""
}

request() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -fsS -X "$method" "http://127.0.0.1:$PORT$path" \
      -H 'Content-Type: application/json' -H 'X-Tenant-Id: 42' -d "$body"
  else
    curl -fsS -X "$method" "http://127.0.0.1:$PORT$path" -H 'X-Tenant-Id: 42'
  fi
}

CUSTOM_EVENT='[{"trace_id":900001,"span_id":900001,"session_id":900001,"ts":900001,"seq":1,"event_type":3,"ext_span_id":"upgrade-span","status":0,"agent_name":"upgrade-agent","attrs":{"project_id":"upgrade-project","skill":"upgrade"},"logs":["升级去重验证 盗刷"]}]'

echo "==> 记录 $BASE_TAG 查询基线"
start_server "$OLD_ROOT/yitrace-engine/target/release/examples/server_durable" "$WORK_DIR/old-server.log"
request POST /v1/ingest "$CUSTOM_EVENT" >"$WORK_DIR/old-ingest.json"
request GET /v1/traces >"$WORK_DIR/old-traces.json"
request POST /v1/search '{"text":"盗刷","k":20}' >"$WORK_DIR/old-search.json"
stop_server

echo "==> 构建当前引擎"
cargo build --offline --manifest-path "$ROOT_DIR/yitrace-engine/Cargo.toml" \
  -p yt-engine --example server_durable --release

echo "==> 用当前引擎打开 $BASE_TAG 数据目录"
CURRENT_BIN="$ROOT_DIR/yitrace-engine/target/release/examples/server_durable"
start_server "$CURRENT_BIN" "$WORK_DIR/current-first.log"
request GET /v1/traces >"$WORK_DIR/current-traces-before-retry.json"
request POST /v1/search '{"text":"盗刷","k":20}' >"$WORK_DIR/current-search.json"
request POST /v1/search '{"text":"盗刷","k":20,"filter":{"attrs":{"project_id":"scale-a"}}}' \
  >"$WORK_DIR/current-filter-search.json"
request POST /v1/trace-aggregate '{"filter":{"projectId":"scale-a"},"groupBy":["skill"],"limit":20}' \
  >"$WORK_DIR/current-rollup.json"
request GET /v1/traces/900001 >"$WORK_DIR/current-custom-trace-before-retry.json"
request POST /v1/search '{"text":"升级去重验证","k":10}' >"$WORK_DIR/current-custom-search-before-retry.json"
request POST /v1/ingest "$CUSTOM_EVENT" >"$WORK_DIR/current-retry-ingest.json"
request GET /v1/traces/900001 >"$WORK_DIR/current-custom-trace-after-retry.json"
request POST /v1/search '{"text":"升级去重验证","k":10}' >"$WORK_DIR/current-custom-search-after-retry.json"
request GET /v1/traces >"$WORK_DIR/current-traces-after-retry.json"
stop_server

python - "$WORK_DIR" "$DATA_DIR" <<'PY'
from pathlib import Path
import json
import struct
import sys

work = Path(sys.argv[1])
data_dir = Path(sys.argv[2])

def load(name):
    return json.loads((work / name).read_text(encoding="utf-8"))

old_traces = load("old-traces.json")
before = load("current-traces-before-retry.json")
after = load("current-traces-after-retry.json")
assert len(old_traces) == len(before) == len(after), (
    len(old_traces), len(before), len(after)
)
assert load("old-search.json"), "baseline search is empty"
assert load("current-search.json"), "current search lost baseline results"
assert load("current-filter-search.json"), "attrs search lost baseline results"
assert load("current-rollup.json")["items"], "rollup lost baseline results"

trace = load("current-custom-trace-after-retry.json")
spans = trace.get("spans", [])
assert len(spans) == 1, f"duplicate retry produced {len(spans)} spans"
assert load("current-custom-trace-before-retry.json") == trace
assert load("current-custom-search-before-retry.json") == load("current-custom-search-after-retry.json")

expected = {
    "filter_attrs.dat": 4,
    # 首次相关使用重建租户身份与事件顺序；clean open 本身仍不加载这些索引。
    "trace_rollup.dat": 6,
    # v1 没有整文件 CRC；当前引擎首次读取会从真实 segment 重建并升级为 v2。
    "segment_bloom.dat": 2,
}
for name, version in expected.items():
    raw = (data_dir / name).read_bytes()
    actual = struct.unpack_from("<I", raw, 4)[0]
    assert actual == version, f"{name}: expected v{version}, got v{actual}"

import zlib
catalog = (data_dir / "bm25.dat").read_bytes()
assert catalog[:8] == b"YTBMTS02", "missing CRC-protected tenant-scoped BM25 catalog"
assert zlib.crc32(catalog[:-4]) == struct.unpack_from("<I", catalog, len(catalog) - 4)[0]
count = struct.unpack_from("<Q", catalog, 24)[0]
assert len(catalog) == 36 + count * 8
for tenant in struct.unpack_from(f"<{count}Q", catalog, 32):
    child = (data_dir / f"bm25.tenant-{tenant}.dat").read_bytes()
    assert struct.unpack_from("<I", child, 4)[0] == 4, "BM25 tenant postings must remain v4"

segment_dir = data_dir / "segments"
segment_files = sorted(segment_dir.glob("seg-*.dat"))
index_files = sorted(segment_dir.glob("seg-*.idx"))
assert segment_files, "upgrade fixture has no segments"
assert len(index_files) == len(segment_files), (len(segment_files), len(index_files))
print(f"upgrade preserved {len(after)} traces and rebuilt {len(index_files)} segment indexes")
PY

if ! rg -q 'event="segment_scan_indexes_ready".*cache_loaded=false' "$WORK_DIR/current-first.log"; then
  echo "第一次打开没有安全重建 v0.1.5 的无 CRC bloom sidecar" >&2
  cat "$WORK_DIR/current-first.log" >&2
  exit 1
fi

echo "==> 再次重启并直接加载当前版本派生索引"
start_server "$CURRENT_BIN" "$WORK_DIR/current-second.log"
request POST /v1/search '{"text":"盗刷","k":20}' >/dev/null
request POST /v1/search '{"text":"盗刷","k":20,"filter":{"attrs":{"project_id":"scale-a"}}}' >/dev/null
request POST /v1/trace-aggregate '{"filter":{"projectId":"scale-a"},"groupBy":["skill"],"limit":20}' >/dev/null
stop_server

for event in bm25_cache_load segment_bloom_cache_load filter_attrs_cache_load trace_rollup_cache_load; do
  if ! rg -q "event=\"$event\"" "$WORK_DIR/current-second.log"; then
    echo "第二次重启没有直接加载 $event" >&2
    cat "$WORK_DIR/current-second.log" >&2
    exit 1
  fi
done

echo "$BASE_TAG -> 当前版本升级回归通过"
