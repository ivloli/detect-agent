# Waydroid Local Minimal Verify Runbook

This runbook verifies `Waydroid + CDP + tab open/list` in `detect-agent` without Kafka, heartbeat, or node reporting.

## What this mode does

- Runs `cmd/waydroid-local` only.
- Uses ADB + dynamic devtools socket detection.
- Remaps one local port (default `9322`) when socket changes.
- Exposes 3 local APIs:
 - Exposes 4 local APIs:
  - `GET /healthz`
  - `GET /tabs/list`
  - `POST /tabs/open`
  - `POST /mock/consume` (TaskCreateRequest-like input -> NodeMessage-like output)

## Prerequisites

- Waydroid session is already running on target host.
- Target browser is installed in Waydroid.
- `adb devices -l` shows one online device (or pass `--serial`).

## Build

```bash
cd /Users/hechuan/Git_repos/GAI/detect-agent
go build ./cmd/waydroid-local
```

## Start (Mi browser example)

```bash
cd /Users/hechuan/Git_repos/GAI/detect-agent

go run ./cmd/waydroid-local \
  --listen :18080 \
  --serial 192.168.240.112:5555 \
  --package com.mi.globalbrowser \
  --port 9222
```

## Start (Vivo browser example)

```bash
cd /Users/hechuan/Git_repos/GAI/detect-agent

go run ./cmd/waydroid-local \
  --listen :18081 \
  --serial 192.168.240.112:5555 \
  --package com.vivo.browser \
  --port 9322
```

Tip: run Mi and Vivo in different local ports to avoid collisions.

## Verify APIs

### 1) Health

```bash
curl -s http://127.0.0.1:18080/healthz
```

Expected: `ok=true`, plus active `socket` and `cdpPort`.

### 2) List tabs

```bash
curl -s http://127.0.0.1:18080/tabs/list
```

### 3) Open one URL as a new navigation/tab and return latest tab list

```bash
curl -s -X POST http://127.0.0.1:18080/tabs/open \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com"}'
```

### 4) Mock Kafka consume/writeback schema locally (without Kafka)

This endpoint accepts TaskCreateRequest-like JSON and returns NodeMessage-like JSON.

```bash
curl -s -X POST http://127.0.0.1:18080/mock/consume \
  -H 'Content-Type: application/json' \
  -d '{
    "timeoutSec": 10,
    "deadline": "",
    "type": "INTERCEPT_DETECT",
    "payloadJson": "{\"url\":\"https://example.com\"}",
    "taskMeta": {"taskId":"demo-1","taskSeqId":"1"}
  }'
```

If you want human-readable nested JSON fields, use `expanded=true`:

```bash
curl -s -X POST 'http://127.0.0.1:18080/mock/consume?expanded=true' \
  -H 'Content-Type: application/json' \
  -d '{
    "timeoutSec": 10,
    "deadline": "",
    "type": "INTERCEPT_DETECT",
    "payloadJson": "{\"url\":\"https://example.com\"}",
    "taskMeta": {"taskId":"demo-1","taskSeqId":"1"}
  }' | jq
```

In expanded mode response adds:

- `eventData.execResult.outputJsonExpanded`
- `eventData.execResult.rawResultExpanded`

Expected output shape:

- `eventType=EVENT_TYPE_EXEC_RESULT`
- `msgStatus=MESSAGE_STATUS_COMPLETE`
- `eventData.execResult.in` echoes input
- `eventData.execResult.outputJson` contains detect result JSON

## Notes

- This mode does not consume Kafka and does not report heartbeat.
- It is intended for runtime/CDP validation before integrating with full detect-agent pipeline.
- For browsers that expose socket but do not serve `/json/list`, API may fail with empty/invalid response.
