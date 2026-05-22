# Kafka Message Structures (Detect Agent)

This document summarizes the message payload shapes used by the current detect-agent pipeline for:

- Task input topic (consume)
- Detect result topic (produce)
- Heartbeat topic (produce)

> Notes
>
> - These structures are inferred from current code paths in `internal/biz/batch_detect_handler.go` and `internal/biz/node_reporter.go`.
> - Enum values are represented as strings in examples for readability.

---

## 1) Task Topic (Input)

Topic examples:

- `intercept_detect_chrome`
- `intercept_detect_mi` (new custom topic if you split Xiaomi route)

Consumed as `TaskCreateRequest` and then `payloadJson` is parsed as `InterceptDetectParam`.

### Minimal JSON example

```json
{
  "timeoutSec": 10,
  "deadline": "",
  "type": "INTERCEPT_DETECT",
  "payloadJson": "{\"url\":\"https://example.com\"}",
  "taskMeta": {
    "taskId": "demo-kafka-1",
    "taskSeqId": "1"
  }
}
```

### Required semantic fields

- `payloadJson`: must contain `url`
- `taskMeta`: recommended (used to correlate output)

---

## 2) Result Topic (Output)

Topic example:

- `task-results`

Produced as `NodeMessage` with `eventType=EXEC_RESULT` and `eventData.execResult` payload.

### JSON shape example

```json
{
  "eventType": "EVENT_TYPE_EXEC_RESULT",
  "messageId": "01JXXXXXXX...",
  "timestamp": 1779352942250,
  "nodeId": "",
  "taskMeta": {
    "taskId": "demo-kafka-1",
    "taskSeqId": "1"
  },
  "msgStatus": "MESSAGE_STATUS_COMPLETE",
  "eventData": {
    "execResult": {
      "in": {
        "timeoutSec": 10,
        "deadline": "",
        "type": "INTERCEPT_DETECT",
        "payloadJson": "{\"url\":\"https://example.com\"}"
      },
      "errorCode": 200,
      "errorMessage": "",
      "errorRawMessage": "",
      "finishedAt": "2026-05-21T08:42:22Z",
      "outputJson": "{\"app\":\"com.mi.globalbrowser\",\"status\":\"NORMAL\",\"error\":\"\",\"rawResult\":\"...\"}"
    }
  }
}
```

### `outputJson` inner object (detect result)

Typical fields:

```json
{
  "app": "com.mi.globalbrowser",
  "status": "NORMAL|BLOCKED|FAIL",
  "error": "",
  "rawResult": "stringified runtime details"
}
```

---

## 3) Heartbeat Topic (Output)

Topic example:

- `intercept_detect_data_report`

Produced as `InterceptNodeInfo` style payload.

### JSON shape example

```json
{
  "reportType": "INTERCEPT_REPORT_TYPE_HEARTBEAT",
  "nodeType": "INTERCEPT_NODE_TYPE_BROWSER_FARM",
  "nodeName": "",
  "publicIpv4": "",
  "timestamp": "2026-05-21T09:00:00Z",
  "nodeDetails": [
    {
      "appName": "INTERCEPT_APP_TYPE_CHROME",
      "appNum": 1
    }
  ]
}
```

At service startup there is usually one register message first:

- `reportType = INTERCEPT_REPORT_TYPE_REGISTER`

Then periodic heartbeat messages.

---

## 4) Mapping with current topics

From current runtime config sample:

- Input (task):
  - `intercept_detect_chrome` (or custom `intercept_detect_mi`)
- Output (result):
  - `task-results`
- Output (heartbeat):
  - `intercept_detect_data_report`

Current broker split used by your team:

- Task input + heartbeat: `13.251.208.168:9092`
- Result output: `10.0.24.217:9092`

---

## 5) Quick kcat examples

### Produce one task message

```bash
cat examples/task_create_request.sample.json \
| kcat -b "<BROKERS>" -t "intercept_detect_chrome" -P
```

### Consume latest results

```bash
kcat -b "<BROKERS>" -t "task-results" -C -o -20
```

### Consume heartbeat

```bash
kcat -b "<BROKERS>" -t "intercept_detect_data_report" -C -o -20
```

---

## 6) Local sample files

- `examples/task_create_request.sample.json`
- `examples/node_message_result.sample.json`
- `examples/heartbeat_report.sample.json`
