# server-lite (No internal private deps)

`server-lite` is an Android/Waydroid-only Kafka detect worker without company-private imports.

## Build

```bash
cd cmd/server-lite
GOWORK=off go mod tidy
GOWORK=off go build
```

## Run (env-first)

```bash
export KAFKA_BROKERS="13.251.208.168:9092"
export KAFKA_GROUP="detect-agent-lite"
export KAFKA_IN_TOPIC="<intercept_detect_chrome_topic>"
export KAFKA_OUT_TOPIC="<intercept_detect_result_topic>"
export KAFKA_HEARTBEAT_TOPIC="<heartbeat_report_topic>"

# Optional split brokers (override KAFKA_BROKERS fallback)
# export KAFKA_IN_BROKERS="13.251.208.168:9092"
# export KAFKA_OUT_BROKERS="10.0.24.217:9092"
# export KAFKA_HEARTBEAT_BROKERS="13.251.208.168:9092"

export WAYDROID_SERIAL="192.168.240.112:5555"
export WAYDROID_PACKAGE="com.mi.globalbrowser"
export WAYDROID_PORT=9222
export WAYDROID_MAX_TABS=10

GOWORK=off ./server-lite -listen :19080
```

## Run (your current broker split)

```bash
export KAFKA_GROUP="intercetp_detect_agent"
export KAFKA_IN_TOPIC="intercept_detect_mi"
export KAFKA_OUT_TOPIC="task-results"
export KAFKA_HEARTBEAT_TOPIC="intercept_detect_data_report"

export KAFKA_IN_BROKERS="13.251.208.168:9092"
export KAFKA_HEARTBEAT_BROKERS="13.251.208.168:9092"
export KAFKA_OUT_BROKERS="10.0.24.217:9092"

export WAYDROID_SERIAL="192.168.240.112:5555"
export WAYDROID_PACKAGE="com.mi.globalbrowser"
export WAYDROID_PORT=9322
export WAYDROID_MAX_TABS=10

GOWORK=off ./server-lite -listen :19080
```

## Run with Nacos config source (optional)

If `KAFKA_BROKERS` is empty, it tries loading `kafka` section from nacos config.

```bash
GOWORK=off ./server-lite \
  -listen :19080 \
  -nacos-addr "47.129.241.108:8848,54.179.140.50:8848" \
  -nacos-user nacos \
  -nacos-pass nacos \
  -nacos-namespace observable-dev \
  -nacos-group boce \
  -nacos-dataid intercept-detect \
  -serial 192.168.240.112:5555 \
  -package com.mi.globalbrowser \
  -port 9222 \
  -max-tabs 10
```

## Health check

```bash
curl -s http://127.0.0.1:19080/healthz
```

It returns runtime serial/socket/port/maxTabs/pageTabs and kafka topic info.

It also returns current broker routing info:

- `inBrokers`
- `outBrokers`
- `heartbeatBrokers`

## Kafka input JSON

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

## Strategy behavior

- `pageTabs < maxTabs` -> open new tab (`strategy=new`)
- `pageTabs >= maxTabs` and idle exists -> reuse idle tab (`strategy=reuse_idle`)
- `pageTabs >= maxTabs` and no idle -> fail (`strategy=fail_no_idle`)

`strategy` is included in result detail JSON (`outputJson.rawResult`).

## Validate with kcat

```bash
# produce one task
cat ../../examples/task_create_request.sample.json \
| kcat -b "13.251.208.168:9092" -t "intercept_detect_mi" -P

# consume detect results
kcat -b "10.0.24.217:9092" -t "task-results" -C -o -20

# consume heartbeat reports
kcat -b "13.251.208.168:9092" -t "intercept_detect_data_report" -C -o -20
```
