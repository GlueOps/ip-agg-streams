#!/usr/bin/env bash
# Run ip-agg-streams on this machine against the GlueOps cluster, without deploying it: read Tempo's own
# `tempo-traces` topic through a port-forward and remote-write the views to the cluster's Prometheus replicas.
#
#   ./deploy/local.sh run     # port-forwards, build, start the app in the foreground (Ctrl-C stops all of it)
#   ./deploy/local.sh check   # from a second terminal: input counters, consumer lag, the last window's top IPs
#   ./deploy/local.sh reset   # after stopping: delete this machine's consumer group, internal topics and local state
#
# One-time setup: the broker advertises its in-cluster name, so that name has to point at the port-forward:
#   echo "127.0.0.1 glueops-core-tempo-kafka.glueops-core-monitoring.svc.cluster.local" | sudo tee -a /etc/hosts
#
# Tempo is not affected: the app reads with its own consumer group (ip-agg-local-<user>). It does create two small
# internal topics on Tempo's broker (<group>-by-ip-repartition, <group>-ip-route-counts-changelog); `reset` removes them.
set -euo pipefail
cd "$(dirname "$0")/.."

MON_NS=${MON_NS:-glueops-core-monitoring}
KAFKA_SVC=${KAFKA_SVC:-glueops-core-tempo-kafka}
KAFKA_HOST=${KAFKA_HOST:-$KAFKA_SVC.$MON_NS.svc.cluster.local}   # must equal the broker's advertised host
KAFKA_PORT=${KAFKA_PORT:-9092}                                    # and port: the client reconnects to exactly this
PROM_PODS=${PROM_PODS:-prometheus-glueops-core-kps-prometheus-0 prometheus-glueops-core-kps-prometheus-1}
PROM_LOCAL_PORT=${PROM_LOCAL_PORT:-19090}                         # replica i is forwarded to PROM_LOCAL_PORT + i
APP_PORT=${APP_PORT:-8080}
WHO=$(whoami | tr -c 'a-zA-Z0-9\n' '-')
export KAFKA_APPLICATION_ID=${KAFKA_APPLICATION_ID:-ip-agg-local-$WHO}
export POD_NAME=${POD_NAME:-local-$WHO}                            # the `instance` label of the pushed series
export INPUT_TOPIC=${INPUT_TOPIC:-tempo-traces}
export INPUT_FORMAT=${INPUT_FORMAT:-tempo}                         # otlp_proto if INPUT_TOPIC=otlp-spans-ip
export KAFKA_STREAM_THREADS=${KAFKA_STREAM_THREADS:-3}             # tempo-traces has 3 partitions
export KAFKA_STATE_DIR=${KAFKA_STATE_DIR:-$PWD/target/local-state}
export SERVER_PORT=$APP_PORT

broker() {  # broker <script> <args...>: a Kafka CLI inside the broker pod (no local Kafka tools needed)
  kubectl -n "$MON_NS" exec "deploy/$KAFKA_SVC" -- "/opt/kafka/bin/$1" --bootstrap-server localhost:9092 "${@:2}"
}

forward() {  # forward <target> <local>:<remote>: keep a port-forward up (pods restart) until this script exits
  ( while true; do kubectl -n "$MON_NS" port-forward "$1" "$2" >/dev/null 2>&1 || true; sleep 1; done ) &
}

wait_port() {
  for _ in $(seq 60); do nc -z localhost "$1" 2>/dev/null && return 0; sleep 0.5; done
  echo "port-forward to localhost:$1 did not come up" >&2; exit 1
}

case "${1:-}" in
  run)
    echo "kube context: $(kubectl config current-context)"
    kubectl -n "$MON_NS" get "deploy/$KAFKA_SVC" >/dev/null
    resolved=$(python3 -c "import socket,sys; print(socket.gethostbyname(sys.argv[1]))" "$KAFKA_HOST" 2>/dev/null || true)
    if [ "$resolved" != "127.0.0.1" ]; then
      echo "$KAFKA_HOST resolves to '${resolved:-nothing}', not 127.0.0.1. Run once:" >&2
      echo "  echo \"127.0.0.1 $KAFKA_HOST\" | sudo tee -a /etc/hosts" >&2
      exit 1
    fi
    for port in "$KAFKA_PORT" "$APP_PORT"; do
      if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "localhost:$port is already in use" >&2; exit 1
      fi
    done
    broker kafka-topics.sh --describe --topic "$INPUT_TOPIC" | head -1

    trap 'trap - EXIT INT TERM; kill 0 2>/dev/null' EXIT INT TERM
    forward "svc/$KAFKA_SVC" "$KAFKA_PORT:9092"
    urls=() i=0
    for pod in $PROM_PODS; do
      forward "pod/$pod" "$((PROM_LOCAL_PORT + i)):9090"
      urls+=("http://localhost:$((PROM_LOCAL_PORT + i))/api/v1/write")
      i=$((i + 1))
    done
    wait_port "$KAFKA_PORT"
    for u in "${urls[@]}"; do
      port=${u#http://localhost:}; port=${port%%/*}
      wait_port "$port"
      # An empty body: 400 means the remote-write receiver is on, 404 that it is off.
      code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$u" || true)
      [ "$code" = 404 ] && { echo "$u: remote-write receiver is not enabled (HTTP 404)" >&2; exit 1; }
    done
    REMOTE_WRITE_URLS=$(IFS=,; echo "${urls[*]}")
    export REMOTE_WRITE_URLS
    export KAFKA_BOOTSTRAP_SERVERS="$KAFKA_HOST:$KAFKA_PORT"

    mvn -q -DskipTests package
    echo "group $KAFKA_APPLICATION_ID reads $INPUT_TOPIC ($INPUT_FORMAT), pushes instance=$POD_NAME to: $REMOTE_WRITE_URLS"
    echo "first window after ~1-2 minutes; ./deploy/local.sh check in another terminal"
    java -jar target/ip-agg-streams-*.jar
    ;;
  check)
    curl -sf "localhost:$APP_PORT/actuator/health/liveness" || { echo "app is not running on :$APP_PORT" >&2; exit 1; }
    echo
    curl -s "localhost:$APP_PORT/actuator/prometheus" \
      | grep -E '^ipagg_(input_spans_total|input_messages_failed_total|windows_total|remote_write_series_total|window_emit_delay_seconds)' || true
    broker kafka-consumer-groups.sh --describe --group "$KAFKA_APPLICATION_ID" 2>/dev/null | grep -E "GROUP|$INPUT_TOPIC" || true
    curl -s "localhost:$APP_PORT/views/latest" | python3 -c '
import json, sys, datetime
body = sys.stdin.read()
if not body.strip():
    sys.exit("no window emitted yet (the first one closes ~1-2 minutes after start)")
v = json.loads(body)
end = datetime.datetime.fromtimestamp(v["windowEndMs"] / 1000).strftime("%H:%M:%S")
print("window ending %s: %d requests, %d unique IPs, %d routes"
      % (end, v["totalRequests"], v["uniqueIps"], v["distinctRoutes"]))
for r in v["topIps"][:10]:
    print("  %8d  %s" % (r["count"], r["ip"]))
'
    ;;
  reset)
    if curl -s -o /dev/null "localhost:$APP_PORT/actuator/health"; then
      echo "stop ./deploy/local.sh run first" >&2; exit 1
    fi
    broker kafka-consumer-groups.sh --delete --group "$KAFKA_APPLICATION_ID" || true
    broker kafka-topics.sh --delete --topic "$KAFKA_APPLICATION_ID-by-ip-repartition" || true
    broker kafka-topics.sh --delete --topic "$KAFKA_APPLICATION_ID-ip-route-counts-changelog" || true
    rm -rf "$KAFKA_STATE_DIR"
    ;;
  *)
    sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
