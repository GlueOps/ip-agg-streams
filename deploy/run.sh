#!/usr/bin/env bash
# ip-agg-streams POC on the GlueOps cluster.
#
#   IMAGE=<registry>/ip-agg-streams:<tag> ./deploy/run.sh apply   # topic + StatefulSet + Service + ServiceMonitor
#   ./deploy/run.sh status    # pods, consumer lag, last pushed window per pod
#   ./deploy/run.sh views [n] # the last window's views as JSON, from pod n (default 0)
#   ./deploy/run.sh delete    # remove the app (the topics stay; see `reset`)
#   ./deploy/run.sh reset     # delete the app's consumer offsets and internal topics, for a clean re-run
#
# Build and push the image first: docker build -t "$IMAGE" . && docker push "$IMAGE"
# Turn the span copy on in the collector chart: collector.ipAggKafka.enabled=true (otel-collector-helm-chart).
set -euo pipefail
cd "$(dirname "$0")"

export NAMESPACE=${NAMESPACE:-ip-agg}
export MON_NS=${MON_NS:-glueops-core-monitoring}
export IMAGE=${IMAGE:-}
export REPLICAS=${REPLICAS:-2}
export STREAM_THREADS=${STREAM_THREADS:-3}     # REPLICAS * STREAM_THREADS <= PARTITIONS, or threads sit idle
export PARTITIONS=${PARTITIONS:-6}
export RETENTION_MS=${RETENTION_MS:-3600000}
export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-glueops-core-tempo-kafka.${MON_NS}.svc.cluster.local:9092}
export KAFKA_IMAGE=${KAFKA_IMAGE:-docker.io/apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7}
export APPLICATION_ID=${APPLICATION_ID:-ip-agg-streams}
export INPUT_TOPIC=${INPUT_TOPIC:-otlp-spans-ip}
export INPUT_FORMAT=${INPUT_FORMAT:-otlp_proto}
# Both Prometheus replicas, like the collector's prometheusremotewrite exporters.
export REMOTE_WRITE_URLS=${REMOTE_WRITE_URLS:-http://prometheus-glueops-core-kps-prometheus-0.prometheus-operated.${MON_NS}.svc:9090/api/v1/write,http://prometheus-glueops-core-kps-prometheus-1.prometheus-operated.${MON_NS}.svc:9090/api/v1/write}
export TOP_IPS=${TOP_IPS:-100}
export TOP_PAIRS=${TOP_PAIRS:-100}
export TOP_IPS_PER_ROUTE=${TOP_IPS_PER_ROUTE:-10}
export MAX_ROUTES=${MAX_ROUTES:-200}
export CPU_REQUEST=${CPU_REQUEST:-500m}
export MEMORY=${MEMORY:-1536Mi}

kafka() {  # kafka <script> <args...>: run a Kafka CLI in a throwaway pod next to the broker
  kubectl -n "$MON_NS" run "ip-agg-kafka-cli-$RANDOM" --rm -i --restart=Never --quiet --image="$KAFKA_IMAGE" \
    --command -- "/opt/kafka/bin/$1" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "${@:2}"
}

case "${1:-}" in
  apply)
    [ -n "$IMAGE" ] || { echo "set IMAGE to the pushed ip-agg-streams image" >&2; exit 1; }
    kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || kubectl create ns "$NAMESPACE"
    envsubst < k8s.yaml | kubectl apply -f -
    echo "pushing to: $REMOTE_WRITE_URLS"
    ;;
  status)
    kubectl -n "$NAMESPACE" get pods -l app=ip-agg-streams -o wide
    for p in $(kubectl -n "$NAMESPACE" get pods -l app=ip-agg-streams -o name); do
      echo "$p: $(kubectl -n "$NAMESPACE" logs "$p" --tail=200 2>/dev/null | grep ' window ' | tail -1)"
    done
    kafka kafka-consumer-groups.sh --describe --group "$APPLICATION_ID" || true
    ;;
  views)
    kubectl -n "$NAMESPACE" port-forward "pod/ip-agg-streams-${2:-0}" 18080:8080 >/dev/null 2>&1 &
    pf=$!; trap 'kill $pf 2>/dev/null' EXIT
    for _ in $(seq 30); do curl -s -o /dev/null localhost:18080/actuator/health && break; sleep 0.5; done
    curl -s localhost:18080/views/latest | python3 -m json.tool
    ;;
  delete)
    envsubst < k8s.yaml | kubectl delete --ignore-not-found -f -
    ;;
  reset)
    kubectl -n "$NAMESPACE" get pods -l app=ip-agg-streams -o name | grep -q . \
      && { echo "delete the app first (./deploy/run.sh delete)" >&2; exit 1; }
    kafka kafka-consumer-groups.sh --delete --group "$APPLICATION_ID" || true
    kafka kafka-topics.sh --delete --topic "$APPLICATION_ID-by-ip-repartition" || true
    kafka kafka-topics.sh --delete --topic "$APPLICATION_ID-ip-route-counts-changelog" || true
    ;;
  *)
    sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
