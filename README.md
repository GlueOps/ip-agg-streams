# ip-agg-streams

Top client IPs, overall, per (IP, method, route) and per route, without one Prometheus series per IP.

```
apps ─OTLP─▶ otel-collector ─┬─▶ Tempo (unchanged)
                             └─ kafka/ip_agg ─▶ topic otlp-spans-ip
                                                   │
                       ip-agg-streams (Kafka Streams, StatefulSet)
                         parse SERVER spans → (ip, method, route) counts per message
                         repartition by IP → 1-minute tumbling windows (RocksDB, changelog in Kafka)
                         each closed window → top-N views → remote write ─▶ both Prometheus replicas ─▶ Grafana
```

The per-IP counting happens in the app's state store. Prometheus only gets a fixed number of series:
`TOP_IPS + TOP_PAIRS + MAX_ROUTES × TOP_IPS_PER_ROUTE + 3` per pod (≤ 2,203 with the defaults, usually far fewer), the
same at 1k or 10M distinct IPs.

## Pushed metrics (remote write, stamped with the window end)

| Metric | Labels | Meaning |
|---|---|---|
| `ip_agg_top_ip_requests` | `ip` | top `TOP_IPS` IPs by requests over all methods/routes |
| `ip_agg_top_ip_route_requests` | `ip, method, route` | top `TOP_PAIRS` (IP, method, route) combinations |
| `ip_agg_route_top_ip_requests` | `method, route, ip` | for the `MAX_ROUTES` busiest routes, their top `TOP_IPS_PER_ROUTE` IPs |
| `ip_agg_unique_ips` | | distinct IPs in the window (exact) |
| `ip_agg_requests` | | requests in the window |
| `ip_agg_distinct_routes` | | distinct (method, route) in the window |
| `ip_agg_last_push_timestamp_seconds` | | heartbeat (Prometheus has no `up` for pushed data) |

All carry `job="ip-agg-streams"`, `instance=<pod>`, `window="60s"`. Values are requests per window, not rates.
An IP that leaves a top list gets a staleness marker, so it disappears at once instead of lingering for 5 minutes.

Health metrics are scraped (ServiceMonitor, `/actuator/prometheus`), prefixed `ipagg_` plus Kafka Streams' own:
`ipagg_input_spans_total{result}`, `ipagg_input_messages_failed_total`, `ipagg_remote_write_series_total{result}`,
`ipagg_windows_total{result}`, `ipagg_window_emit_delay_seconds`, `kafka_consumer_fetch_manager_records_lag_max`.

### Queries

```promql
topk(20, max by (ip) (ip_agg_top_ip_requests))                                   # top IPs
topk(20, max by (ip, method, route) (ip_agg_top_ip_route_requests))              # top IP + endpoint
topk(10, max by (ip) (ip_agg_route_top_ip_requests{method="POST", route="/api/login"}))  # who hammers login
sum(ip_agg_unique_ips)
```

Use `max by`, not `sum by`: each IP lives on one pod, but right after a rebalance two pods can report the same IP for
one window.

## Why the per-pod top lists are exact

The repartition step partitions by **IP only** (`IpPartitioner`), so every (ip, method, route) key of an IP is on the
same partition and pod. Each pod's top N over its own IPs is complete for those IPs, so the global top N is always
inside the union of the pods' top Ns, and `topk()` across pods gives the exact answer. The same holds per route and for
pairs. Each view is computed from the full window data, never from another view (an IP spread over 20 routes can top
the IP list without any top pair).

## When a window is emitted

Every 5s the emitter checks whether the next window is closed: every active partition's stream time has passed
`end + grace` (10s), or at the latest `max-wait` (60s) later by the wall clock (idle/lagging partition). It then reads
that window from all local stores (interactive queries), builds the views, and pushes. Normal delay from window end to
Prometheus is ~10–15s. After a restart it starts with the previous window (re-pushing identical values is accepted).

## Deploy (GlueOps cluster)

1. Build and push the image: `docker build -t <registry>/ip-agg-streams:0.1.0 . && docker push <registry>/ip-agg-streams:0.1.0`
2. Start the app (creates `otlp-spans-ip` with 6 partitions on the Tempo broker in an init container):
   `IMAGE=<registry>/ip-agg-streams:0.1.0 ./deploy/run.sh apply`
3. Turn on the span copy in `otel-collector-helm-chart`: `collector.ipAggKafka.enabled: true`
4. Import `deploy/grafana-dashboard.json` into Grafana.
5. Load + check (in `../ip-loadtest`): `IP_MODE=heavy DISTINCT_IPS=1000000 ./run.sh start`, wait 2–3 minutes,
   `./run.sh streams` → expects the 10 heavy IPs as top 10 (in order), as top pairs, and leading their routes.
   `./run.sh watch` shows Prometheus memory staying flat while distinct IPs grow.

`./deploy/run.sh status` shows pods, the last pushed window per pod and the consumer lag; `./deploy/run.sh views` the
last window's views as JSON straight from a pod.

Settings (env, see `deploy/run.sh` and `application.yaml`): `TOP_IPS`, `TOP_PAIRS`, `TOP_IPS_PER_ROUTE`, `MAX_ROUTES`,
`WINDOW_SIZE`, `WINDOW_GRACE`, `REPLICAS`, `STREAM_THREADS`, `PARTITIONS`, `INPUT_FORMAT` (`otlp_proto`/`otlp_json`,
must match the collector's `encoding`; `tempo` for Tempo's own `tempo-traces` topic).

## Run locally against the cluster (nothing deployed)

`deploy/local.sh` runs the app on your machine. It reads Tempo's own `tempo-traces` topic through a port-forward, so the
collector change isn't needed, and remote-writes to both Prometheus replicas through port-forwards. Grafana shows the
data under `instance="local-<you>"`.

```sh
# once: the broker advertises its in-cluster name, point it at the port-forward
echo "127.0.0.1 glueops-core-tempo-kafka.glueops-core-monitoring.svc.cluster.local" | sudo tee -a /etc/hosts

./deploy/local.sh run      # needs kubectl on the GlueOps context, Java 21, Maven; Ctrl-C stops app + port-forwards
./deploy/local.sh check    # 2nd terminal: span counters, consumer lag, last window's top IPs
./deploy/local.sh reset    # afterwards: drop the consumer group (ip-agg-local-<you>), its 2 internal topics, local state
```

- **Tempo is not touched.** The app reads with its own consumer group from the latest offset. Tempo's block-builders
  and live-stores keep their own offsets.
- **`INPUT_FORMAT=tempo`** decodes Tempo 3's record format: `tempopb.PushBytesRequest`, where field 2 holds one marshalled
  `tempopb.Trace` per entry, wire-compatible with OTLP `TracesData` (`grafana/tempo` `pkg/ingest/encoding.go`,
  `modules/distributor`). It is Tempo's internal format, not a public contract. If a Tempo upgrade changes it,
  `ipagg_input_messages_failed_total` rises. Use `otlp-spans-ip` for anything beyond the test.
- **Check the counters first.** `ipagg_input_spans_total{result="counted"}` must grow. If only `skipped_no_ip` grows,
  the spans reaching Tempo carry no `client.address` / `http.client_ip` / `net.peer.ip`.
- **Bandwidth.** All of the cluster's trace volume crosses the port-forward to your machine. That's fine for the load
  test, but not for a busy cluster.
- The scraped health panels of the dashboard stay empty, because there's no ServiceMonitor for a local process. Use
  `check` or `localhost:8080/actuator/prometheus` instead. `../ip-loadtest/run.sh streams` works unchanged, since it
  queries Prometheus.

## Build and test

```sh
mvn test        # parser, normalizers, views, remote-write encoding, staleness, topology (TopologyTestDriver)
mvn package     # target/ip-agg-streams-0.1.0-SNAPSHOT.jar
```

## Limits of the POC

- **Top N per minute only.** The top IPs of the last hour are not the sum of per-minute top lists (an IP ranked 101st
  every minute is missing). Needs a second, 1-hour window in the app if that question matters.
- **Unique IPs per minute** cannot be summed into unique IPs per hour (the same IP is in many minutes).
- **Route** is `http.route`, else a normalized `url.path`/`http.target` (query dropped, numeric/UUID/hex/token segments →
  `{id}`, max 8 segments). Paths that defeat the normalizer still multiply state, not Prometheus series.
- **Record time** is the Kafka record timestamp (when the collector exported the batch), not the span's own time.
- **State size** grows with distinct (IP × route) per window: ~3M keys at 1M IPs × 3 routes. RocksDB memory is capped
  (`ROCKSDB_CACHE_MB`, shared); the per-window scan holds a map of this pod's IPs in heap (~150 B per IP).
- **Shared broker.** By default the topics live on Tempo's single broker (emptyDir). A broker restart loses the
  unconsumed spans, the app's changelog and the input topic. If Kafka Streams then fails, the `kafkaStreams` liveness
  check goes DOWN and the pod restarts: the init container recreates the input topic with its partitions, and Kafka
  Streams recreates its internal topics. If the collector writes first, it auto-creates the topic with the broker's
  default of 1 partition. Use a dedicated, replicated Kafka beyond the POC.
- **At-least-once.** A crash between processing and commit can count a batch twice in one window.
# ip-agg-streams
