# dev-compose

A three-broker Apache Kafka 4.3.1 cluster in KRaft mode, for local diagnostic
runs of the benchmark harness. It is the cluster the `diagnostic` CI lane
stands up, and the one to point `scripts/bench-producer-*` at while developing.

The topology is deliberate: three brokers with `default.replication.factor=3`
and `min.insync.replicas=2`, so `acks=all` measures real replication rather
than a local write. Auto topic creation is off — the harness owns topic
geometry. The heap is fixed at 1G per broker so the JVM does not resize
mid-run.

This is a development cluster, not a measurement rig. Numbers taken from
containers sharing a laptop with an IDE are diagnostics, never claims.

## Ports

Brokers publish on `127.0.0.1` at:

| Broker | Default host port |
| --- | --- |
| kafka-1 | 39092 |
| kafka-2 | 39093 |
| kafka-3 | 39094 |

So the bootstrap string is
`127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094`.

Override any of them with `KAFKA_1_HOST_PORT`, `KAFKA_2_HOST_PORT`,
`KAFKA_3_HOST_PORT` — for example when another Kafka already owns a port:

```sh
KAFKA_1_HOST_PORT=49092 docker compose -f clusters/dev-compose/compose.yml up -d --wait
```

The same variable feeds both the published port and the advertised listener, so
overriding one is enough. `KAFKA_HEAP_OPTS` overrides the broker heap.

## Use

```sh
docker compose -f clusters/dev-compose/compose.yml up -d --wait
docker compose -f clusters/dev-compose/compose.yml down --volumes --remove-orphans
```

`--wait` blocks until every broker passes its healthcheck, so a cluster that
never comes up fails there instead of surfacing later as an unexplained
producer timeout.

Data lives in container-local storage only; `down --volumes` returns the
cluster to a clean state.

## Boundary

This cluster serves local diagnostics for this repository. The
smoque-driven smoke cluster stays in Kafkars for this loop — the compose
file here is a copy with defaulted host ports, not a replacement for it.
