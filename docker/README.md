# Docker

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Local development stack: PostgreSQL, Redis and Kafka, plus the API behind the `app` profile. |
| `Dockerfile.backend` | Two-stage image for the Python services. Build context is the repository root. |
| `.dockerignore` (repo root) | Keeps virtualenvs, `node_modules`, and `.env` out of the build context. |

## Usage

```bash
docker compose -f docker/docker-compose.yml up -d          # infrastructure
docker compose -f docker/docker-compose.yml ps             # status and health
docker compose -f docker/docker-compose.yml logs -f        # follow logs
docker compose -f docker/docker-compose.yml down           # stop
docker compose -f docker/docker-compose.yml down -v        # stop and delete data
```

The API and the frontend normally run on the host during development — the reload loop is
faster and the debugger works without extra setup. `--profile app` runs the API in a
container, which is how to check the image itself is correct.

## Notes

* Both services bind to `127.0.0.1` only. They have development credentials, so they must
  not be reachable from the network.
* Redis runs with persistence **off** on purpose. It is a cache and a fan-out bus, never
  the system of record (ADR-0003); starting it this way means that assumption is exercised
  on every restart rather than merely asserted in a document.
* PostgreSQL is initialised with `--locale=C` and `TZ=UTC` so sort order and timestamps do
  not depend on the host's locale.
* Kafka runs as a **single broker in KRaft mode** — no ZooKeeper (ADR-0001). One broker is
  the right size for a laptop and the wrong size for anything else, which is why every
  replication factor is 1 and says so in the file. `acks=all` means less than it sounds like
  when there is one replica.
* Kafka advertises **two listeners**, and this is the setting most likely to waste an
  afternoon. Containers reach the broker as `kafka:19092`; the host reaches it as
  `127.0.0.1:9092`. A single advertised listener cannot be correct for both — a client
  connects, fetches metadata, and then times out talking to an address it cannot route to.
* **Topic auto-creation is off.** A topic created implicitly by whoever connected first gets
  the broker's default of one partition, which is one consumer forever, and infinite
  retention, which contradicts Kafka being a transport rather than the system of record
  (ADR-0002). The workers create the topics deliberately at startup instead.

## Kafka from the command line

```bash
K='docker exec tradewatch-kafka /opt/kafka/bin'

$K/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
$K/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group processor-persist --describe
$K/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic tw.dlq.v1     --from-beginning --property print.headers=true
```

The last one is the reason ADR-0005 chose JSON: reading the dead-letter topic with a
console consumer is a bigger advantage during development than is usually admitted.
