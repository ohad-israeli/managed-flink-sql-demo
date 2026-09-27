# managed-flink-sql-demo

A Flink SQL enrichment job — no custom JAR, no hand-operated cluster — submitted and
managed entirely through **CMF (Confluent Manager for Apache Flink)**, running against a
real **Confluent Platform** backend (Kafka, Schema Registry, Control Center) deployed via
**CFK (Confluent for Kubernetes)**. The job joins a stream of clickstream events against a
customer table and writes the enriched result to its own Kafka topic, entirely through SQL
and a managed console.

> 📝 Write-up: **https://ohad-israeli.github.io/projects/managed-flink-sql-demo**

## What this repo demonstrates

- **CMF**, the **Flink Kubernetes Operator**, and the enrichment SQL job running in one
  Kubernetes namespace (`flink-sql-demo`).
- A real **Confluent Platform** deployment — Kafka, Schema Registry, Control Center — in a
  separate namespace (`confluent`), via CFK custom resources.
- Both namespaces on a single **minikube** cluster (the mechanism is identical on any
  Kubernetes distro).
- Three UI surfaces (CMF, the native Flink job dashboard, Control Center) reachable via SSH
  tunneling, each showing the same job/data from a different angle.

## Why this matters

Confluent's managed-Flink pitch centers on removing operational burden: "one-click
integration between Kafka and Flink with schemas managed automatically," a SQL-first
developer experience (ANSI SQL, Python, Java, a browser workspace, the CLI, VS Code),
security shared with the rest of Confluent Platform, one control plane across clusters and
regions, and enterprise support included. This repo puts that pitch to a direct test: does
submitting a real join, with a real backend, actually stay pure SQL — no JAR, no
Dockerfile, no hand-rolled JobManager call? It does, with a few real gotchas along the way
(below).

## The key idea: CMF Statement, not job

CMF treats everything as a **Statement** resource — a one-shot `CREATE TABLE` and a
long-running streaming join are the same resource type, just with different field values:

- **Kind** — `INSERT_INTO` marks continuous operations identically to batch ones; what
  differs is —
- **Bounded** — `false` for the unbounded, continuously-running enrichment join; `true`
  for the table DDL and the seed inserts.
- **Append only** — `true` shows the sink never retracts a row.
- **Parallelism & resources** — configured as compute-pool settings, not code changes.

The SQL text itself appears directly in the console. Nothing about submitting it needed a
JAR, a Dockerfile, or a JobManager REST call.

## Try it yourself

```bash
git clone https://github.com/ohad-israeli/managed-flink-sql-demo
cd managed-flink-sql-demo

# bring up minikube + CFK (Confluent Platform) + CMF/Flink, open the 3 UI port-forwards
./scripts/demo.sh start

# submit the full lifecycle as named CMF Statements: create tables, seed data, run the join
export CONFLUENT_CMF_URL=http://localhost:8080
./scripts/run-statements.sh <environment> <compute-pool>

# tear down the port-forwards and pause minikube (state is preserved) when you're done
./scripts/demo.sh stop
```

- `k8s/confluent-platform.yaml` — the CFK custom resources for Kafka, KRaftController,
  Schema Registry, and Control Center.
- `sql/01_create_clicks.sql` through `sql/06_customer_click_enrichment.sql` — the six
  statements in order: two `CREATE TABLE`s for the sources, one for the output table, two
  seed `INSERT INTO ... VALUES`, and the streaming enrichment join.
- `scripts/demo.sh` — starts/stops the whole stack (minikube, both namespaces, all three
  port-forwards) on demand.
- `scripts/run-statements.sh` — submits all six SQL files in order as named CMF Statements.
- **[`WALKTHROUGH.md`](WALKTHROUGH.md)** — a full narrated tour with the exact commands,
  real captured output, and screenshot-cued steps through all three UIs.

## Real findings

- **`CREATE TABLE ... AS SELECT` isn't supported on Kafka catalogs.** CMF rejects it
  outright: `"CREATE TABLE AS is not supported on Kafka catalog"`. The fix is the standard
  two-step Flink SQL pattern: an explicit `CREATE TABLE` for the output table, then a
  separate `INSERT INTO ... SELECT` to populate it continuously.

- **No built-in demo catalog on self-managed CMF.** Confluent Cloud's
  `examples.marketplace` catalog doesn't exist here — every catalog has to be a real
  **Kafka Catalog**, explicitly registered against an actual Kafka cluster and Schema
  Registry.

- **A persistent, misleading "This server does not host this topic-partition" error** —
  the kind that looks like a broker-level partition-assignment bug. A long diagnostic pass
  ruled out connectivity, DNS, listener configuration, and CPU contention, all clean. The
  actual cause: a CMF Statement's lifecycle record is scoped to its owning Environment, but
  the CMF **environment/catalog objects themselves** had accumulated inconsistent state
  from being repeatedly re-registered against different backing Kafka clusters over the
  course of debugging. A completely fresh environment/catalog/compute-pool (new names,
  nothing reused) fixed it immediately.

- **A `KafkaDatabase`'s `ddlEnvironments` allowlist is a separate permission gate from
  catalog registration.** Registering a catalog and being allowed to run DDL against it are
  two different steps — `CREATE TABLE` failed with `"This environment does not have CREATE
  TABLE permissions for database ..."` even though the environment could already query the
  catalog, because `ddlEnvironments` still listed an old, deleted environment name. It
  doesn't update itself when an environment is renamed or recreated — that's on you.

- **Statements and Applications aren't the same resource type, and the CMF UI reflects
  that.** Applications get a full Events Log tab; Statements don't have one at all —
  instead they expose an **Exceptions** view (`GET /statements/{name}/exceptions`, backed
  by real `StatementException` records). Not a missing feature — a deliberate distinction
  between the two resource types.

- **CP 8.x quietly retired the classic Control Center image.**
  `confluentinc/cp-enterprise-control-center` tops out at `7.9.10` on Docker Hub — CP 8.x
  ships `cp-enterprise-control-center-next-gen` instead. An official-looking reference
  manifest that still points at the classic image name will pull-fail on a current CP line.

- **Flink's transactional Kafka sink needs `transaction.max.timeout.ms` raised** above the
  Kafka broker's default, or the sink fails with `"The transaction timeout is larger than
  the maximum value allowed by the broker."` A gap even Confluent's own official
  single-node reference manifest doesn't cover.

## Cost

This runs entirely on an existing GCP box via minikube — there's no Confluent Cloud
cluster, no cloud Flink compute pool, and no other new billed infrastructure involved.
Unlike the other demos in this workspace, there's nothing here that needs tearing down to
stop a meter running.

## Limitations

- Single-broker Kafka, single-node CFK deployment — this proves the mechanism, not
  production sizing or HA.
- `minikube` on a single VM, not a real multi-node Kubernetes cluster — CFK and CMF deploy
  the same way on a real cluster, but this hasn't been tested there.
- The enrichment join is a plain (non-temporal) stream-stream join over two small,
  hand-seeded tables — enough to prove the join executes correctly, not a realistic data
  volume or join shape.

## License

MIT — see [LICENSE](LICENSE).
