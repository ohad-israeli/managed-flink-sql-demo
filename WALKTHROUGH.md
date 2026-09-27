# Walkthrough: `managed-flink-sql-demo`

## What this demonstrates

A Flink SQL enrichment job — no custom JAR, just a `CREATE TABLE` and an
`INSERT INTO ... SELECT` — submitted and managed through **CMF (Confluent
Manager for Apache Flink)**, running against a real **Confluent Platform**
backend: Kafka, Schema Registry, and Control Center, all deployed via **CFK
(Confluent for Kubernetes)** on a real Kubernetes cluster (minikube, here, but
the mechanism is identical on any Kubernetes distro). The job joins a stream
of clickstream events against a customer table and writes the enriched result
to its own Kafka topic — the kind of thing that would otherwise mean writing,
packaging, and deploying a Java job by hand.

## Starting the demo

```bash
cd managed-flink-sql-demo
./scripts/demo.sh start
```

This starts minikube (if it isn't already running), waits for every pod in
both the `confluent` (CFK) and `flink-sql-demo` (CMF/Flink) namespaces to
actually be `Ready` — not just scheduled — then opens three port-forwards and
confirms each one is reachable before printing anything. On a cold start this
takes a few minutes (the `confluent` namespace alone took ~185 seconds in
testing); on a warm restart it's much faster since Kubernetes brings the
existing job back on its own.

When it's done, it prints something like:

```
Demo is up.

Now run this on your Mac (separate terminal, leave it open) to view the UIs:
  gcloud compute ssh ohad-demo-1 --zone us-east1-d --tunnel-through-iap -- -L 8080:localhost:8080 -L 8090:localhost:8090 -L 9021:localhost:9021

Then open these in your Mac browser:
  CMF UI:          http://localhost:8080
  Flink job UI:    http://localhost:8090
  Control Center:  http://localhost:9021
```

Run that `gcloud compute ssh ... -L ...` command in its own terminal window
and leave it running — that's what actually makes the three ports reachable
from your Mac's browser, not just from the box itself.

## The full lifecycle: create, seed, enrich

The running enrichment job doesn't appear out of nowhere — it's the last of
six named CMF Statements, submitted in order, each one a durable resource you
can see and inspect on its own:

1. `create-clicks` — `CREATE TABLE` for the clickstream source
2. `create-customers` — `CREATE TABLE` for the customer dimension
3. `create-enrichment-table` — `CREATE TABLE` for the output table
4. `seed-customers` — `INSERT INTO ... VALUES`, a bounded batch job
5. `seed-clicks` — `INSERT INTO ... VALUES`, a bounded batch job
6. `customer-click-enrichment` — the unbounded streaming join, stays `RUNNING`

The SQL for each step lives in `sql/01_create_clicks.sql` through
`sql/06_customer_click_enrichment.sql`. To reproduce the whole thing against a
fresh Kafka catalog (empty topics, no prior state):

```bash
export CONFLUENT_CMF_URL=http://localhost:8080
scripts/run-statements.sh sql-demo sql-demo-pool
```

This submits all six in order and prints each one's result as it goes. Two
real gotchas worth knowing before you run it:

- **The Database's `ddlEnvironments` allowlist has to include your
  environment, or every `CREATE TABLE` fails** with `"This environment does
  not have CREATE TABLE permissions for database <db>."` — this bit us
  directly when an environment got renamed and the Kafka Database resource
  still only allowed the old (now-deleted) environment name. Fix: `PUT` the
  `KafkaDatabase` with the current environment name in `ddlEnvironments`.
- **Re-running the seed steps against topics that already have data
  duplicates rows into an append-only log**, and a regular (non-temporal)
  stream-stream join re-matches everything in retained state against every
  new row — so re-seeding into a live topic doesn't just add a few extra
  rows, it can multiply the join output. If you're re-running this against
  an already-used catalog, delete the `clicks`/`customers`/
  `customer_click_activity` topics (and their Schema Registry subjects)
  first so `CREATE TABLE` starts genuinely fresh.

Once all six have been submitted, the CMF UI's Statements list shows the
complete history — three `COMPLETED` table creations, two `COMPLETED` seed
inserts, and one `RUNNING` streaming job — telling the whole create → seed →
enrich story in one place, not just the final job in isolation.
**[SCREENSHOT HERE — Statements list, all 6 statements: 3x COMPLETED CREATE_TABLE, 2x COMPLETED INSERT_INTO seeds, 1x RUNNING INSERT_INTO enrichment]**

## A guided tour

### 1. CMF UI — `http://localhost:8080`

- Open the **Environments** list. You should see `sql-demo`.
  **[SCREENSHOT HERE — Environments list]**
- Click into it and open **SQL Statements** — this is the same full history
  from the previous section.
- Open the `customer-click-enrichment` statement's detail page. Statements don't have an Events tab
  (that's an Applications-only feature — see the finding below); what they
  have instead is an **Exceptions** view, surfaced via a "See Exceptions"
  button on the results panel.
  **[SCREENSHOT HERE — Statement detail page, Exceptions view]**

This is the fleet-management side of things: what's deployed, what's running,
what its lifecycle looks like. Nothing here shows you the actual data flowing
— that's the next stop.

### 2. Native Flink job dashboard — `http://localhost:8090`

This is the classic Apache Flink Web UI — a completely separate application
from the CMF UI (confirmed directly: it serves the stock Apache-licensed
Flink dashboard HTML, not anything CMF-branded). CMF manages the job's
lifecycle; this shows you its actual execution.

- Find the running job and open its **job graph**. You should see three
  operators: a source reading `clicks`, a join against `customers`, and a
  sink writing `customer_click_activity`.
  **[SCREENSHOT HERE — job graph: source → join → sink]**

### 3. Control Center — `http://localhost:9021`

This is the "real Confluent Platform" side — the same Kafka cluster CMF's
Flink job is reading from and writing to, viewed through Confluent's own
Kafka console, not a Flink-specific tool.

- Navigate to the cluster's **Topics** view.
- Find `clicks`, `customers`, and `customer_click_activity`.
  **[SCREENSHOT HERE — topic list showing all three, with non-zero message counts]**

This is the pairing Ohad specifically wanted: CMF/Flink managing the
compute, Control Center showing the same data from the Kafka side — two
real, separate Confluent Platform consoles agreeing with each other, not
one merged view (there isn't one — Stream Lineage's unified topology graph
is a Confluent Cloud feature, not something CP ships).

## Proving the join is actually correct

Screenshots show state; this shows the actual data. From the box:

```bash
kubectl exec -n confluent kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka.confluent.svc.cluster.local:9092 \
  --topic customer_click_activity --from-beginning --max-messages 4 --timeout-ms 15000
```

Real captured output:

```
c1  Alice Nguyen   alice@example.com  /products/shoes  Mozilla/5.0  42
c2  Ben Ortiz      ben@example.com    /products/hats   Chrome/120   17
c3  Alice Nguyen   alice@example.com  /cart            Mozilla/5.0  95
c4  Carla Diaz     carla@example.com /products/bags    Safari/17    8
```

Every click correctly resolved to its customer — `c1`/`c3` both to Alice
(user 101), `c2` to Ben (102), `c4` to Carla (103) — with no manual topic or
schema setup beyond the two `CREATE TABLE` statements. This is real, direct
proof, not a Console screenshot standing in for one.

## What actually didn't work the first time, and why

Worth calling out honestly, since none of this was obvious going in:

- **`CREATE TABLE ... AS SELECT` isn't supported on Kafka catalogs.** CMF
  rejects it outright: `"CREATE TABLE AS is not supported on Kafka catalog"`.
  The fix is the standard two-step Flink SQL pattern instead: an explicit
  `CREATE TABLE` for the output table, then a separate `INSERT INTO ...
  SELECT` to populate it continuously. Not a bug — just not the shortcut a
  Flink-SQL-from-elsewhere user might reasonably expect to work.

- **CMF's own statement-status reporting lags behind reality.** More than
  once, `confluent flink statement describe` reported a job as `PENDING` /
  `"Job Not Found"` while the job was, verifiably, already `RUNNING` and
  producing correct output in Kafka. Don't trust the CLI's status alone if
  something seems stuck — check the actual topic.

- **CP 8.x quietly retired the classic Control Center image.**
  `confluentinc/cp-enterprise-control-center` tops out at `7.9.10` on Docker
  Hub — there is no `8.3.2` tag. CP 8.x ships `cp-enterprise-control-center-
  next-gen` instead. An official-looking reference manifest that still
  points at the classic image name will pull-fail on a current CP line.

- **Flink's transactional Kafka sink needs `transaction.max.timeout.ms`
  raised.** By default, the Kafka broker rejects the transaction timeout
  Flink's exactly-once sink requests (`"The transaction timeout is larger
  than the maximum value allowed by the broker"`). Setting
  `transaction.max.timeout.ms=3600000` in the Kafka CR's `configOverrides`
  fixes it — worth setting proactively on any single-broker demo Kafka a
  Flink job will write to transactionally.

- **Statements and Applications aren't the same resource type under the
  hood, and the CMF UI reflects that.** Applications get a full Events Log
  tab (`Flink.ApplicationDetails.Events.*` in the UI, backed by a real
  `/applications/{name}/events`-style view). Statements don't have an Events
  tab at all — instead they expose an **Exceptions** view (a "See Exceptions"
  button on the results panel, backed by a working
  `GET /statements/{name}/exceptions` endpoint that returns real
  `StatementException` entries). Confirmed directly against the CMF REST API
  and the UI's own frontend bundle, not inferred from the UI alone. Small
  distinction, but a real one if you're used to thinking of "Statement" and
  "Application" as interchangeable Flink-job wrappers in CMF.

- **A KafkaDatabase's `ddlEnvironments` allowlist doesn't update itself when
  an environment is renamed/recreated.** After recreating `sql-demo` (see
  above), `CREATE TABLE` against the existing `fresh-catalog`.`fresh-db`
  failed with `"This environment does not have CREATE TABLE permissions for
  database fresh-db"` — the Database resource's `ddlEnvironments` field still
  only listed the old, already-deleted `sql-demo-fresh`. A Kafka Catalog and
  its Databases are global/shared objects (confirmed earlier), but DDL
  *permission* on a Database is scoped per-environment via this explicit
  allowlist, and it's on you to update it (`PUT` the `KafkaDatabase` with the
  new environment name) whenever the owning environment changes.

- **The real "what looked like X was actually Y" story**: a `CREATE TABLE`
  against the Kafka catalog started failing consistently with `"This server
  does not host this topic-partition"` — the kind of error that looks like a
  broker-level partition-assignment bug. A long diagnostic pass ruled out
  connectivity, DNS, listener configuration, and CPU contention one at a
  time, all clean. The actual cause: the CMF **environment/catalog objects
  themselves** had accumulated inconsistent state from being repeatedly
  re-pointed at different backing Kafka clusters over the course of
  debugging. Creating a **completely fresh** environment/catalog/compute-pool
  (new names, nothing reused) fixed it immediately — first `CREATE TABLE`
  attempt succeeded cleanly. The lesson: when a CMF Kafka Catalog has been
  re-registered against more than one backend, don't keep debugging the
  Kafka side — start over with fresh CMF-side objects before assuming
  anything about the cluster itself.

## Stopping the demo

```bash
./scripts/demo.sh stop
```

Kills the three port-forwards and runs `minikube stop` (pauses the cluster,
doesn't delete it) — everything's still there, just not consuming CPU/memory
on the box, ready to pick back up with `./scripts/demo.sh start` next time.
