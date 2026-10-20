# Cart recovery

Run three Go applications against local Kafka and PostgreSQL. Publish cart events over HTTP, advance the demo clock, and inspect the results in PostgreSQL. Workers poll automatically. A reminder reaching `TRIGGERED` means its personalized message was recorded; nothing is emailed or sent.

The five automated cases remain as the fake-clock verifier required by the assignment. They are separate from the application demo below.

## 1. Install and start Kafka and PostgreSQL

You need Docker Desktop with its engine running, and Go 1.24 or newer. Kafka and PostgreSQL run in containers; you do not need separate native installations.

Check your installations:

```sh
docker --version
docker compose version
go version
```

From the project directory, start the infrastructure yourself:

```sh
cd /Users/swapnilmehta/projects/cart-recovery
docker compose up -d --wait postgres kafka
docker compose run --rm kafka-init
docker compose ps
```

The first command downloads the images when needed. The second creates the `cart-events` topic with three partitions. Both commands can be repeated. PostgreSQL's schema is initialized from `commons/schema.sql`; the consumer also applies this idempotent schema at startup.

Run every `docker compose` command from this project directory, including commands in new terminals. If you see `no configuration file provided: not found`, run `cd /Users/swapnilmehta/projects/cart-recovery` first. Alternatively, supply `-f /Users/swapnilmehta/projects/cart-recovery/compose.yaml` immediately after `docker compose`.

| Component | Local address | Credentials |
| --- | --- | --- |
| PostgreSQL | `localhost:15432` | Database `cart_recovery`, user `cart`, password `cart` |
| Kafka | `localhost:19092` | No authentication for this local demo |

## 2. Connect to PostgreSQL

**Simplest option: use the client already inside the container.** Open a terminal in the project directory:

```sh
docker compose exec postgres psql -U cart -d cart_recovery
```

At the `cart_recovery=#` prompt, run:

```sql
\conninfo
\dt
SELECT cart_id, last_revision, status, last_edit_at, abandon_at
FROM cart_recovery ORDER BY cart_id;

SELECT cart_id, step, status, due_at, retry_count, lease_until,
       rendered_payload, triggered_at
FROM cart_reminders ORDER BY cart_id, due_at;
```

Keep this terminal open during the demo. Run a query again to see updates. Put `\watch 2` after a query to refresh it every two seconds; Ctrl+C stops refreshing. `\q` exits `psql`.

**If `psql` is installed on your laptop**, connect directly instead:

```sh
psql 'postgres://cart:cart@localhost:15432/cart_recovery?sslmode=disable'
```

**For a GUI**, create a PostgreSQL connection in your database client using:

```text
Host:     localhost
Port:     15432
Database: cart_recovery
User:     cart
Password: cart
SSL:      disabled
Schema:   public
```

These are the same credentials used by the applications. Application rows stay in `public` after a process stops, so you can inspect an unfinished lease and restart a worker against it.

## 3. Start the three applications

Open three terminals. Run `cd /Users/swapnilmehta/projects/cart-recovery` in each one. Start the consumer first and leave it running throughout the demo.

**Terminal 1 — consumer, port 8081:**

```sh
go run ./consumer/cmd/consumer -demo
```

**Terminal 2 — abandonment worker, port 8082:**

```sh
go run ./abandonment/cmd/abandonment \
  -clock-url http://localhost:8081/demo/clock -poll 1s
```

**Terminal 3 — reminder worker, port 8083:**

```sh
go run ./reminders/cmd/reminders \
  -clock-url http://localhost:8081/demo/clock -poll 1s
```

Check them from a fourth terminal:

```sh
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8082/health
curl -fsS http://localhost:8083/health
curl -fsS http://localhost:8081/demo/clock
```

Ports 8082 and 8083 expose health endpoints. Their background loops do the work; no HTTP call is needed to detect abandonment or fire reminders. Each process has its own PostgreSQL connection pool, capped at 12 connections.

```text
POST /demo/events → Kafka cart-events → consumer → PostgreSQL
                                                     ↑
                            abandonment worker ──────┤
                            reminder worker ─────────┘
```

The consumer's demo API stands in for the existing cart service's publisher. Every event goes through Kafka. The API waits until its revision appears in PostgreSQL before responding, so you can safely advance time afterward.

### How the shared fake clock works

With `-demo`, the consumer creates a `FakeClock` in Go memory, initially set to its startup time in UTC. Time stays frozen until you advance it. Both workers use `HTTPClock` because their startup commands include `-clock-url http://localhost:8081/demo/clock`.

```text
POST /demo/clock/advance → consumer's in-memory FakeClock
                                      ↑
                  GET /demo/clock ─────┤ abandonment worker
                  GET /demo/clock ─────┘ reminder worker
```

Whenever a worker needs the current time, it requests `GET /demo/clock`. The consumer returns JSON such as `{"now":"2026-10-20T12:30:00Z"}`. The worker uses that value in its PostgreSQL queries for inactivity, reminder deadlines and lease expiry. Events published through `/demo/events` are timestamped using the same fake clock.

Read the current time or advance it:

```sh
curl -fsS http://localhost:8081/demo/clock

curl -fsS -X POST http://localhost:8081/demo/clock/advance \
  -H 'Content-Type: application/json' -d '{"by":"30m"}'
```

The advance endpoint only changes the clock; it does not invoke either worker. Polling still uses real timers: `-poll 1s` means a poll every real second. On the next poll, workers fetch the advanced time and process eligible rows. Waiting a real minute without advancing this clock does not make deadlines or leases expire.

**Keep the consumer running when restarting a worker.** Restarting the consumer creates a fresh clock at its new startup time; it does not restore the previously advanced time. Clock advances affect every demo cart. PostgreSQL stores ordinary deadlines and leases; its system clock is unchanged and there is no clock table. If the clock endpoint is unavailable, a worker exits with an error; restart it after the consumer is available. Workers without `-clock-url` use real time, so keep that flag on both workers during this demo.

## 4. Try the five cases

Run these commands in the fourth terminal. **After each clock advance, wait for the expected state before advancing again.** With `-poll 1s`, this normally takes a couple of seconds. Inspect the cart using its HTTP endpoint or the SQL queries above. HTTP JSON can be formatted by appending `| python3 -m json.tool`.

Run each block separately and check its expected result before moving on. Repeat a cart's `GET` request if a worker has not polled yet. Reminder status lists below are ordered by step. The examples use the default inactivity of 30 minutes and offsets of 30 minutes, 1 hour and 24 hours **from abandonment**.

Use new cart IDs when repeating a case, because previous cart revisions and reminders are intentionally retained. Finish or cancel the reminders in each case before starting the next. Jumping beyond a reminder's due time by 15 minutes or more expires it; advance through the checkpoints below to observe each trigger.

### Case 1: abandonment and all three reminders

```sh
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"timing-1","revision":1,"kind":"CART_UPDATED"}'

curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"30m"}'
curl -fsS localhost:8081/carts/timing-1
```

Wait for `ABANDONED` and three `PENDING` reminders. Their deadlines are abandonment +30 minutes, +1 hour and +24 hours.

```sh
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"30m"}'
curl -fsS localhost:8081/carts/timing-1
```

Wait for `TRIGGERED, PENDING, PENDING`. Terminal 3 prints **DEMO — would send**, followed by the subject and body with the greeting and cart contents. The same message is stored in PostgreSQL's `rendered_payload` column.

```sh
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"30m"}'
curl -fsS localhost:8081/carts/timing-1
```

Wait for `TRIGGERED, TRIGGERED, PENDING`, then run:

```sh
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"23h"}'
curl -fsS localhost:8081/carts/timing-1
```

Wait for all three to become `TRIGGERED`. This last jump reaches abandonment +24 hours exactly. All work for this cart is now finished.

### Case 2: an edit resets inactivity

```sh
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"edit-1","revision":1,"kind":"CART_UPDATED"}'
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"20m"}'
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"edit-1","revision":2,"kind":"CART_UPDATED"}'
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"10m"}'
curl -fsS localhost:8081/carts/edit-1
```

The cart must still be `ACTIVE`, with no reminders (`reminders` can be `null` or an empty array). Revision 2 moved the inactivity deadline forward by 20 minutes.

```sh
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"20m"}'
curl -fsS localhost:8081/carts/edit-1
```

Wait for `ABANDONED` and three `PENDING` reminders. The abandonment time is 30 minutes after revision 2. Finish this case:

```sh
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"edit-1","revision":3,"kind":"PURCHASE_COMPLETED"}'
curl -fsS localhost:8081/carts/edit-1
```

Expect `COMPLETED` and three `CANCELLED` reminders.

### Case 3: checkout cancels reminders

```sh
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"checkout-1","revision":1,"kind":"CART_UPDATED"}'
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"30m"}'
curl -fsS localhost:8081/carts/checkout-1
```

Wait for `ABANDONED` and three `PENDING` reminders. Then publish:

```sh
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"checkout-1","revision":2,"kind":"CHECKOUT_STARTED"}'
curl -fsS localhost:8081/carts/checkout-1
```

Expect `RESUMED` and all reminders `CANCELLED`. `PURCHASE_COMPLETED`, `CART_CLEARED` and `CART_DELETED` also cancel reminders, but mark the cart `COMPLETED`.

To inspect **mid-flight cancellation**, use the pause flag from Case 5, wait until the first reminder is `PROCESSING`, then publish checkout. It must become `CANCELLED`; restarting the reminder worker must not trigger it.

### Case 4: duplicate events

```sh
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"duplicate-1","revision":1,"kind":"CART_UPDATED"}'
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"duplicate-1","revision":1,"kind":"CART_UPDATED"}'
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"duplicate-1","revision":2,"kind":"CART_OWNER_CHANGED"}'
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"30m"}'
curl -fsS localhost:8081/carts/duplicate-1
```

The same revision was published twice. The ownership event at revision 2 lets us confirm both earlier events have been consumed from that cart's Kafka partition; ownership changes do not reset inactivity. Wait for `ABANDONED` with exactly three reminders.

```sh
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"30m"}'
curl -fsS localhost:8081/carts/duplicate-1
```

Wait for `TRIGGERED, PENDING, PENDING`. Read the cart again after several real polling intervals. There must still be exactly three rows, with one recorded trigger for step 1. Cancel the other two before the next case:

```sh
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"duplicate-1","revision":3,"kind":"CHECKOUT_STARTED"}'
curl -fsS localhost:8081/carts/duplicate-1
```

Expect `RESUMED` and `TRIGGERED, CANCELLED, CANCELLED`.

### Case 5: stop a worker after it claims work

First, stop **only Terminal 3** with Ctrl+C. Restart that worker with a deliberate pause after claiming:

```sh
go run ./reminders/cmd/reminders \
  -clock-url http://localhost:8081/demo/clock -poll 1s -pause-after-claim 1h
```

```sh
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"crash-1","revision":1,"kind":"CART_UPDATED"}'
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"1h"}'
curl -fsS localhost:8081/carts/crash-1
```

At one hour after the edit, abandonment is due and the first reminder is due too. The abandonment worker schedules the reminders; the reminder worker claims the first one. Wait for Terminal 3's `demo pause after claim` log and read the cart again. The first reminder should be `PROCESSING`, with `lease_until` 30 fake seconds ahead and `retry_count = 0`. The pause uses real time; the fake clock remains frozen.

Stop Terminal 3 with Ctrl+C during the pause. That process exits without completing the claim, leaving the same persisted state as a crash after claim commit. Restart it **without** the pause flag:

```sh
go run ./reminders/cmd/reminders \
  -clock-url http://localhost:8081/demo/clock -poll 1s
```

Read `/carts/crash-1` again. Even after several real polls, the first reminder stays `PROCESSING`, because the fake clock is still before lease expiry. Then advance it:

```sh
curl -fsS -X POST localhost:8081/demo/clock/advance -H 'Content-Type: application/json' \
  -d '{"by":"31s"}'
curl -fsS localhost:8081/carts/crash-1
```

Wait for `TRIGGERED` and `retry_count = 1`. The replacement process reclaimed the expired lease. The remaining reminders stay `PENDING`.

Finish by publishing `PURCHASE_COMPLETED`, revision 2, for `crash-1`:

```sh
curl -fsS -X POST localhost:8081/demo/events -H 'Content-Type: application/json' \
  -d '{"cart_id":"crash-1","revision":2,"kind":"PURCHASE_COMPLETED"}'
curl -fsS localhost:8081/carts/crash-1
```

Expect `COMPLETED` and `TRIGGERED, CANCELLED, CANCELLED`. The pause flag is only a demo aid and defaults to zero.

## 5. Inspect Kafka messages

Run this in another terminal **from the project directory**; Ctrl+C stops reading:

```sh
cd /Users/swapnilmehta/projects/cart-recovery
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic cart-events --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true
```

Events are keyed by cart ID, keeping one cart's events in the same partition. To inspect the consumer group's committed offsets and lag:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group cart-recovery
```

## 6. Integration tests: automated fake-clock verifier

**Only Kafka and PostgreSQL need to be running.** You do not need to start the three applications for these tests. Each test starts the consumer and workers inside the Go test process, using the same packages as the applications. Tests do not start or stop Docker.

Run all five cases:

```sh
cd /Users/swapnilmehta/projects/cart-recovery
go test -v -count=1 ./integration
```

`-v` prints events, clock times, statuses and PASS/FAIL results. `-count=1` runs the tests again rather than returning cached results.

| Test | What it checks |
| --- | --- |
| `TestRemindersFireAtConfiguredTimes` | Abandonment at edit +30 minutes; all three reminder deadlines measured from abandonment; each reminder triggers at its due time. |
| `TestCartEditResetsInactivity` | An edit at +20 minutes leaves the cart active at the original deadline and moves abandonment and reminders forward. |
| `TestCheckoutCancelsClaimedReminder` | Checkout cancels a reminder already in `PROCESSING`; completing the old claim fails, and later polls do not trigger cancelled reminders. |
| `TestDuplicateEventsDoNotRepeatReminders` | Publishing the same cart revision twice still creates exactly three reminders and records the first trigger once. |
| `TestReplacementWorkerRecoversExpiredClaim` | An unfinished claim stays leased before expiry. A replacement worker with a new database pool reclaims it after expiry, increments `retry_count` to 1, and rejects completion by the old claim. |

Run one case, for example lease recovery:

```sh
go test -v -count=1 ./integration -run '^TestReplacementWorkerRecoversExpiredClaim$'
```

**Clock behavior in integration tests:** each test creates a `FakeClock` at `2026-10-20T12:00:00Z` and passes that same Go object directly to both workers. There are no HTTP clock calls or application ports. `demo.emit(...)` publishes through real Kafka and waits for the cart revision to reach PostgreSQL. `demo.clock.Advance(...)` moves the shared time instantly. Workers continue polling on real 50 ms timers. `demo.expect(...)` waits for the expected PostgreSQL rows; it never calls a worker to force detection or triggering.

The cancellation and recovery tests deliberately leave a PostgreSQL claim unfinished to exercise the in-flight case. The recovery test then starts a replacement worker with a separate connection pool. It models a lost worker in process; Case 5 above demonstrates stopping and restarting an actual application process.

**Data isolation:** each test creates its own Kafka topic and PostgreSQL schema, whose names are printed in the output. Its schema is removed afterward; Kafka topics remain available for debugging. The application's `public` tables and demo clock are unaffected, so applications may remain running while you run the verifier. To inspect test Kafka messages, use its printed topic name instead of `cart-events`. To inspect test database rows, pause the test in a debugger before cleanup and query the printed schema.

Defaults are `localhost:19092` for Kafka and `postgres://cart:cart@localhost:15432/cart_recovery?sslmode=disable` for PostgreSQL. Set `KAFKA_BROKERS` or `DATABASE_URL` in the test terminal to override them. The database user must be able to create and drop test schemas.

To change a case, edit [integration/pipeline_test.go](integration/pipeline_test.go), then rerun its test command. For example, change `commons.CheckoutStarted` to `commons.PurchaseComplete` or `commons.CartCleared` in the cancellation test to check those events. Change `demo.clock.Advance(...)` durations and expected deadlines together when experimenting with timing. Connection and worker setup is confined to `integration/demo_setup_test.go`.

## Configuration and code

| Module | Responsibility |
| --- | --- |
| `consumer` | Batch Kafka events, apply cart revisions, cancel obsolete reminders, then commit Kafka offsets. Its executable is under `cmd/consumer`. |
| `abandonment` | Poll active carts and atomically create their reminder schedules. Executable: `cmd/abandonment`. |
| `reminders` | Claim eligible reminders, render templates and record triggers. Executable: `cmd/reminders`. |
| `commons` | Shared types, PostgreSQL operations and clocks. |

Set `DATABASE_URL` to override PostgreSQL. The consumer also accepts `KAFKA_BROKERS`, `KAFKA_TOPIC` and `KAFKA_GROUP`. Each application supports `-addr` to change its port. The consumer supports `-inactivity 30m -offsets 30m,1h,24h`. Restart it to change that policy; new edits use the new settings.

Templates are in `reminders/templates.json` and are embedded at build time. Change the greeting or placeholders there and restart the reminder application. Event requests can include a full `snapshot` object with `recipient` and `cart` fields to customize the shopper and items; see `commons/model.go`. When omitted, the demo uses Sam and two travel mugs, or preserves that cart's existing snapshot.

For ordinary wall-clock operation, omit `-demo` on the consumer and `-clock-url` on both workers. The demo event/clock endpoints are then disabled; the existing cart service must publish events to Kafka. Worker polling defaults to five seconds unless overridden. The existing cart service owns guest-cart merging; recovery consumes its delete/update events.

## Stop

Stop each Go application with Ctrl+C, then stop the infrastructure yourself:

```sh
docker compose down
```

Volumes and application rows are retained. These examples demonstrate behavior; they do not establish capacity at 5,000 events/second or delivery guarantees for a real notification provider.
