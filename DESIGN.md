# Order & Inventory Sync Service — Design

## 1. Architecture

NestJS API endpoints and workers accept events, PostgreSQL is the source of truth for orders and inventory, Redis supports caching/rate-limit coordination, and AWS SQS buffers asynchronous work. Intake, stock publishing, and ERP delivery use separate queues so a channel or ERP outage does not block other channels.

See [architecture.mmd](./architecture.mmd) for the diagram.

- **NestJS channel adapters:** receive website, Nykaa and Blinkit webhooks; normalize events. A scheduled poller fetches Flipkart orders every 2 minutes.
- **SQS:** durable queues for order processing, stock publishing and ERP delivery, each with a dead-letter queue (DLQ).
- **PostgreSQL:** orders, order items, inventory, reservations, inbound-event deduplication, transactional outbox and audit trail.
- **Workers:** atomically reserve stock, publish the latest stock to each channel, and deliver confirmed orders to Business Central.
- **Redis:** short-lived cache and distributed rate-limit coordination only; never the authority for stock availability.
- **Ops screen:** failed syncs, attempts, last error, next retry and authorized manual retry.
- **Observability:** structured logs, metrics and correlation IDs across events.

## 2. Data model and duplicate protection

| Table | Key columns and constraints |
|---|---|
| `orders` | `id` PK, `channel`, `external_order_id`, `status`, `event_time`, `total_amount`; UNIQUE (`channel`, `external_order_id`) |
| `order_items` | `order_id` FK, `sku`, `qty`, `unit_price`; PK on order and line/SKU |
| `inventory` | `sku` PK, `on_hand_qty`, `reserved_qty`; CHECK quantities are non-negative and reserved <= on-hand |
| `stock_reservations` | `order_id`, `sku`, `qty`, `status`; unique active reservation per order/SKU |
| `channel_events` | channel, external event ID, external order ID, event type, payload hash, received time, processing status; unique channel/event ID when available |
| `outbox` | message type, aggregate/order ID, payload, UNIQUE `idempotency_key`, status, attempts, next retry, last error |
| `stock_sync_jobs` | channel, SKU, desired quantity, status, attempts, next retry, last error; coalesce/update to latest desired state |
| `inventory_audit` | SKU, stock/reserved deltas, reason, order ID, channel/actor, correlation ID, timestamp |

Persist each inbound event and use database unique constraints—not an in-memory check—to prevent duplicate processing. If an event ID is absent, use channel + external order ID + event type + payload hash as a fallback. Apply explicit state transitions and source version/time checks so an older event cannot blindly overwrite a newer state.

## 3. Order ingestion

- **Webhooks:** validate authentication and schema, durably persist/enqueue, then acknowledge. Return a retryable error if durable acceptance fails.
- **Flipkart polling:** poll every 2 minutes with a persisted cursor and small overlap window; deduplicate in PostgreSQL. Advance the cursor only after events are durable.
- **Normalization:** map each channel's payload to a common event/order model while preserving original IDs and payload for debugging.
- **Duplicates/out-of-order:** make processing idempotent. If cancellation arrives before creation, retain it as an event/tombstone and reconcile when creation arrives. Use source versions/timestamps where reliable.
- **Channel down:** retry queued work; reconcile missed webhooks through provider APIs where supported. Such reconciliation capability is an assumption where not specified in the brief.

## 4. Inventory consistency: two channels sell the last unit

PostgreSQL is authoritative; cached or channel-displayed stock never approves an order. In one transaction, lock the SKU row (`SELECT ... FOR UPDATE`), compute `available = on_hand_qty - reserved_qty`, and reserve only if enough is available. Write reservation, order state, audit row and stock-change outbox event in the same transaction.

```text
BEGIN;
SELECT on_hand_qty, reserved_qty
  FROM inventory WHERE sku = :sku FOR UPDATE;
available = on_hand_qty - reserved_qty;
IF available >= :qty THEN
  UPDATE inventory SET reserved_qty = reserved_qty + :qty
    WHERE sku = :sku;
  INSERT stock_reservations (..., status='RESERVED');
  INSERT inventory_audit (..., reason='ORDER_RESERVED');
  INSERT outbox (..., message_type='STOCK_CHANGED', idempotency_key=...);
  COMMIT; return RESERVED;
ELSE
  ROLLBACK; return INSUFFICIENT_STOCK;
END IF;
```

If website and Nykaa race for the final unit, PostgreSQL serializes row-lock transactions: the first reserves it; the second sees zero available and cannot reserve. For multi-SKU orders, lock SKUs in sorted order to reduce deadlocks and safely retry deadlock errors.

A cancellation before dispatch releases reserved stock once. For returns, put stock back on sale only after the item is received/approved. Every change records a reason and timestamp in the audit table.

## 5. Stock publishing within 60 seconds

The transaction writes an outbox event; a dispatcher creates stock-sync jobs. Coalesce changes so each channel/SKU publishes the latest desired quantity. Target p95 under 60 seconds in normal conditions; provider outages can make this objective temporarily impossible and must trigger alerts.

- **Website:** update internal API/database.
- **Nykaa:** one SKU per call; throttle below 5 requests/second.
- **Flipkart:** batches up to 50 SKUs; throttle below 10 requests/second; back off with jitter on HTTP 429.
- **Blinkit:** urgent REST API for urgent changes; retry timeouts with bounded exponential backoff and jitter, then reconcile. Generate daily CSV through a scheduled export.
- **Stale jobs:** attach a version/time to each desired state, discard superseded jobs, and serialize sends per channel/SKU if the provider cannot enforce versions.

At 300 orders/minute (5/second), SQS buffers bursts and workers scale from queue age/depth without manual scale-up. Prioritize latest stock state and alert on backlog.

## 6. ERP, retries and poison messages

Write an ERP-delivery outbox record in the same transaction as confirmed order/reservation state. A worker sends the order to Business Central using a stable idempotency key (internal order ID) and records the ERP reference. During Sunday maintenance, events remain durable; order intake and stock processing continue. On recovery, drain the backlog with concurrency limits and alert on oldest-message age.

Retry transient failures with exponential backoff and jitter. After bounded attempts, move messages to a DLQ and show them in the ops screen with error and retry action. Manual retries reuse the same idempotency key. Blinkit timeouts have an unknown outcome, so retry safely and reconcile if supported.

**Exactly-once caveat:** sender-side retries alone cannot guarantee exactly-once effects if the ERP commits an order but its response times out. Business Central must support an idempotency key or a lookup/reconciliation mechanism by external order ID. This is an assumption to validate.

## 7. Day-one metrics and alerts

1. Reservation failures and oversell/cancel-for-stock rate; stock constraint violations should be zero.
2. Percentage of stock changes published within 60 seconds and oldest pending job, by channel.
3. SQS queue depth, oldest message age and DLQ count.
4. ERP delivery success/failure rate, retries and oldest pending ERP order.
5. Channel webhook/poll failures, HTTP 429s, Blinkit timeouts and API latency.

Use correlation IDs and searchable audit history to answer what changed, when, by how much and why.

## 8. Scope, assumptions and trade-offs

**V1 scope:** one warehouse; no forecasting, multi-warehouse allocation, customer-facing order management, automatic refund/payment workflows or LLM ops assistant. Prioritize reliable intake, atomic reservations, stock sync, ERP delivery, audit and manual recovery for a three-developer/six-week team.

**Assumptions**
- Each channel provides a stable external order ID; event IDs/versions may be absent.
- Webhook authentication and credentials can be configured securely; the brief does not specify the method.
- ERP supports idempotency or lookup/reconciliation; otherwise exactly-once effects cannot be guaranteed after ambiguous timeouts.
- Available stock = `on_hand_qty - reserved_qty`; initial stock is reconciled with warehouse counts.
- Cancelled orders release stock once; returned items become sellable only after receipt/inspection.
- The 60-second objective is for normal operation, not provider outages.
- Blinkit's daily CSV delivery/confirmation mechanism is not specified and needs an operational procedure.

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| Stock authority | PostgreSQL transaction + row lock | Redis-only counter | Durable atomic reservation and audit |
| Async work | SQS queues + DLQs | Synchronous channel/ERP calls in webhook | Buffers spikes and isolates failures |
| Event delivery | Transactional outbox + idempotent consumers | Publish only after DB commit | Avoids lost events if process crashes |
| Stock updates | Coalesce latest SKU/channel state | Send every intermediate change | Respects limits and avoids stale writes |

**AI usage note:** ChatGPT helped structure and review this design. I reviewed the assumptions and will be prepared to explain and revise each decision during the follow-up discussion.
