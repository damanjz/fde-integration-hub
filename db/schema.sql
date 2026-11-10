-- Integration hub schema. Applied by docker-entrypoint-initdb.d, tests and chaos.

CREATE TABLE IF NOT EXISTS events (
  event_id        text PRIMARY KEY,
  seq             bigserial UNIQUE,                    -- arrival order; tie-break for per-order ordering
  event_type      text NOT NULL,
  order_id        text,
  occurred_at     timestamptz NOT NULL,           -- storefront event time (payload created_at)
  raw_body        text NOT NULL,
  payload         jsonb NOT NULL,
  body_sha256     text NOT NULL,
  received_at     timestamptz NOT NULL DEFAULT now(),
  duplicate_count integer NOT NULL DEFAULT 0
);

-- Outbox: one row per (event, destination).
-- status: pending (waiting for next_attempt_at), in_flight (claimed; next_attempt_at is the lease expiry),
-- delivered, dead, blocked (an earlier delivery for the same order and destination is dead: never sent),
-- superseded (a later event for the same order and destination was already delivered: never sent, audited),
-- skipped (a dead row an operator released on purpose, audited).
-- Ordering key within (order_id, destination): (occurred_at, event_seq).
CREATE TABLE IF NOT EXISTS deliveries (
  id              bigserial PRIMARY KEY,
  event_id        text NOT NULL REFERENCES events (event_id),
  destination     text NOT NULL,
  order_id        text,
  occurred_at     timestamptz NOT NULL,
  event_seq       bigint NOT NULL,
  delivery_key    text NOT NULL UNIQUE,
  status          text NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'in_flight', 'delivered', 'dead', 'blocked', 'superseded', 'skipped')),
  attempts        integer NOT NULL DEFAULT 0,   -- claim cycles, including 429s
  uncounted       integer NOT NULL DEFAULT 0,   -- cycles that do not spend the retry budget: 429, failures while the breaker is open
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  lease_token     uuid,
  needs_reconcile boolean NOT NULL DEFAULT false, -- previous outcome unknown: look at the destination first
  event_override  jsonb,                          -- corrected event used instead of the stored one (audited replay)
  last_error      text,
  last_status_code integer,
  dead_reason     text,
  dead_body       text,
  delivered_via   text,                           -- send | reconcile | duplicate_ack
  replay_count    integer NOT NULL DEFAULT 0,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  delivered_at    timestamptz,
  dead_at         timestamptz,
  UNIQUE (event_id, destination)
);
CREATE INDEX IF NOT EXISTS deliveries_claim_idx ON deliveries (next_attempt_at)
  WHERE status IN ('pending', 'in_flight');
CREATE INDEX IF NOT EXISTS deliveries_status_idx ON deliveries (status, destination);
CREATE INDEX IF NOT EXISTS deliveries_order_idx ON deliveries (destination, order_id) WHERE order_id IS NOT NULL;

-- Failure log: every call to a destination, plus a row when a lease expired (worker crash).
CREATE TABLE IF NOT EXISTS delivery_attempts (
  id            bigserial PRIMARY KEY,
  delivery_id   bigint NOT NULL REFERENCES deliveries (id),
  attempt_no    integer NOT NULL,
  kind          text NOT NULL CHECK (kind IN ('send', 'lookup', 'lease_expired', 'internal', 'superseded')),
  started_at    timestamptz NOT NULL,
  latency_ms    integer,
  status_code   integer,
  error_class   text,
  outcome       text NOT NULL,
  retry_after_s double precision,
  detail        text
);
CREATE INDEX IF NOT EXISTS delivery_attempts_delivery_idx ON delivery_attempts (delivery_id);
CREATE INDEX IF NOT EXISTS delivery_attempts_started_idx ON delivery_attempts (started_at);

-- Shared per-destination gate: token bucket + Retry-After block + circuit breaker.
-- Times are epoch seconds from the database clock so every worker process agrees.
CREATE TABLE IF NOT EXISTS destination_state (
  destination          text PRIMARY KEY,
  rate                 double precision NOT NULL,
  burst                double precision NOT NULL,
  tokens               double precision NOT NULL,
  refilled_at          double precision NOT NULL,
  blocked_until        double precision NOT NULL DEFAULT 0,
  consecutive_failures integer NOT NULL DEFAULT 0,
  rate_scale           double precision NOT NULL DEFAULT 1  -- adaptive multiplier on rate, lowered by 429s
);

CREATE TABLE IF NOT EXISTS counters (
  name  text PRIMARY KEY,
  value bigint NOT NULL DEFAULT 0
);

-- Operator and system decisions on a delivery: replay, corrected replay, skip, supersede.
CREATE TABLE IF NOT EXISTS delivery_audit (
  id          bigserial PRIMARY KEY,
  delivery_id bigint NOT NULL REFERENCES deliveries (id),
  action      text NOT NULL,
  actor       text NOT NULL,
  reason      text,
  detail      jsonb,
  at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS delivery_audit_delivery_idx ON delivery_audit (delivery_id);
