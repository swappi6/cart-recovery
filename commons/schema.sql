CREATE TABLE IF NOT EXISTS cart_recovery (
  cart_id TEXT PRIMARY KEY,
  last_revision BIGINT,
  status TEXT NOT NULL DEFAULT 'COMPLETED',
  last_edit_at TIMESTAMPTZ,
  abandon_at TIMESTAMPTZ,
  snapshot JSONB NOT NULL DEFAULT '{}',
  policy JSONB NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS cart_reminders (
  reminder_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cart_id TEXT NOT NULL REFERENCES cart_recovery(cart_id),
  step SMALLINT NOT NULL,
  due_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL,
  config_version TEXT NOT NULL,
  retry_count SMALLINT NOT NULL DEFAULT 0,
  lease_until TIMESTAMPTZ,
  last_error TEXT NOT NULL DEFAULT '',
  rendered_payload JSONB,
  triggered_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS carts_due ON cart_recovery (abandon_at) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS reminders_due ON cart_reminders (due_at) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS reminders_cart ON cart_reminders (cart_id);
CREATE INDEX IF NOT EXISTS reminders_leases ON cart_reminders (lease_until) WHERE status = 'PROCESSING';
