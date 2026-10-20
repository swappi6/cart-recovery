package commons

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Postgres struct{ Pool *pgxpool.Pool }

func OpenPostgres(ctx context.Context, url string) (*Postgres, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 12
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Postgres{Pool: pool}, nil
}

func (p *Postgres) Close()                            { p.Pool.Close() }
func (p *Postgres) Migrate(ctx context.Context) error { _, err := p.Pool.Exec(ctx, schema); return err }

func (p *Postgres) InTransaction(ctx context.Context, apply func(*Tx) error) error {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	work := &Tx{ctx: ctx, tx: tx, carts: make(map[string]Cart), reminders: make(map[string]Reminder)}
	if err = apply(work); err != nil {
		return err
	}
	if err = work.flush(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Tx queues writes so a consumer batch shares one database round trip.
// Reads take row locks; queued state is visible to subsequent primary-key reads.
type Tx struct {
	ctx       context.Context
	tx        pgx.Tx
	writes    pgx.Batch
	carts     map[string]Cart
	reminders map[string]Reminder
}

const cartColumns = `cart_id,last_revision,status,last_edit_at,abandon_at,snapshot,policy`
const reminderColumns = `reminder_id::text,cart_id,step,due_at,expires_at,status,config_version,retry_count,lease_until,last_error,rendered_payload,triggered_at`

type scanner interface{ Scan(...any) error }

func scanCart(row scanner) (Cart, bool, error) {
	var c Cart
	var revision *int64
	var edited, abandoned *time.Time
	var snapshot, policy []byte
	err := row.Scan(&c.ID, &revision, &c.Status, &edited, &abandoned, &snapshot, &policy)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	if revision == nil {
		return c, false, nil
	} // A locked placeholder for a new cart.
	c.Revision = *revision
	if edited != nil {
		c.LastEdit = *edited
	}
	if abandoned != nil {
		c.AbandonAt = *abandoned
	}
	if err = json.Unmarshal(snapshot, &c.Snapshot); err != nil {
		return c, false, err
	}
	err = json.Unmarshal(policy, &c.Policy)
	return c, true, err
}

func scanReminder(row scanner) (Reminder, bool, error) {
	var r Reminder
	var lease, triggered *time.Time
	var message []byte
	err := row.Scan(&r.ID, &r.CartID, &r.Step, &r.DueAt, &r.ExpiresAt, &r.Status, &r.ConfigVersion, &r.RetryCount, &lease, &r.LastError, &message, &triggered)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	if lease != nil {
		r.LeaseUntil = *lease
	}
	if triggered != nil {
		r.TriggeredAt = *triggered
	}
	if len(message) > 0 {
		err = json.Unmarshal(message, &r.Message)
	}
	return r, true, err
}

func (t *Tx) LockCarts(ids []string) (map[string]Cart, error) {
	// Insert missing keys before locking so first events for the same cart also
	// serialize. Sorting keys keeps competing consumer batches in lock order.
	_, err := t.tx.Exec(t.ctx, `INSERT INTO cart_recovery(cart_id)
  SELECT id FROM unnest($1::text[]) AS ids(id) ORDER BY id ON CONFLICT DO NOTHING`, ids)
	if err != nil {
		return nil, err
	}
	rows, err := t.tx.Query(t.ctx, `SELECT `+cartColumns+` FROM cart_recovery WHERE cart_id=ANY($1) ORDER BY cart_id FOR UPDATE`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	carts := make(map[string]Cart, len(ids))
	for rows.Next() {
		c, ok, err := scanCart(rows)
		if err != nil {
			return nil, err
		}
		if ok {
			carts[c.ID] = c
		}
	}
	return carts, rows.Err()
}

func (t *Tx) Cart(id string) (Cart, bool, error) {
	if cart, ok := t.carts[id]; ok {
		return cart, true, nil
	}
	return scanCart(t.tx.QueryRow(t.ctx, `SELECT `+cartColumns+` FROM cart_recovery WHERE cart_id=$1 FOR UPDATE`, id))
}

func (t *Tx) WriteCart(c Cart) error {
	snapshot, err := json.Marshal(c.Snapshot)
	if err != nil {
		return err
	}
	policy, err := json.Marshal(c.Policy)
	if err != nil {
		return err
	}
	t.writes.Queue(`UPDATE cart_recovery SET last_revision=$2,status=$3,last_edit_at=$4,abandon_at=$5,snapshot=$6,policy=$7 WHERE cart_id=$1`, c.ID, c.Revision, c.Status, nullableTime(c.LastEdit), nullableTime(c.AbandonAt), snapshot, policy)
	t.carts[c.ID] = c
	return nil
}

func (t *Tx) Reminder(id string) (Reminder, bool, error) {
	if r, ok := t.reminders[id]; ok {
		return r, true, nil
	}
	// Match the consumer/claim lock order: cart first, then reminder. The cart ID
	// lookup itself is unlocked; reminders never move between carts.
	var cartID string
	err := t.tx.QueryRow(t.ctx, `SELECT cart_id FROM cart_reminders WHERE reminder_id=$1`, id).Scan(&cartID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reminder{}, false, nil
	}
	if err != nil {
		return Reminder{}, false, err
	}
	if _, _, err = t.Cart(cartID); err != nil {
		return Reminder{}, false, err
	}
	return scanReminder(t.tx.QueryRow(t.ctx, `SELECT `+reminderColumns+` FROM cart_reminders WHERE reminder_id=$1 FOR UPDATE`, id))
}

func (t *Tx) WriteReminder(r Reminder) error {
	message, err := json.Marshal(r.Message)
	if err != nil {
		return err
	}
	t.writes.Queue(`UPDATE cart_reminders SET status=$2,retry_count=$3,lease_until=$4,last_error=$5,rendered_payload=$6,triggered_at=$7 WHERE reminder_id=$1`, r.ID, r.Status, r.RetryCount, nullableTime(r.LeaseUntil), r.LastError, message, nullableTime(r.TriggeredAt))
	t.reminders[r.ID] = r
	return nil
}

func (t *Tx) InsertReminder(r Reminder) error {
	t.writes.Queue(`INSERT INTO cart_reminders(cart_id,step,due_at,expires_at,status,config_version) VALUES($1,$2,$3,$4,$5,$6)`, r.CartID, r.Step, r.DueAt, r.ExpiresAt, r.Status, r.ConfigVersion)
	return nil
}

func (t *Tx) CartReminders(id string) ([]Reminder, error) {
	rows, err := t.tx.Query(t.ctx, `SELECT `+reminderColumns+` FROM cart_reminders WHERE cart_id=$1 ORDER BY reminder_id FOR UPDATE`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Reminder
	for rows.Next() {
		r, _, err := scanReminder(rows)
		if err != nil {
			return nil, err
		}
		if queued, ok := t.reminders[r.ID]; ok {
			r = queued
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (t *Tx) DueCarts(now time.Time, limit int) ([]Cart, error) {
	rows, err := t.tx.Query(t.ctx, `SELECT `+cartColumns+` FROM cart_recovery WHERE status='ACTIVE' AND abandon_at <= $1 ORDER BY abandon_at,cart_id LIMIT $2 FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Cart
	for rows.Next() {
		c, _, err := scanCart(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (t *Tx) DueReminders(now time.Time, limit int) ([]Claim, error) {
	// Join eligibility and lock acquisition in one query. Locking the cart also
	// serializes claims against the consumer's cancellation transaction.
	rows, err := t.tx.Query(t.ctx, `SELECT r.reminder_id::text,r.cart_id,r.step,r.due_at,r.expires_at,r.status,r.config_version,r.retry_count,r.lease_until,r.last_error,r.rendered_payload,r.triggered_at,c.snapshot
 FROM cart_reminders r JOIN cart_recovery c ON c.cart_id=r.cart_id
 WHERE c.status='ABANDONED' AND ((r.status='PENDING' AND r.due_at <= $1) OR (r.status='PROCESSING' AND r.lease_until <= $1))
 ORDER BY r.due_at,r.reminder_id LIMIT $2 FOR UPDATE OF c,r SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Claim
	for rows.Next() {
		var r Reminder
		var lease, triggered *time.Time
		var message, snapshot []byte
		if err = rows.Scan(&r.ID, &r.CartID, &r.Step, &r.DueAt, &r.ExpiresAt, &r.Status, &r.ConfigVersion, &r.RetryCount, &lease, &r.LastError, &message, &triggered, &snapshot); err != nil {
			return nil, err
		}
		if lease != nil {
			r.LeaseUntil = *lease
		}
		if triggered != nil {
			r.TriggeredAt = *triggered
		}
		if len(message) > 0 {
			if err = json.Unmarshal(message, &r.Message); err != nil {
				return nil, err
			}
		}
		claim := Claim{Reminder: r}
		if err = json.Unmarshal(snapshot, &claim.Snapshot); err != nil {
			return nil, err
		}
		result = append(result, claim)
	}
	return result, rows.Err()
}

func (t *Tx) flush() error {
	if t.writes.Len() == 0 {
		return nil
	}
	results := t.tx.SendBatch(t.ctx, &t.writes)
	defer results.Close()
	for range t.writes.Len() {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return results.Close()
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// Read helpers do not take locks and are used to inspect the pipeline.
func (p *Postgres) Cart(ctx context.Context, id string) (Cart, bool, error) {
	return scanCart(p.Pool.QueryRow(ctx, `SELECT `+cartColumns+` FROM cart_recovery WHERE cart_id=$1`, id))
}
func (p *Postgres) Reminders(ctx context.Context, id string) ([]Reminder, error) {
	rows, err := p.Pool.Query(ctx, `SELECT `+reminderColumns+` FROM cart_reminders WHERE cart_id=$1 ORDER BY step,due_at`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Reminder
	for rows.Next() {
		r, _, err := scanReminder(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
