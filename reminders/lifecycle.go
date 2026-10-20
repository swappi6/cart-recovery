package reminders

import (
	"context"
	"time"

	"cartrecovery/commons"
)

// ClaimDue checks eligibility and takes a lease in one transaction. PostgreSQL
// joins the cart state before acquiring row locks with SKIP LOCKED.
func (w *Worker) ClaimDue(ctx context.Context, limit int) ([]commons.Claim, error) {
	now, err := w.Clock.Read(ctx)
	if err != nil {
		return nil, err
	}
	var claims []commons.Claim
	err = w.Store.InTransaction(ctx, func(tx *commons.Tx) error {
		due, err := tx.DueReminders(now, limit)
		if err != nil {
			return err
		}
		for _, claim := range due {
			reminder := claim.Reminder
			recipient := claim.Snapshot.Recipient
			switch {
			case !recipient.ContactAllowed || recipient.ContactRef == "":
				reminder.Status = commons.Cancelled
			case !reminder.ExpiresAt.After(now):
				reminder.Status = commons.Expired
			case reminder.Status == commons.Processing && reminder.RetryCount >= w.MaxCrashRetries:
				reminder.Status = commons.Failed
				reminder.LastError = "crash retry limit reached"
			default:
				if reminder.Status == commons.Processing {
					reminder.RetryCount++
				}
				reminder.Status = commons.Processing
				// PostgreSQL stores microsecond timestamps; return the exact persisted fence.
				reminder.LeaseUntil = now.Add(w.Lease).Truncate(time.Microsecond)
				claim.Reminder = reminder
				claims = append(claims, claim)
			}
			if reminder.Status != commons.Processing {
				reminder.LeaseUntil = time.Time{}
			}
			if err := tx.WriteReminder(reminder); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}

func (w *Worker) Complete(ctx context.Context, claim commons.Claim, message commons.Message) (bool, error) {
	now, err := w.Clock.Read(ctx)
	if err != nil {
		return false, err
	}
	triggered := false
	err = w.Store.InTransaction(ctx, func(tx *commons.Tx) error {
		r, _, err := tx.Reminder(claim.Reminder.ID)
		if err != nil {
			return err
		}
		if !owns(r, claim, now) {
			return nil
		}
		if !r.ExpiresAt.After(now) {
			r.Status = commons.Expired
		} else {
			r.Status = commons.Triggered
			r.Message = message
			r.TriggeredAt = now
			triggered = true
		}
		r.LeaseUntil = time.Time{}
		return tx.WriteReminder(r)
	})
	return triggered && err == nil, err
}

func (w *Worker) Fail(ctx context.Context, claim commons.Claim, failure error) (bool, error) {
	now, err := w.Clock.Read(ctx)
	if err != nil {
		return false, err
	}
	failed := false
	err = w.Store.InTransaction(ctx, func(tx *commons.Tx) error {
		r, _, err := tx.Reminder(claim.Reminder.ID)
		if err != nil {
			return err
		}
		if !owns(r, claim, now) {
			return nil
		}
		r.Status = commons.Failed
		r.LastError = failure.Error()
		r.LeaseUntil = time.Time{}
		failed = true
		return tx.WriteReminder(r)
	})
	return failed && err == nil, err
}

// ReplayFailed preserves reminder identity and rechecks current eligibility.
func (w *Worker) ReplayFailed(ctx context.Context, id string) (bool, error) {
	now, err := w.Clock.Read(ctx)
	if err != nil {
		return false, err
	}
	replayed := false
	err = w.Store.InTransaction(ctx, func(tx *commons.Tx) error {
		r, ok, err := tx.Reminder(id)
		if err != nil {
			return err
		}
		if !ok || r.Status != commons.Failed || !r.ExpiresAt.After(now) {
			return nil
		}
		cart, _, err := tx.Cart(r.CartID)
		if err != nil {
			return err
		}
		recipient := cart.Snapshot.Recipient
		if cart.Status != commons.Abandoned || !recipient.ContactAllowed || recipient.ContactRef == "" {
			return nil
		}
		r.Status = commons.Pending
		r.RetryCount = 0
		replayed = true
		return tx.WriteReminder(r)
	})
	return replayed && err == nil, err
}

func (w *Worker) isCurrent(ctx context.Context, claim commons.Claim) (bool, error) {
	now, err := w.Clock.Read(ctx)
	if err != nil {
		return false, err
	}
	current := false
	err = w.Store.InTransaction(ctx, func(tx *commons.Tx) error {
		r, _, err := tx.Reminder(claim.Reminder.ID)
		if err != nil {
			return err
		}
		current = owns(r, claim, now)
		return nil
	})
	return current, err
}

func owns(r commons.Reminder, claim commons.Claim, now time.Time) bool {
	return r.Status == commons.Processing && r.LeaseUntil.Equal(claim.Reminder.LeaseUntil) && r.LeaseUntil.After(now)
}
