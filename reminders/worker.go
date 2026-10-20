// Package reminders claims scheduled work and records personalized triggers.
// There is intentionally no email/SMS provider adapter.
package reminders

import (
	"context"
	"log"
	"math/rand/v2"
	"time"

	"cartrecovery/commons"
)

type Worker struct {
	Store           *commons.Postgres
	Clock           commons.Clock
	Renderer        *Renderer
	BatchSize       int
	PollInterval    time.Duration
	Lease           time.Duration
	MaxCrashRetries int
	WorkAttempts    int
	RetryBase       time.Duration
	PauseAfterClaim time.Duration // Demo only: leave time to stop the process with an unfinished lease.
}

func New(store *commons.Postgres, clock commons.Clock, renderer *Renderer) *Worker {
	return &Worker{
		Store: store, Clock: clock, Renderer: renderer,
		BatchSize: 50, Lease: 30 * time.Second, PollInterval: 5 * time.Second,
		MaxCrashRetries: 3, WorkAttempts: 3, RetryBase: 100 * time.Millisecond,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()
	for {
		if _, err := w.Poll(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) Poll(ctx context.Context) (int, error) {
	claims, err := w.ClaimDue(ctx, w.BatchSize)
	if err != nil {
		return 0, err
	}
	triggered := 0
	for _, claim := range claims {
		log.Printf("reminders: claimed cart=%s step=%d retry=%d lease_until=%s", claim.Reminder.CartID, claim.Reminder.Step, claim.Reminder.RetryCount, claim.Reminder.LeaseUntil.Format(time.RFC3339))
		ok, err := w.Process(ctx, claim)
		if err != nil {
			return triggered, err
		}
		if ok {
			triggered++
		}
	}
	return triggered, nil
}

// Process retries transient work in code. The database is updated only for
// the terminal outcome; leases recover a lost process, not normal failures.
func (w *Worker) Process(ctx context.Context, claim commons.Claim) (bool, error) {
	if w.PauseAfterClaim > 0 {
		log.Printf("reminders: demo pause after claim (%s); stop this process to leave the lease unfinished", w.PauseAfterClaim)
		if err := (commons.RealClock{}).Sleep(ctx, w.PauseAfterClaim); err != nil {
			return false, err
		}
	}
	var lastError error
	for attempt := 0; attempt < w.WorkAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return false, err // Leave the claim for lease recovery.
		}
		current, err := w.isCurrent(ctx, claim)
		if err != nil {
			return false, err
		}
		if !current {
			return false, nil
		}
		message, err := w.Renderer.Render(claim)
		if err == nil {
			triggered, err := w.Complete(ctx, claim, message)
			if triggered {
				log.Printf("reminders: TRIGGERED cart=%s step=%d reminder=%s\nDEMO — would send\nSubject: %s\nBody: %s", claim.Reminder.CartID, claim.Reminder.Step, claim.Reminder.ID, message.Subject, message.Body)
			}
			return triggered, err
		}
		lastError = err
		if attempt+1 < w.WorkAttempts {
			backoff := min(w.RetryBase*time.Duration(1<<attempt), 2*time.Second)
			delay := time.Duration(rand.Float64() * float64(backoff))
			if err := w.Clock.Sleep(ctx, delay); err != nil {
				return false, err
			}
		}
	}
	_, err := w.Fail(ctx, claim, lastError)
	return false, err
}
