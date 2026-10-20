// Package abandonment owns the inactivity scan and reminder scheduling.
package abandonment

import (
	"context"
	"log"
	"time"

	"cartrecovery/commons"
)

type Worker struct {
	Store        *commons.Postgres
	Clock        commons.Clock
	BatchSize    int
	PollInterval time.Duration
}

func New(store *commons.Postgres, clock commons.Clock) *Worker {
	return &Worker{Store: store, Clock: clock, BatchSize: 200, PollInterval: 5 * time.Second}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()
	for {
		count, err := w.Poll(ctx)
		if err != nil {
			return err
		}
		if count > 0 {
			log.Printf("abandonment: scheduled reminders for %d carts", count)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) Poll(ctx context.Context) (int, error) {
	now, err := w.Clock.Read(ctx)
	if err != nil {
		return 0, err
	}
	scheduled := 0
	err = w.Store.InTransaction(ctx, func(tx *commons.Tx) error {
		carts, err := tx.DueCarts(now, w.BatchSize)
		if err != nil {
			return err
		}
		for _, cart := range carts {
			cart.Status = commons.Abandoned
			if err := tx.WriteCart(cart); err != nil {
				return err
			}
			for step, offset := range cart.Policy.Offsets {
				due := cart.AbandonAt.Add(offset)
				if err := tx.InsertReminder(commons.Reminder{
					CartID: cart.ID, Step: step + 1, DueAt: due, ExpiresAt: due.Add(cart.Policy.MaxLateness), Status: commons.Pending, ConfigVersion: cart.Policy.Version,
				}); err != nil {
					return err
				}
			}
			scheduled++
		}
		return nil
	})
	return scheduled, err
}
