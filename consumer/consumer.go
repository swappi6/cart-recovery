// Package consumer batches events from the existing cart service. A production
// Kafka adapter calls Apply and commits offsets only after it succeeds.
package consumer

import (
	"context"
	"fmt"
	"sort"
	"time"

	"cartrecovery/commons"
)

type Consumer struct {
	Store         *commons.Postgres
	Policy        commons.Policy
	BatchSize     int
	FlushInterval time.Duration
}

func New(store *commons.Postgres, policy commons.Policy) *Consumer {
	return &Consumer{Store: store, Policy: policy, BatchSize: 1000, FlushInterval: 100 * time.Millisecond}
}

func (c *Consumer) Apply(events []commons.Event) error {
	return c.ApplyContext(context.Background(), events)
}

func (c *Consumer) ApplyContext(ctx context.Context, events []commons.Event) error {
	groups := make(map[string][]commons.Event)
	for _, event := range events {
		switch event.Kind {
		case commons.CartCreated, commons.CartUpdated, commons.CartCleared, commons.CartDeleted,
			commons.CheckoutStarted, commons.PurchaseComplete, commons.OwnerChanged:
		default:
			return fmt.Errorf("unsupported event kind %q", event.Kind)
		}
		groups[event.CartID] = append(groups[event.CartID], event)
	}
	for _, group := range groups {
		sort.SliceStable(group, func(i, j int) bool { return group[i].Revision < group[j].Revision })
	}

	return c.Store.InTransaction(ctx, func(tx *commons.Tx) error {
		ids := make([]string, 0, len(groups))
		for id := range groups {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		carts, err := tx.LockCarts(ids)
		if err != nil {
			return err
		}
		for cartID, group := range groups {
			cart, exists := carts[cartID]
			changed, cancel := false, false
			for _, event := range group {
				if exists && event.Revision <= cart.Revision {
					continue
				}
				if !exists {
					cart = commons.Cart{ID: cartID, Policy: c.Policy}
				}
				cart.Revision = event.Revision
				if event.Kind == commons.OwnerChanged {
					if !exists {
						cart.Snapshot = event.Snapshot
						cart.LastEdit = event.LastEditAt
						cart.AbandonAt = event.LastEditAt.Add(c.Policy.Inactivity)
						cart.Status = statusForItems(event.Snapshot)
					}
					cart.Snapshot.Recipient = event.Snapshot.Recipient
				} else {
					cancel = true
					cart.Snapshot = event.Snapshot
					switch event.Kind {
					case commons.CartCreated, commons.CartUpdated:
						cart.Status = statusForItems(event.Snapshot)
						cart.LastEdit = event.OccurredAt
						cart.Policy = c.Policy
						cart.AbandonAt = event.OccurredAt.Add(c.Policy.Inactivity)
					case commons.CheckoutStarted:
						cart.Status = commons.Resumed
					default:
						cart.Status = commons.Completed
					}
				}
				exists, changed = true, true
			}
			if !changed {
				continue
			}
			if err := tx.WriteCart(cart); err != nil {
				return err
			}
			if cancel {
				if err := cancelReminders(tx, cartID); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func statusForItems(snapshot commons.Snapshot) commons.CartStatus {
	if len(snapshot.Cart.Items) == 0 {
		return commons.Completed
	}
	return commons.Active
}

func cancelReminders(tx *commons.Tx, cartID string) error {
	rows, err := tx.CartReminders(cartID)
	if err != nil {
		return err
	}
	for _, reminder := range rows {
		if reminder.Status == commons.Pending || reminder.Status == commons.Processing || reminder.Status == commons.Failed {
			reminder.Status = commons.Cancelled
			reminder.LeaseUntil = time.Time{}
			if err := tx.WriteReminder(reminder); err != nil {
				return err
			}
		}
	}
	return nil
}
