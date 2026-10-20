package integration

import (
	"testing"
	"time"

	"cartrecovery/commons"
)

// Edit the events and clock advances in these five cases to change the demo.
// Each case publishes to Kafka and checks the resulting PostgreSQL rows.

func TestRemindersFireAtConfiguredTimes(t *testing.T) {
	demo := startDemo(t)
	start := demo.clock.Now()
	demo.emit(1, commons.CartUpdated)

	demo.clock.Advance(30 * time.Minute)
	demo.expect(commons.Pending, commons.Pending, commons.Pending)
	demo.expectDeadlines(start.Add(time.Hour), start.Add(90*time.Minute), start.Add(24*time.Hour+30*time.Minute))

	demo.clock.Advance(30 * time.Minute)
	demo.expect(commons.Triggered, commons.Pending, commons.Pending)

	demo.clock.Advance(30 * time.Minute)
	demo.expect(commons.Triggered, commons.Triggered, commons.Pending)

	demo.clock.Advance(23 * time.Hour)
	demo.expect(commons.Triggered, commons.Triggered, commons.Triggered)
}

func TestCartEditResetsInactivity(t *testing.T) {
	demo := startDemo(t)
	start := demo.clock.Now()
	demo.emit(1, commons.CartUpdated)

	demo.clock.Advance(20 * time.Minute)
	demo.emit(2, commons.CartUpdated) // Edit before the original 30-minute deadline.
	demo.clock.Advance(10 * time.Minute)
	demo.expect() // At the old deadline, there must still be no reminders.

	demo.clock.Advance(20 * time.Minute)
	demo.expect(commons.Pending, commons.Pending, commons.Pending)
	demo.expectDeadlines(start.Add(80*time.Minute), start.Add(110*time.Minute), start.Add(24*time.Hour+50*time.Minute))

	demo.clock.Advance(30 * time.Minute)
	demo.expect(commons.Triggered, commons.Pending, commons.Pending)
}

func TestCheckoutCancelsClaimedReminder(t *testing.T) {
	demo := startDemo(t)
	demo.emit(1, commons.CartUpdated)
	demo.clock.Advance(30 * time.Minute)
	demo.expect(commons.Pending, commons.Pending, commons.Pending)

	// Pause only to arrange the in-flight cancellation. Normal cases poll automatically.
	demo.stopReminderWorker()
	demo.clock.Advance(30 * time.Minute)
	unfinished := demo.claimWithoutCompleting()
	demo.emit(2, commons.CheckoutStarted) // Change to PurchaseComplete or CartCleared to try those events.
	if ok, err := demo.reminderWorker.Complete(demo.ctx, unfinished, commons.Message{Body: "stale worker"}); err != nil || ok {
		t.Fatalf("cancelled reminder completed: ok=%v err=%v", ok, err)
	}
	demo.restartReminderWorker(demo.database)
	demo.expect(commons.Cancelled, commons.Cancelled, commons.Cancelled)

	demo.clock.Advance(24 * time.Hour)
	demo.expect(commons.Cancelled, commons.Cancelled, commons.Cancelled)
}

func TestDuplicateEventsDoNotRepeatReminders(t *testing.T) {
	demo := startDemo(t)
	demo.emit(1, commons.CartUpdated)
	demo.clock.Advance(30 * time.Minute)

	demo.emit(1, commons.CartUpdated)                              // Replay the same revision through Kafka.
	demo.emit(2, commons.OwnerChanged)                             // A later revision confirms the replay was consumed.
	demo.expect(commons.Pending, commons.Pending, commons.Pending) // Still exactly three rows.

	demo.clock.Advance(30 * time.Minute)
	demo.expect(commons.Triggered, commons.Pending, commons.Pending)
}

func TestReplacementWorkerRecoversExpiredClaim(t *testing.T) {
	demo := startDemo(t)
	demo.emit(1, commons.CartUpdated)
	demo.clock.Advance(30 * time.Minute)
	demo.expect(commons.Pending, commons.Pending, commons.Pending)

	demo.stopReminderWorker()
	demo.clock.Advance(30 * time.Minute)
	unfinished := demo.claimWithoutCompleting() // Model a worker dying after persisting its claim.
	demo.expect(commons.Processing, commons.Pending, commons.Pending)

	database, err := commons.OpenPostgres(demo.ctx, demo.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { demo.stopReminderWorker(); database.Close() })
	demo.restartReminderWorker(database)
	demo.expect(commons.Processing, commons.Pending, commons.Pending) // Not reclaimed before expiry.

	demo.clock.Advance(30 * time.Second)
	demo.expect(commons.Triggered, commons.Pending, commons.Pending) // The replacement polls automatically.
	if got := demo.rows()[0].RetryCount; got != 1 {
		t.Fatalf("retry_count=%d, want 1", got)
	}
	if ok, err := demo.reminderWorker.Complete(demo.ctx, unfinished, commons.Message{Body: "stale worker"}); err != nil || ok {
		t.Fatalf("old claim completed again: ok=%v err=%v", ok, err)
	}
}
