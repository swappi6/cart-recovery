package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cartrecovery/abandonment"
	"cartrecovery/commons"
	"cartrecovery/consumer"
	"cartrecovery/reminders"
	"github.com/segmentio/kafka-go"
)

// All code in this file is demo setup. It connects to infrastructure the user
// has already started; it never starts or stops Docker containers.
type demo struct {
	t                 *testing.T
	ctx               context.Context
	cancel            context.CancelFunc
	database          *commons.Postgres
	databaseURL       string
	clock             *commons.FakeClock
	cart              string
	renderer          *reminders.Renderer
	abandonmentWorker *abandonment.Worker
	reminderWorker    *reminders.Worker
	producer          *kafka.Writer
	reader            *kafka.Reader
	running           sync.WaitGroup
	failures          chan error
	reminderCancel    context.CancelFunc
	reminderDone      chan struct{}
}

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func startDemo(t *testing.T) *demo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	databaseURL := testDatabaseURL(t)
	database, err := commons.OpenPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
	if err = database.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	brokers := strings.Split(setting("KAFKA_BROKERS", "localhost:19092"), ",")
	topic := fmt.Sprintf("recovery-demo-%d", time.Now().UnixNano())
	client := kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	created, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{
		{Topic: topic, NumPartitions: 3, ReplicationFactor: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = created.Errors[topic]; err != nil {
		t.Fatal(err)
	}
	renderer, err := reminders.DefaultRenderer()
	if err != nil {
		t.Fatal(err)
	}
	clock := commons.NewFakeClock(time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC))
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: topic, StartOffset: kafka.FirstOffset,
		CommitInterval: 0, MaxWait: 100 * time.Millisecond,
	})
	d := &demo{
		t: t, ctx: ctx, cancel: cancel, database: database, databaseURL: databaseURL, clock: clock, cart: t.Name(), renderer: renderer,
		abandonmentWorker: abandonment.New(database, clock), reminderWorker: reminders.New(database, clock, renderer),
		producer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll},
		reader:   reader, failures: make(chan error, 3),
	}
	d.abandonmentWorker.PollInterval = 50 * time.Millisecond
	d.reminderWorker.PollInterval = 50 * time.Millisecond
	t.Cleanup(d.close)
	input := consumer.New(database, commons.DefaultPolicy())
	d.startBackground(func() error { return input.RunKafka(ctx, reader) })
	d.startBackground(func() error { return d.abandonmentWorker.Run(ctx) })
	d.startReminderWorker()
	t.Logf("Kafka topic: %s", topic)
	return d
}

func (d *demo) startBackground(run func() error) {
	d.running.Add(1)
	go func() {
		defer d.running.Done()
		if err := run(); err != nil && !errors.Is(err, context.Canceled) {
			d.failures <- err
		}
	}()
}
func (d *demo) startReminderWorker() {
	ctx, cancel := context.WithCancel(d.ctx)
	done := make(chan struct{})
	d.reminderCancel, d.reminderDone = cancel, done
	worker := d.reminderWorker
	d.startBackground(func() error { defer close(done); return worker.Run(ctx) })
}
func (d *demo) stopReminderWorker() { d.reminderCancel(); <-d.reminderDone }
func (d *demo) restartReminderWorker(database *commons.Postgres) {
	d.stopReminderWorker()
	d.reminderWorker = reminders.New(database, d.clock, d.renderer)
	d.reminderWorker.PollInterval = 50 * time.Millisecond
	d.startReminderWorker()
}
func (d *demo) close() {
	d.cancel()
	d.reader.Close()
	d.running.Wait()
	d.producer.Close()
}
func (d *demo) checkWorkerErrors() {
	d.t.Helper()
	select {
	case err := <-d.failures:
		d.t.Fatal(err)
	default:
	}
}

func (d *demo) emit(revision int64, kind commons.EventKind) {
	d.t.Helper()
	event := commons.Event{
		ID: fmt.Sprintf("%s-%d", d.cart, revision), CartID: d.cart, Revision: revision, Kind: kind, OccurredAt: d.clock.Now(),
		Snapshot: commons.Snapshot{
			Recipient: commons.Recipient{UserID: "shopper-" + d.cart, FirstName: "Sam", ContactRef: "contact-" + d.cart, ContactAllowed: true},
			Cart:      commons.CartContents{Items: []commons.Item{{ProductID: "mug", Name: "Travel mug", Quantity: 2}}, URL: "https://shop.example/cart/" + d.cart},
		},
	}
	data, err := json.Marshal(event)
	if err != nil {
		d.t.Fatal(err)
	}
	if err = d.producer.WriteMessages(d.ctx, kafka.Message{Key: []byte(d.cart), Value: data}); err != nil {
		d.t.Fatal(err)
	}
	// Wait for database application before the case advances the clock again.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		d.checkWorkerErrors()
		cart, ok, err := d.database.Cart(d.ctx, d.cart)
		if err != nil {
			d.t.Fatal(err)
		}
		if ok && cart.Revision >= revision {
			break
		}
		select {
		case <-d.ctx.Done():
			d.t.Fatal(d.ctx.Err())
		case <-ticker.C:
		}
	}
	d.t.Logf("%s Kafka → %s, revision %d", d.clock.Now().Format("Mon 15:04:05"), kind, revision)
}
func (d *demo) rows() []commons.Reminder {
	d.t.Helper()
	rows, err := d.database.Reminders(d.ctx, d.cart)
	if err != nil {
		d.t.Fatal(err)
	}
	return rows
}

// Observe database state across several automatic polls. This method only
// reads rows; it never runs detection or triggers a reminder itself.
func (d *demo) expect(statuses ...commons.ReminderStatus) {
	d.t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var stableSince time.Time
	for {
		d.checkWorkerErrors()
		rows := d.rows()
		match := len(rows) == len(statuses)
		for i := 0; match && i < len(rows); i++ {
			match = rows[i].Status == statuses[i]
		}
		if match {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= 150*time.Millisecond {
				d.t.Logf("%s reminder states: %v", d.clock.Now().Format("Mon 15:04:05"), statuses)
				return
			}
		} else {
			stableSince = time.Time{}
		}
		select {
		case <-timeout.C:
			d.t.Fatalf("wanted states %v, got %+v", statuses, rows)
		case <-ticker.C:
		}
	}
}
func (d *demo) expectDeadlines(deadlines ...time.Time) {
	d.t.Helper()
	rows := d.rows()
	if len(rows) != len(deadlines) {
		d.t.Fatalf("reminders=%d, want %d", len(rows), len(deadlines))
	}
	for i, due := range deadlines {
		if !rows[i].DueAt.Equal(due) {
			d.t.Fatalf("step %d: due=%s, want %s", i+1, rows[i].DueAt, due)
		}
	}
}

// Only the two failure demos use this: leave a real claim unfinished to model
// a worker pausing or dying at that point. Normal cases use automatic polling.
func (d *demo) claimWithoutCompleting() commons.Claim {
	d.t.Helper()
	rows, err := d.reminderWorker.ClaimDue(d.ctx, 1)
	if err != nil {
		d.t.Fatal(err)
	}
	if len(rows) != 1 {
		d.t.Fatalf("claims=%d, want 1", len(rows))
	}
	return rows[0]
}
