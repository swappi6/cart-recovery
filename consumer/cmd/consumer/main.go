package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cartrecovery/commons"
	"cartrecovery/consumer"
	"github.com/segmentio/kafka-go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	address := flag.String("addr", "127.0.0.1:8081", "HTTP listen address")
	demoMode := flag.Bool("demo", false, "enable event publishing and an adjustable in-memory clock")
	inactivity := flag.Duration("inactivity", 30*time.Minute, "inactivity before abandonment")
	offsets := flag.String("offsets", "30m,1h,24h", "reminder offsets from abandonment")
	flag.Parse()
	policy := commons.DefaultPolicy()
	policy.Inactivity = *inactivity
	policy.Offsets = nil
	for _, value := range strings.Split(*offsets, ",") {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return fmt.Errorf("invalid reminder offset %q", value)
		}
		policy.Offsets = append(policy.Offsets, d)
	}
	if policy.Inactivity <= 0 {
		return fmt.Errorf("inactivity must be positive")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	database, err := commons.OpenPostgres(ctx, setting("DATABASE_URL", "postgres://cart:cart@localhost:15432/cart_recovery?sslmode=disable"))
	if err != nil {
		return err
	}
	defer database.Close()
	if err := database.Migrate(ctx); err != nil {
		return err
	}
	brokers := strings.Split(setting("KAFKA_BROKERS", "localhost:19092"), ",")
	topic := setting("KAFKA_TOPIC", "cart-events")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: setting("KAFKA_GROUP", "cart-recovery"),
		StartOffset: kafka.FirstOffset, CommitInterval: 0, MaxWait: 100 * time.Millisecond,
	})
	defer reader.Close()
	producer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	defer producer.Close()
	api := &demoAPI{store: database, producer: producer, clock: commons.RealClock{}}
	if *demoMode {
		api.fakeClock = commons.NewFakeClock(time.Now().UTC().Truncate(time.Second))
		api.clock = api.fakeClock
	}
	server := &http.Server{Addr: *address, Handler: api.routes(), ReadHeaderTimeout: 5 * time.Second}
	failures := make(chan error, 2)
	go func() { failures <- server.ListenAndServe() }()
	go func() { failures <- consumer.New(database, policy).RunKafka(ctx, reader) }()
	log.Printf("consumer: HTTP %s, Kafka topic=%s, demo=%t", *address, topic, *demoMode)
	if api.fakeClock != nil {
		log.Printf("demo clock frozen at %s; advance via POST /demo/clock/advance", api.fakeClock.Now().Format(time.RFC3339))
	}
	select {
	case <-ctx.Done():
	case err = <-failures:
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	_ = server.Shutdown(shutdown)
	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
