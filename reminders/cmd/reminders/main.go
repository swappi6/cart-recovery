package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cartrecovery/commons"
	"cartrecovery/reminders"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	address := flag.String("addr", "127.0.0.1:8083", "HTTP listen address")
	clockURL := flag.String("clock-url", "", "demo clock URL; omit to use real time")
	poll := flag.Duration("poll", 5*time.Second, "reminder polling interval")
	pause := flag.Duration("pause-after-claim", 0, "demo only: pause before processing a claimed reminder")
	flag.Parse()
	if *poll <= 0 || *pause < 0 {
		return errors.New("poll must be positive and pause-after-claim cannot be negative")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://cart:cart@localhost:15432/cart_recovery?sslmode=disable"
	}
	database, err := commons.OpenPostgres(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	var clock commons.Clock = commons.RealClock{}
	if *clockURL != "" {
		clock = commons.HTTPClock{URL: *clockURL}
	}
	renderer, err := reminders.DefaultRenderer()
	if err != nil {
		return err
	}
	worker := reminders.New(database, clock, renderer)
	worker.PollInterval = *poll
	worker.PauseAfterClaim = *pause
	// Keep the crash demo focused on one persisted claim at a time.
	if *pause > 0 {
		worker.BatchSize = 1
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := database.Pool.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if _, err := clock.Read(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("reminders: up\n"))
	})
	server := &http.Server{Addr: *address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	failures := make(chan error, 2)
	go func() { failures <- server.ListenAndServe() }()
	go func() { failures <- worker.Run(ctx) }()
	log.Printf("reminders: HTTP %s, automatic polling every %s, pause_after_claim=%s", *address, *poll, *pause)
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
