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

	"cartrecovery/abandonment"
	"cartrecovery/commons"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	address := flag.String("addr", "127.0.0.1:8082", "HTTP listen address")
	clockURL := flag.String("clock-url", "", "demo clock URL; omit to use real time")
	poll := flag.Duration("poll", 5*time.Second, "abandonment polling interval")
	flag.Parse()
	if *poll <= 0 {
		return errors.New("poll must be positive")
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
	worker := abandonment.New(database, clock)
	worker.PollInterval = *poll
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
		_, _ = w.Write([]byte("abandonment: up\n"))
	})
	server := &http.Server{Addr: *address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	failures := make(chan error, 2)
	go func() { failures <- server.ListenAndServe() }()
	go func() { failures <- worker.Run(ctx) }()
	log.Printf("abandonment: HTTP %s, automatic polling every %s", *address, *poll)
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
