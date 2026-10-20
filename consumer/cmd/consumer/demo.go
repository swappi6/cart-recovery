package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"cartrecovery/commons"
	"github.com/segmentio/kafka-go"
)

// This API is a local demo tool, not a replacement for the cart service.
// Events still travel through Kafka before the consumer writes PostgreSQL.
type demoAPI struct {
	store     *commons.Postgres
	producer  *kafka.Writer
	clock     commons.Clock
	fakeClock *commons.FakeClock
}

func (a *demoAPI) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := a.store.Pool.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, map[string]string{"service": "consumer", "status": "up"})
	})
	if a.fakeClock != nil {
		mux.HandleFunc("POST /demo/events", a.publishEvent)
		mux.HandleFunc("GET /demo/clock", a.readClock)
		mux.HandleFunc("POST /demo/clock/advance", a.advanceClock)
		mux.HandleFunc("GET /carts/{id}", a.readCart)
	}
	return mux
}

func (a *demoAPI) publishEvent(w http.ResponseWriter, r *http.Request) {
	var input struct {
		CartID   string            `json:"cart_id"`
		Revision int64             `json:"revision"`
		Kind     commons.EventKind `json:"kind"`
		Snapshot *commons.Snapshot `json:"snapshot,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if input.CartID == "" || input.Revision < 1 {
		http.Error(w, "cart_id and a positive revision are required", http.StatusBadRequest)
		return
	}
	switch input.Kind {
	case commons.CartCreated, commons.CartUpdated, commons.CheckoutStarted, commons.PurchaseComplete,
		commons.CartCleared, commons.CartDeleted, commons.OwnerChanged:
	default:
		http.Error(w, "unsupported event kind", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	now, err := a.clock.Read(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	cart, exists, err := a.store.Cart(ctx, input.CartID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	snapshot := commons.Snapshot{
		Recipient: commons.Recipient{UserID: "shopper-" + input.CartID, FirstName: "Sam", ContactRef: "contact-" + input.CartID, ContactAllowed: true},
		Cart: commons.CartContents{
			Items: []commons.Item{{ProductID: "mug", Name: "Travel mug", Quantity: 2}}, URL: "https://shop.example/cart/" + input.CartID,
		},
	}
	if exists {
		snapshot = cart.Snapshot
	}
	if input.Snapshot != nil {
		snapshot = *input.Snapshot
	}
	if input.Kind == commons.CartCleared || input.Kind == commons.CartDeleted {
		snapshot.Cart.Items = nil
	}
	lastEdit := now
	if exists {
		lastEdit = cart.LastEdit
	}
	event := commons.Event{
		ID: fmt.Sprintf("%s-%d", input.CartID, input.Revision), CartID: input.CartID,
		Revision: input.Revision, Kind: input.Kind, OccurredAt: now, LastEditAt: lastEdit, Snapshot: snapshot,
	}
	data, err := json.Marshal(event)
	if err == nil {
		err = a.producer.WriteMessages(ctx, kafka.Message{Key: []byte(input.CartID), Value: data})
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	// Wait only in this demo endpoint, so the next clock advance cannot overtake
	// the event. The real cart service publishes asynchronously.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, found, err := a.store.Cart(ctx, input.CartID)
		if err != nil {
			http.Error(w, "published to Kafka; database observation failed: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if found && current.Revision >= input.Revision {
			log.Printf("consumer: observed cart=%s revision=%d kind=%s", input.CartID, input.Revision, input.Kind)
			writeJSON(w, map[string]any{"published": true, "cart": current})
			return
		}
		select {
		case <-ctx.Done():
			http.Error(w, "published to Kafka; still waiting for consumer. Inspect the cart before retrying.", http.StatusGatewayTimeout)
			return
		case <-ticker.C:
		}
	}
}

func (a *demoAPI) readClock(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"now": a.fakeClock.Now()})
}

func (a *demoAPI) advanceClock(w http.ResponseWriter, r *http.Request) {
	var input struct {
		By string `json:"by"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	d, err := time.ParseDuration(input.By)
	if err != nil || d <= 0 {
		http.Error(w, "by must be a positive duration such as 30m, 1h or 31s", http.StatusBadRequest)
		return
	}
	a.fakeClock.Advance(d)
	log.Printf("demo clock: advanced %s to %s", d, a.fakeClock.Now().Format(time.RFC3339))
	a.readClock(w, r)
}

func (a *demoAPI) readCart(w http.ResponseWriter, r *http.Request) {
	cart, exists, err := a.store.Cart(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !exists {
		http.NotFound(w, r)
		return
	}
	rows, err := a.store.Reminders(r.Context(), cart.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]any{"cart": cart, "reminders": rows})
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
