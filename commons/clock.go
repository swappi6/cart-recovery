package commons

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Clock keeps deadlines and retry waits testable without wall-clock sleeps.
type Clock interface {
	Read(context.Context) (time.Time, error)
	Sleep(context.Context, time.Duration) error
}

type RealClock struct{}

func (RealClock) Read(ctx context.Context) (time.Time, error) {
	return time.Now().UTC(), ctx.Err()
}

func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// HTTPClock is only for the multi-process demo. Normal services use RealClock.
// A clock outage stops work rather than silently switching to a different time.
type HTTPClock struct {
	URL string
}

func (c HTTPClock) Read(ctx context.Context) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return time.Time{}, err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return time.Time{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("demo clock: HTTP %d", response.StatusCode)
	}
	var body struct {
		Now time.Time `json:"now"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return time.Time{}, err
	}
	if body.Now.IsZero() {
		return time.Time{}, fmt.Errorf("demo clock returned no time")
	}
	return body.Now, nil
}

func (HTTPClock) Sleep(ctx context.Context, d time.Duration) error {
	return (RealClock{}).Sleep(ctx, d)
}

// FakeClock advances explicitly; Sleep advances it immediately for retry tests.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewFakeClock(now time.Time) *FakeClock { return &FakeClock{now: now} }

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) Read(ctx context.Context) (time.Time, error) {
	return c.Now(), ctx.Err()
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *FakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(d)
	return nil
}
