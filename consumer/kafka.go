package consumer

import (
	"context"
	"encoding/json"
	"time"

	"cartrecovery/commons"
	"github.com/segmentio/kafka-go"
)

// RunKafka applies each batch before committing offsets. A decode/database/
// commit error stops the consumer; restarting the same group replays any
// uncommitted work. Cart revisions make that replay safe.
func (c *Consumer) RunKafka(ctx context.Context, reader *kafka.Reader) error {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	type fetched struct {
		message kafka.Message
		err     error
	}
	messages := make(chan fetched)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		for {
			message, err := reader.FetchMessage(child)
			select {
			case messages <- fetched{message, err}:
			case <-child.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { cancel(); <-exited }()
	ticker := time.NewTicker(c.FlushInterval)
	defer ticker.Stop()
	var events []commons.Event
	var offsets []kafka.Message
	flush := func() error {
		if len(events) == 0 {
			return nil
		}
		if err := c.ApplyContext(ctx, events); err != nil {
			return err
		}
		if err := reader.CommitMessages(ctx, offsets...); err != nil {
			return err
		}
		events = events[:0]
		offsets = offsets[:0]
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case next := <-messages:
			if next.err != nil {
				return next.err
			}
			var event commons.Event
			if err := json.Unmarshal(next.message.Value, &event); err != nil {
				return err
			}
			events = append(events, event)
			offsets = append(offsets, next.message)
			if len(events) >= c.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}
