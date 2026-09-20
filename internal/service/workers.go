package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/SelfRef/beeper-intercom/internal/store"
)

// deliveryWorker drains the outbox. A match becomes a row before any HTTP
// happens, so an action survives n8n restarting; the worker is what makes
// that row eventually become a request.
func (s *Service) deliveryWorker(ctx context.Context) {
	client := &http.Client{Timeout: s.conf().Actions.Timeout.Or(15 * time.Second)}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		due, err := s.store.DueDeliveries(ctx, 20)
		if err != nil {
			s.log.Error().Err(err).Msg("Failed to read the outbox")
			continue
		}
		for _, delivery := range due {
			s.attemptDelivery(ctx, client, delivery)
		}
	}
}

func (s *Service) attemptDelivery(ctx context.Context, client *http.Client, delivery *store.Delivery) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.URL, bytes.NewReader(delivery.Payload))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		var resp *http.Response
		resp, err = client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			if resp.StatusCode >= 300 {
				err = fmt.Errorf("HTTP %d", resp.StatusCode)
			}
		}
	}
	if err == nil {
		if err := s.store.MarkDelivered(ctx, delivery.ID); err != nil {
			s.log.Error().Err(err).Msg("Failed to mark delivery as delivered")
		}
		return
	}

	attempts := delivery.Attempts + 1
	status := "pending"
	if attempts >= s.conf().Actions.MaxAttempts {
		status = "failed"
	}
	// Exponential backoff capped at an hour: a receiver that is down for a
	// deploy should not be hammered, and one that is down for a day should
	// still get the action when it comes back.
	delay := time.Duration(1<<min(attempts, 12)) * time.Second
	if delay > time.Hour {
		delay = time.Hour
	}
	next := time.Now().Add(delay).UnixMilli()
	if err2 := s.store.MarkDeliveryFailed(ctx, delivery.ID, attempts, next, status, err.Error()); err2 != nil {
		s.log.Error().Err(err2).Msg("Failed to record delivery failure")
	}
	s.log.Warn().Err(err).Int("attempt", attempts).Str("url", delivery.URL).Msg("Delivery failed")
	if status == "failed" {
		// Out of retries: the action is lost unless someone retries it by hand,
		// which is worth a line in the status room rather than only in the logs.
		s.bridge.Status(ctx, fmt.Sprintf("Delivery #%d to %s gave up after %d attempts (%s). `POST /v1/deliveries/%d/retry` to try again.",
			delivery.ID, delivery.URL, attempts, err, delivery.ID))
	}
}

// cleanupWorker trims history. Notifications outlive deliveries because a
// reaction can arrive days after the message it acts on.
func (s *Service) cleanupWorker(ctx context.Context) {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		if err := s.store.Cleanup(ctx, s.conf().Limits.NotifyRetentionDays, s.conf().Limits.DeliveryRetentionDays); err != nil {
			s.log.Error().Err(err).Msg("Cleanup failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
