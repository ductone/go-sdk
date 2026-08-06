// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// FORK: distributed-sessions
// Tests for the SessionBackend interface and MemorySessionBackend implementation.

package mcp

import (
	"context"
	"testing"
	"time"
)

func TestMemorySessionBackend_CreateAndGet(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx := context.Background()

	// Create a session
	data := &SessionData{
		UserID: "user123",
		State: &ServerSessionState{
			LogLevel: "info",
		},
	}

	id, err := backend.Create(ctx, data)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if id == "" {
		t.Fatal("Create returned empty ID")
	}

	// Get the session
	retrieved, err := backend.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved.SessionID != id {
		t.Errorf("SessionID mismatch: got %q, want %q", retrieved.SessionID, id)
	}
	if retrieved.UserID != "user123" {
		t.Errorf("UserID mismatch: got %q, want %q", retrieved.UserID, "user123")
	}
}

func TestMemorySessionBackend_GetReturnsDefensiveCopy(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx := context.Background()

	// Create a session with state
	data := &SessionData{
		UserID: "user123",
		State:  &ServerSessionState{LogLevel: "info"},
	}
	id, err := backend.Create(ctx, data)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Get and modify
	retrieved, _ := backend.Get(ctx, id)
	retrieved.UserID = "modified"
	retrieved.State.LogLevel = "debug"

	// Get again - should not see modifications
	retrieved2, _ := backend.Get(ctx, id)
	if retrieved2.UserID != "user123" {
		t.Errorf("Get returned mutable reference: UserID was modified")
	}
	if retrieved2.State.LogLevel != "info" {
		t.Errorf("Get returned mutable reference: LogLevel was modified")
	}
}

func TestMemorySessionBackend_GetNotFound(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx := context.Background()

	_, err := backend.Get(ctx, "nonexistent")
	if err != ErrSessionNotFound {
		t.Errorf("Expected ErrSessionNotFound, got: %v", err)
	}
}

func TestMemorySessionBackend_Update(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx := context.Background()

	// Create a session
	data := &SessionData{UserID: "user123"}
	id, err := backend.Create(ctx, data)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update the session
	updated := &SessionData{
		SessionID: id,
		UserID:    "user456",
	}
	if err := backend.Update(ctx, id, updated); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify the update
	retrieved, err := backend.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved.UserID != "user456" {
		t.Errorf("UserID not updated: got %q, want %q", retrieved.UserID, "user456")
	}
}

func TestMemorySessionBackend_UpdateNotFound(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx := context.Background()

	err := backend.Update(ctx, "nonexistent", &SessionData{})
	if err != ErrSessionNotFound {
		t.Errorf("Expected ErrSessionNotFound, got: %v", err)
	}
}

func TestMemorySessionBackend_Delete(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx := context.Background()

	// Create a session
	data := &SessionData{UserID: "user123"}
	id, err := backend.Create(ctx, data)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Delete the session
	if err := backend.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	_, err = backend.Get(ctx, id)
	if err != ErrSessionNotFound {
		t.Errorf("Expected ErrSessionNotFound after delete, got: %v", err)
	}

	// Delete again should be idempotent
	if err := backend.Delete(ctx, id); err != nil {
		t.Errorf("Second delete should be idempotent, got: %v", err)
	}
}

func TestMemorySessionBackend_TouchNotFound(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx := context.Background()

	err := backend.Touch(ctx, "nonexistent")
	if err != ErrSessionNotFound {
		t.Errorf("Expected ErrSessionNotFound, got: %v", err)
	}
}

func TestMemorySessionBackend_PublishSubscribe(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a session
	data := &SessionData{UserID: "user123"}
	id, err := backend.Create(ctx, data)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Channel to collect received messages
	received := make(chan []byte, 10)

	// Start subscriber
	subCtx, subCancel := context.WithCancel(ctx)
	subDone := make(chan error, 1)
	go func() {
		err := backend.Subscribe(subCtx, id, func(ctx context.Context, msg []byte) error {
			received <- msg
			return nil
		})
		subDone <- err
	}()

	// Give subscriber time to start
	time.Sleep(50 * time.Millisecond)

	// Publish some messages
	messages := [][]byte{
		[]byte(`{"jsonrpc":"2.0","method":"test1"}`),
		[]byte(`{"jsonrpc":"2.0","method":"test2"}`),
	}

	for _, msg := range messages {
		if err := backend.Publish(ctx, id, msg); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Verify messages received
	for i, expected := range messages {
		select {
		case got := <-received:
			if string(got) != string(expected) {
				t.Errorf("Message %d mismatch: got %q, want %q", i, string(got), string(expected))
			}
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for message %d", i)
		}
	}

	// Cancel subscription
	subCancel()

	select {
	case err := <-subDone:
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for subscription to end")
	}
}

func TestMemorySessionBackend_SubscribeSessionDeleted(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a session
	data := &SessionData{UserID: "user123"}
	id, err := backend.Create(ctx, data)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Start subscriber
	subDone := make(chan error, 1)
	go func() {
		err := backend.Subscribe(ctx, id, func(ctx context.Context, msg []byte) error {
			return nil
		})
		subDone <- err
	}()

	// Give subscriber time to start
	time.Sleep(50 * time.Millisecond)

	// Delete the session
	if err := backend.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify subscription ends with ErrSessionNotFound
	select {
	case err := <-subDone:
		if err != ErrSessionNotFound {
			t.Errorf("Expected ErrSessionNotFound, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for subscription to end")
	}
}

func TestMemorySessionBackend_SubscribeSuperseded(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a session
	data := &SessionData{UserID: "user123"}
	id, err := backend.Create(ctx, data)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Start first subscriber
	sub1Done := make(chan error, 1)
	go func() {
		err := backend.Subscribe(ctx, id, func(ctx context.Context, msg []byte) error {
			return nil
		})
		sub1Done <- err
	}()

	// Give first subscriber time to start
	time.Sleep(50 * time.Millisecond)

	// Start second subscriber - should supersede first
	sub2Ctx, sub2Cancel := context.WithCancel(ctx)
	sub2Done := make(chan error, 1)
	go func() {
		err := backend.Subscribe(sub2Ctx, id, func(ctx context.Context, msg []byte) error {
			return nil
		})
		sub2Done <- err
	}()

	// First subscriber should be superseded
	select {
	case err := <-sub1Done:
		if err != ErrSubscriptionSuperseded {
			t.Errorf("Expected ErrSubscriptionSuperseded for first subscriber, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for first subscriber to be superseded")
	}

	// Second subscriber should still be active
	select {
	case err := <-sub2Done:
		t.Errorf("Second subscriber ended unexpectedly: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected - subscriber is still running
	}

	// Clean up
	sub2Cancel()
}

func TestMemorySessionBackend_PublishNoSubscriber(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx := context.Background()

	// Create a session
	data := &SessionData{UserID: "user123"}
	id, err := backend.Create(ctx, data)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Publish without subscriber - should not error
	err = backend.Publish(ctx, id, []byte(`{"test":"message"}`))
	if err != nil {
		t.Errorf("Publish without subscriber should not error, got: %v", err)
	}
}

func TestMemorySessionBackend_ConcurrentAccess(t *testing.T) {
	backend := NewMemorySessionBackend()
	ctx := context.Background()

	// Create a session
	data := &SessionData{UserID: "user123"}
	id, err := backend.Create(ctx, data)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Concurrent operations
	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func(n int) {
			// Mix of operations
			switch n % 4 {
			case 0:
				backend.Get(ctx, id)
			case 1:
				backend.Update(ctx, id, &SessionData{SessionID: id, UserID: "concurrent"})
			case 2:
				backend.Touch(ctx, id)
			case 3:
				backend.Publish(ctx, id, []byte(`{"test":"concurrent"}`))
			}
			done <- true
		}(i)
	}

	// Wait for all operations
	for i := 0; i < 100; i++ {
		<-done
	}
}
