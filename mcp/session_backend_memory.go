// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// FORK: distributed-sessions
// In-memory implementation of SessionBackend for development and testing.

package mcp

import (
	"context"
	"sync"
)

// MemorySessionBackend is an in-memory implementation of SessionBackend.
//
// This implementation is suitable for:
//   - Development and testing
//   - Single-replica deployments
//   - Prototyping before implementing a production backend
//
// Limitations:
//   - Data is not persisted across restarts
//   - Does not work across multiple processes/pods
//   - No TTL enforcement (Touch is a no-op)
//   - Messages are lost if no subscriber is active (no persistence)
//
// For production multi-replica deployments, implement SessionBackend
// using Redis, PostgreSQL, or another distributed data store.
type MemorySessionBackend struct {
	mu       sync.RWMutex
	sessions map[string]*SessionData
	subs     map[string]*subscription
}

// subscription tracks the active subscriber for a session.
type subscription struct {
	ch     chan []byte
	cancel context.CancelFunc
}

// NewMemorySessionBackend creates a new in-memory session backend.
func NewMemorySessionBackend() *MemorySessionBackend {
	return &MemorySessionBackend{
		sessions: make(map[string]*SessionData),
		subs:     make(map[string]*subscription),
	}
}

// Create implements SessionBackend.Create.
func (m *MemorySessionBackend) Create(ctx context.Context, data *SessionData) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := randText() // Use the SDK's random text generator
	data.SessionID = id
	m.sessions[id] = data
	return id, nil
}

// Get implements SessionBackend.Get.
func (m *MemorySessionBackend) Get(ctx context.Context, id string) (*SessionData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}
	// Return a copy to prevent mutation
	copy := *data
	if data.State != nil {
		stateCopy := *data.State
		copy.State = &stateCopy
	}
	return &copy, nil
}

// Update implements SessionBackend.Update.
func (m *MemorySessionBackend) Update(ctx context.Context, id string, data *SessionData) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.sessions[id]; !ok {
		return ErrSessionNotFound
	}
	m.sessions[id] = data
	return nil
}

// Delete implements SessionBackend.Delete.
func (m *MemorySessionBackend) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.sessions, id)

	// Close subscriber channel if exists
	if sub, ok := m.subs[id]; ok {
		close(sub.ch)
		delete(m.subs, id)
	}

	return nil
}

// Touch implements SessionBackend.Touch.
// In this implementation, Touch is a no-op since there's no TTL.
func (m *MemorySessionBackend) Touch(ctx context.Context, id string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.sessions[id]; !ok {
		return ErrSessionNotFound
	}
	return nil
}

// Publish implements SessionBackend.Publish.
func (m *MemorySessionBackend) Publish(ctx context.Context, sessionID string, msg []byte) error {
	m.mu.RLock()
	sub := m.subs[sessionID]
	m.mu.RUnlock()

	if sub == nil {
		// No subscriber - in production, you might queue the message
		return nil
	}

	// Non-blocking send
	select {
	case sub.ch <- msg:
		return nil
	default:
		// Channel full - drop message (in production, handle backpressure)
		return nil
	}
}

// Subscribe implements SessionBackend.Subscribe.
// Only one subscriber is allowed per session. New subscribers supersede old ones.
func (m *MemorySessionBackend) Subscribe(ctx context.Context, sessionID string, handler MessageHandler) error {
	ch := make(chan []byte, 100)

	// Create cancellable context for this subscription
	subCtx, subCancel := context.WithCancel(ctx)

	m.mu.Lock()
	// Supersede existing subscriber
	if existing, ok := m.subs[sessionID]; ok {
		close(existing.ch) // Will cause existing subscriber to return ErrSubscriptionSuperseded
	}
	m.subs[sessionID] = &subscription{ch: ch, cancel: subCancel}
	m.mu.Unlock()

	defer func() {
		subCancel()
		m.mu.Lock()
		// Only remove if we're still the active subscriber
		if sub, ok := m.subs[sessionID]; ok && sub.ch == ch {
			delete(m.subs, sessionID)
		}
		m.mu.Unlock()
	}()

	for {
		select {
		case <-subCtx.Done():
			return subCtx.Err()
		case msg, ok := <-ch:
			if !ok {
				// Channel closed - either session deleted or superseded
				m.mu.RLock()
				_, sessionExists := m.sessions[sessionID]
				m.mu.RUnlock()
				if !sessionExists {
					return ErrSessionNotFound
				}
				return ErrSubscriptionSuperseded
			}
			if err := handler(subCtx, msg); err != nil {
				return err
			}
		}
	}
}

// Verify MemorySessionBackend implements SessionBackend
var _ SessionBackend = (*MemorySessionBackend)(nil)
