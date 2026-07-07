// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// FORK: distributed-sessions
// Tests that the initialize state transition is persisted to the SessionBackend
// synchronously (before the initialize response is flushed), while all other
// state changes remain asynchronous.

package mcp

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestStreamableServerConn_InitializeStateChangeIsSynchronous verifies the
// branching in sessionUpdated: the initialize transition (InitializeParams set,
// InitializedParams still nil) runs onStateChange inline on the calling
// goroutine, while every other transition is dispatched in its own goroutine.
func TestStreamableServerConn_InitializeStateChangeIsSynchronous(t *testing.T) {
	t.Run("initialize transition runs inline", func(t *testing.T) {
		var callbackReturned bool
		conn := &streamableServerConn{
			sessionID: "test",
			logger:    slog.Default(),
			onStateChange: func(ctx context.Context, state ServerSessionState) error {
				callbackReturned = true
				return nil
			},
		}
		conn.sessionUpdated(context.Background(), ServerSessionState{
			InitializeParams: &InitializeParams{},
		})
		// If the callback ran inline, it has already completed by the time
		// sessionUpdated returns. Reading callbackReturned here is race-free only
		// because the write happened on this same goroutine.
		if !callbackReturned {
			t.Fatal("initialize state change did not run synchronously")
		}
	})

	t.Run("post-initialize transition runs in goroutine", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		conn := &streamableServerConn{
			sessionID: "test",
			logger:    slog.Default(),
			onStateChange: func(ctx context.Context, state ServerSessionState) error {
				close(started)
				<-release
				return nil
			},
		}
		// InitializedParams != nil marks a post-initialize state change (e.g. the
		// initialized notification or a log-level update).
		conn.sessionUpdated(context.Background(), ServerSessionState{
			InitializeParams:  &InitializeParams{},
			InitializedParams: &InitializedParams{},
		})
		// sessionUpdated must have returned without waiting for the (blocked)
		// callback, proving it dispatched to a goroutine.
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("callback goroutine never started")
		}
		close(release)
	})
}

// gatingBackend blocks the initialize-transition Update until released, so a test
// can observe whether the initialize HTTP response is flushed before the persist
// completes. All other operations pass through to the embedded memory backend.
type gatingBackend struct {
	*MemorySessionBackend

	entered     chan struct{} // closed once the initialize Update is entered
	release     chan struct{} // test closes this to unblock the initialize Update
	enteredOnce sync.Once
}

func (b *gatingBackend) Update(ctx context.Context, id string, data *SessionData) error {
	if data.State != nil && data.State.InitializeParams != nil && data.State.InitializedParams == nil {
		b.enteredOnce.Do(func() { close(b.entered) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.MemorySessionBackend.Update(ctx, id, data)
}

// TestStreamable_InitializeResponseGatedOnBackendPersist proves the initialize
// response is not delivered to the client until the SessionBackend Update that
// persists InitializeParams has returned. This is the ack-before-durable race:
// without the synchronous persist, a second pod handling the client's follow-up
// request would reconstruct a session whose InitializeParams is still nil and
// reject it as "invalid during session initialization".
func TestStreamable_InitializeResponseGatedOnBackendPersist(t *testing.T) {
	backend := &gatingBackend{
		MemorySessionBackend: NewMemorySessionBackend(),
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
	}

	server := NewServer(testImpl, nil)
	handler := NewStreamableHTTPHandler(
		func(*http.Request) *Server { return server },
		&StreamableHTTPOptions{SessionBackend: backend, Logger: slog.Default()},
	)
	httpSrv := httptest.NewServer(handler)
	defer httpSrv.Close()
	defer handler.closeAll()

	// respStatus receives the initialize response status once the full body has
	// been read (i.e. once the InitializeResult has actually been delivered).
	respStatus := make(chan int, 1)
	go func() {
		body := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":` +
			`{"protocolVersion":"` + latestProtocolVersion + `","capabilities":{},` +
			`"clientInfo":{"name":"test","version":"v1"}}}`)
		req, err := http.NewRequest(http.MethodPost, httpSrv.URL, bytes.NewReader(body))
		if err != nil {
			respStatus <- -1
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			respStatus <- -1
			return
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		respStatus <- resp.StatusCode
	}()

	// Wait until the request goroutine is blocked inside the initialize Update.
	select {
	case <-backend.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("initialize Update was never entered")
	}

	// While the persist is blocked, the initialize response must not have been
	// delivered. If it has, the server acked before the session was durable.
	select {
	case code := <-respStatus:
		t.Fatalf("initialize response was delivered (status %d) before the backend persist completed", code)
	case <-time.After(150 * time.Millisecond):
		// Expected: the response is still gated on the persist.
	}

	// Release the persist; the response must now arrive.
	close(backend.release)
	select {
	case code := <-respStatus:
		if code != http.StatusOK {
			t.Fatalf("initialize returned status %d, want %d", code, http.StatusOK)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initialize response never arrived after releasing the backend persist")
	}
}
