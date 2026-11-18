// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestDistributed_IdleCloseKeepsBackendRecord verifies that the creating pod's
// idle timeout does not delete the backend record while another pod is still
// serving the session.
func TestDistributed_IdleCloseKeepsBackendRecord(t *testing.T) {
	ctx := context.Background()
	cluster := NewTestCluster(t, 2)
	defer cluster.Close()

	podA := cluster.Pod("A")
	podA.handler.opts.SessionTimeout = 100 * time.Millisecond
	var mu sync.Mutex
	var evicted []string
	podA.handler.opts.OnSessionEvicted = func(sessionID string) {
		mu.Lock()
		defer mu.Unlock()
		evicted = append(evicted, sessionID)
	}

	session, sessionID := ConnectTestClient(ctx, t, podA, nil)
	defer session.Close()

	// Pod B serves the session while Pod A sees no traffic.
	if _, err := CallToolOnPod(ctx, t, cluster.Pod("B"), sessionID, "echo", map[string]any{"msg": "hi"}); err != nil {
		t.Fatalf("CallToolOnPod(B) failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for podA.HasLocalSession(sessionID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if podA.HasLocalSession(sessionID) {
		t.Fatal("Pod A should have closed the idle session")
	}

	if _, err := cluster.Backend().Get(ctx, sessionID); err != nil {
		t.Fatalf("backend record should survive Pod A's idle close, got: %v", err)
	}
	if _, err := CallToolOnPod(ctx, t, cluster.Pod("B"), sessionID, "echo", map[string]any{"msg": "again"}); err != nil {
		t.Fatalf("CallToolOnPod(B) after Pod A idle close failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(evicted, sessionID) {
		t.Fatalf("expected OnSessionEvicted(%q) on Pod A, got %v", sessionID, evicted)
	}
}

// TestDistributed_DeleteRestoredSessionDeletesBackend verifies that a DELETE
// served by a pod holding a restored copy removes the backend record.
func TestDistributed_DeleteRestoredSessionDeletesBackend(t *testing.T) {
	ctx := context.Background()
	cluster := NewTestCluster(t, 2)
	defer cluster.Close()

	session, sessionID := ConnectTestClient(ctx, t, cluster.Pod("A"), nil)
	defer session.Close()

	if _, err := CallToolOnPod(ctx, t, cluster.Pod("B"), sessionID, "echo", map[string]any{"msg": "hi"}); err != nil {
		t.Fatalf("CallToolOnPod(B) failed: %v", err)
	}
	if !cluster.Pod("B").HasLocalSession(sessionID) {
		t.Fatal("session should be local on Pod B after restore")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, cluster.Pod("B").URL, nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set(sessionIDHeader, sessionID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	if _, err := cluster.Backend().Get(ctx, sessionID); err != ErrSessionNotFound {
		t.Fatalf("expected ErrSessionNotFound after DELETE, got: %v", err)
	}
}
