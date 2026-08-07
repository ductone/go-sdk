// FORK: distributed-sessions
// Test for the OnSessionEvicted hook.

package mcp

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestDistributed_OnSessionEvicted verifies that closing a session that was
// restored from the backend (a cross-pod takeover) fires OnSessionEvicted on
// the restoring pod, giving embedding applications the signal to release any
// per-pod state mirroring the session — the restore-path onClose deliberately
// does not delete the backend record, so no other signal exists on that path.
func TestDistributed_OnSessionEvicted(t *testing.T) {
	ctx := context.Background()
	cluster := NewTestCluster(t, 2)
	defer cluster.Close()

	var mu sync.Mutex
	var evicted []string
	cluster.Pod("B").handler.opts.OnSessionEvicted = func(sessionID string) {
		mu.Lock()
		defer mu.Unlock()
		evicted = append(evicted, sessionID)
	}

	// Create on Pod A, then force Pod B to restore it from the backend.
	session, sessionID := ConnectTestClient(ctx, t, cluster.Pod("A"), nil)
	defer session.Close()
	time.Sleep(50 * time.Millisecond)
	cluster.Pod("A").ClearLocalSession(sessionID)

	if _, err := CallToolOnPod(ctx, t, cluster.Pod("B"), sessionID, "echo", map[string]any{"msg": "hi"}); err != nil {
		t.Fatalf("CallToolOnPod failed: %v", err)
	}
	if !cluster.Pod("B").HasLocalSession(sessionID) {
		t.Fatal("session should be local on Pod B after takeover")
	}

	// DELETE on Pod B closes the locally-restored session; its onClose must
	// fire the hook.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, cluster.Pod("B").URL, nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	httpReq.Header.Set(sessionIDHeader, sessionID)
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("http request: %v", err)
	}
	resp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(evicted)
		mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(evicted) != 1 || evicted[0] != sessionID {
		t.Fatalf("expected OnSessionEvicted(%q) exactly once, got %v", sessionID, evicted)
	}
}
