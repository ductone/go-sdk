// FORK: session-close-listen-deadlock (TEMPORARY)
//
// Tests for the carried fix tracked in ServerSession.trackListen/
// untrackListen/closing and ServerSession.Close (see the FORK comments on
// those). Delete this file, along with the fields/methods it tests, once the
// fork re-syncs to an upstream revision that includes the merged fix for
// modelcontextprotocol/go-sdk#1160 (proposed in #1166).
package mcp

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// TestServerSessionTrackListen_SelfCancelsAfterClose deterministically
// proves the accepted-before-registration race cannot deadlock: a
// subscriptions/listen request whose handler goroutine reaches trackListen
// only after Close has already run (closing already set, listenIDs already
// snapshotted) must be told to self-cancel rather than silently register
// into a slice nobody will ever read again.
func TestServerSessionTrackListen_SelfCancelsAfterClose(t *testing.T) {
	ss := &ServerSession{}
	id, err := jsonrpc.MakeID(float64(1))
	if err != nil {
		t.Fatalf("MakeID: %v", err)
	}

	// Simulate Close's critical section having already run: closing set,
	// listenIDs already drained to nil.
	ss.mu.Lock()
	ss.closing = true
	ss.mu.Unlock()

	if selfCancel := ss.trackListen(id); !selfCancel {
		t.Fatal("a listen request arriving after Close must self-cancel")
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.listenIDs) != 0 {
		t.Fatalf("a self-cancelling request must not be registered, got %v", ss.listenIDs)
	}
}

// TestServerSessionTrackListen_RegistersBeforeClose proves the mirror-image
// ordering: a listen request that registers first is guaranteed to be in
// Close's snapshot, because both critical sections are serialized by the
// same mutex.
func TestServerSessionTrackListen_RegistersBeforeClose(t *testing.T) {
	ss := &ServerSession{}
	id, err := jsonrpc.MakeID(float64(1))
	if err != nil {
		t.Fatalf("MakeID: %v", err)
	}

	if selfCancel := ss.trackListen(id); selfCancel {
		t.Fatal("a listen request registering before Close must not self-cancel")
	}

	// Reproduce Close's own critical section rather than calling the real
	// Close, which requires a live conn.
	ss.mu.Lock()
	ss.closing = true
	listenIDs := ss.listenIDs
	ss.listenIDs = nil
	ss.mu.Unlock()

	if len(listenIDs) != 1 || listenIDs[0] != id {
		t.Fatalf("Close's snapshot must contain the pre-registered ID, got %v", listenIDs)
	}
}

// TestServerSessionUntrackListen_RemovesOnlyItsOwnID proves normal
// completion cleans up after itself under the mutex, so repeated
// subscribe/unsubscribe cycles cannot accumulate stale IDs (a slow leak that
// would otherwise cancel unrelated, already-retired jsonrpc2 request IDs the
// next time Close ran).
func TestServerSessionUntrackListen_RemovesOnlyItsOwnID(t *testing.T) {
	ss := &ServerSession{}
	id1, err := jsonrpc.MakeID(float64(1))
	if err != nil {
		t.Fatalf("MakeID: %v", err)
	}
	id2, err := jsonrpc.MakeID(float64(2))
	if err != nil {
		t.Fatalf("MakeID: %v", err)
	}

	if ss.trackListen(id1) {
		t.Fatal("first trackListen must not self-cancel")
	}
	if ss.trackListen(id2) {
		t.Fatal("second trackListen must not self-cancel")
	}

	ss.untrackListen(id1)

	ss.mu.Lock()
	defer ss.mu.Unlock()
	if want := []jsonrpc.ID{id2}; !reflect.DeepEqual(ss.listenIDs, want) {
		t.Fatalf("listenIDs = %v, want %v", ss.listenIDs, want)
	}
}

// TestServerSessionCloseWithActiveListen is an end-to-end regression test:
// ServerSession.Close must not deadlock when the client has an active
// subscriptions/listen stream open and the server initiates the close (as
// opposed to the client closing its side first, which independently
// unblocks the handler via a transport read error). See
// modelcontextprotocol/go-sdk#1160.
//
// The client's auto-listen (triggered by registering a list-changed
// handler) opens the stream on Connect -- no explicit Subscribe is needed.
// Polling ss.listenIDs directly (rather than a fixed sleep) makes this
// deterministic: it blocks exactly until the async listen handler has
// registered, however long that takes on a loaded machine.
func TestServerSessionCloseWithActiveListen(t *testing.T) {
	ctx := context.Background()
	s := NewServer(&Implementation{Name: "s", Version: "0"}, nil)
	AddTool(s, &Tool{Name: "t"}, sayHi)

	ct, st := NewInMemoryTransports()
	if _, err := s.Connect(ctx, st, nil); err != nil {
		t.Fatalf("server connect: %v", err)
	}
	c := NewClient(&Implementation{Name: "c", Version: "0"}, &ClientOptions{
		ToolListChangedHandler: func(context.Context, *ToolListChangedRequest) {},
	})
	cs, err := c.Connect(ctx, ct, &ClientSessionOptions{ProtocolVersion: protocolVersion20260728})
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cs.Close()

	var ss *ServerSession
	deadline := time.Now().Add(5 * time.Second)
	for {
		for x := range s.Sessions() {
			ss = x
		}
		if ss != nil {
			ss.mu.Lock()
			n := len(ss.listenIDs)
			ss.mu.Unlock()
			if n > 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for auto-listen to register on the server session")
		}
		time.Sleep(5 * time.Millisecond)
	}

	done := make(chan error, 1)
	go func() { done <- ss.Close() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ServerSession.Close deadlocked with an active subscriptions/listen")
	}
}
