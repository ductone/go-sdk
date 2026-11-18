// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/internal/jsonrpc2"
)

// countingBackend counts Create calls on top of the in-memory backend.
type countingBackend struct {
	*MemorySessionBackend
	creates atomic.Int32
}

func (b *countingBackend) Create(ctx context.Context, data *SessionData) (string, error) {
	b.creates.Add(1)
	return b.MemorySessionBackend.Create(ctx, data)
}

func newVersionRejectServer(t *testing.T) (*httptest.Server, *atomic.Int32, *countingBackend) {
	t.Helper()
	var getServerCalls atomic.Int32
	server := NewServer(testImpl, nil)
	backend := &countingBackend{MemorySessionBackend: NewMemorySessionBackend()}
	handler := NewStreamableHTTPHandler(func(*http.Request) *Server {
		getServerCalls.Add(1)
		return server
	}, &StreamableHTTPOptions{SessionBackend: backend})
	t.Cleanup(handler.closeAll)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	return ts, &getServerCalls, backend
}

func postSessionless(t *testing.T, url, protocolVersion string, msg jsonrpc2.Message) *http.Response {
	t.Helper()
	body, err := jsonrpc2.EncodeMessage(msg)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")
	if protocolVersion != "" {
		httpReq.Header.Set(protocolVersionHeader, protocolVersion)
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// TestStatefulSessionlessModernCall_RejectedBeforeSessionCreation verifies
// that a stateful handler refuses a sessionless 2026-07-28 call with the
// UnsupportedProtocolVersion error, without building a Server or creating a
// backend session.
func TestStatefulSessionlessModernCall_RejectedBeforeSessionCreation(t *testing.T) {
	ts, getServerCalls, backend := newVersionRejectServer(t)

	discover := &jsonrpc2.Request{
		ID:     jsonrpc2.Int64ID(7),
		Method: methodDiscover,
		Params: json.RawMessage(`{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}`),
	}
	resp := postSessionless(t, ts.URL, protocolVersion20260728, discover)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	got, _ := io.ReadAll(resp.Body)
	var errResp struct {
		ID    int64 `json:"id"`
		Error struct {
			Code int64                          `json:"code"`
			Data UnsupportedProtocolVersionData `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(got, &errResp); err != nil {
		t.Fatalf("unmarshal response: %v\nbody: %s", err, got)
	}
	if errResp.ID != 7 {
		t.Errorf("id = %d, want 7", errResp.ID)
	}
	if errResp.Error.Code != CodeUnsupportedProtocolVersion {
		t.Errorf("error code = %d, want %d", errResp.Error.Code, CodeUnsupportedProtocolVersion)
	}
	if errResp.Error.Data.Requested != protocolVersion20260728 {
		t.Errorf("requested = %q, want %q", errResp.Error.Data.Requested, protocolVersion20260728)
	}
	wantSupported := []string{protocolVersion20251125, protocolVersion20250618, protocolVersion20250326, protocolVersion20241105}
	if !slices.Equal(errResp.Error.Data.Supported, wantSupported) {
		t.Errorf("supported = %v, want %v", errResp.Error.Data.Supported, wantSupported)
	}
	if n := getServerCalls.Load(); n != 0 {
		t.Errorf("getServer called %d times, want 0", n)
	}
	if n := backend.creates.Load(); n != 0 {
		t.Errorf("backend Create called %d times, want 0", n)
	}
}

// TestStatefulSessionlessLegacyInitialize_StillCreatesSession verifies that
// the early rejection leaves a legacy initialize untouched.
func TestStatefulSessionlessLegacyInitialize_StillCreatesSession(t *testing.T) {
	ts, getServerCalls, backend := newVersionRejectServer(t)

	resp := postSessionless(t, ts.URL, "", req(1, methodInitialize, &InitializeParams{
		ProtocolVersion: protocolVersion20251125,
		ClientInfo:      &Implementation{Name: "test", Version: "v1"},
	}))
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d\nbody: %s", resp.StatusCode, http.StatusOK, body)
	}
	if resp.Header.Get(sessionIDHeader) == "" {
		t.Error("missing session ID header")
	}
	if n := getServerCalls.Load(); n != 1 {
		t.Errorf("getServer called %d times, want 1", n)
	}
	if n := backend.creates.Load(); n != 1 {
		t.Errorf("backend Create called %d times, want 1", n)
	}
}
