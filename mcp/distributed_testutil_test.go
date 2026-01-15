// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// FORK: distributed-sessions
// Test utilities for distributed session testing.

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestCluster manages multiple simulated pods sharing a SessionBackend.
type TestCluster struct {
	t       *testing.T
	pods    map[string]*TestPod
	backend *InstrumentedBackend
	server  *Server // Shared server instance
}

// TestPod represents a single server replica in the test cluster.
type TestPod struct {
	Name    string
	handler *StreamableHTTPHandler
	httpSrv *httptest.Server
	URL     string
}

// NewTestCluster creates a test cluster with the specified number of pods.
func NewTestCluster(t *testing.T, numPods int) *TestCluster {
	t.Helper()

	backend := NewInstrumentedBackend()

	// Create shared server with test tools
	server := NewServer(testImpl, nil)
	AddTool(server, &Tool{Name: "echo", Description: "echo input"}, echoTool)
	AddTool(server, &Tool{Name: "getState", Description: "get session state"}, getStateTool)

	cluster := &TestCluster{
		t:       t,
		pods:    make(map[string]*TestPod),
		backend: backend,
		server:  server,
	}

	// Create pods A, B, C, ...
	for i := 0; i < numPods; i++ {
		name := string(rune('A' + i))
		cluster.createPod(name)
	}

	return cluster
}

func (c *TestCluster) createPod(name string) *TestPod {
	handler := NewStreamableHTTPHandler(
		func(req *http.Request) *Server { return c.server },
		&StreamableHTTPOptions{
			SessionBackend: c.backend,
			Logger:         slog.Default(),
		},
	)

	httpSrv := httptest.NewServer(handler)

	pod := &TestPod{
		Name:    name,
		handler: handler,
		httpSrv: httpSrv,
		URL:     httpSrv.URL,
	}
	c.pods[name] = pod
	return pod
}

// Pod returns the pod with the given name.
func (c *TestCluster) Pod(name string) *TestPod {
	pod, ok := c.pods[name]
	if !ok {
		c.t.Fatalf("pod %q not found", name)
	}
	return pod
}

// Backend returns the instrumented backend for verification.
func (c *TestCluster) Backend() *InstrumentedBackend {
	return c.backend
}

// Close shuts down all pods.
func (c *TestCluster) Close() {
	for _, pod := range c.pods {
		pod.httpSrv.Close()
		pod.handler.closeAll()
	}
}

// ClearLocalSession removes a session from a pod's local cache.
// This simulates a pod restart.
func (p *TestPod) ClearLocalSession(sessionID string) {
	p.handler.mu.Lock()
	defer p.handler.mu.Unlock()
	delete(p.handler.sessions, sessionID)
}

// HasLocalSession checks if a session is in the pod's local cache.
func (p *TestPod) HasLocalSession(sessionID string) bool {
	p.handler.mu.Lock()
	defer p.handler.mu.Unlock()
	_, ok := p.handler.sessions[sessionID]
	return ok
}

// Tool handlers for tests
func echoTool(ctx context.Context, req *CallToolRequest, args struct {
	Msg string `json:"msg"`
}) (*CallToolResult, any, error) {
	return &CallToolResult{
		Content: []Content{&TextContent{Text: args.Msg}},
	}, nil, nil
}

func getStateTool(ctx context.Context, req *CallToolRequest, args any) (*CallToolResult, any, error) {
	// Return a simple response - we can't easily access session state from here
	return &CallToolResult{
		Content: []Content{&TextContent{Text: "ok"}},
	}, nil, nil
}

// InstrumentedBackend wraps MemorySessionBackend with instrumentation and
// failure injection for testing error handling paths.
type InstrumentedBackend struct {
	*MemorySessionBackend

	mu         sync.Mutex
	ops        []BackendOp
	subCount   map[string]int // sessionID -> active subscriber count
	superseded map[string]chan struct{}

	// Failure injection
	failGet       error // If set, Get() returns this error
	failUpdate    error // If set, Update() returns this error
	failTouch     error // If set, Touch() returns this error
	failPublish   error // If set, Publish() returns this error
	failSubscribe error // If set, Subscribe() returns this error immediately

	// Conditional failures
	failGetAfterN    int // Fail Get after N successful calls (-1 to disable)
	failUpdateAfterN int // Fail Update after N successful calls (-1 to disable)
	getCount         int
	updateCount      int

	// Latency injection
	getLatency    time.Duration
	updateLatency time.Duration
}

// BackendOp records a backend operation for verification.
type BackendOp struct {
	Op        string
	SessionID string
	Error     error
	Timestamp time.Time
}

// NewInstrumentedBackend creates a new instrumented backend.
func NewInstrumentedBackend() *InstrumentedBackend {
	return &InstrumentedBackend{
		MemorySessionBackend: NewMemorySessionBackend(),
		subCount:             make(map[string]int),
		superseded:           make(map[string]chan struct{}),
		failGetAfterN:        -1,
		failUpdateAfterN:     -1,
	}
}

func (b *InstrumentedBackend) recordOp(op, sessionID string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ops = append(b.ops, BackendOp{Op: op, SessionID: sessionID, Error: err, Timestamp: time.Now()})
}

// SetFailGet configures Get to fail with the given error.
func (b *InstrumentedBackend) SetFailGet(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failGet = err
}

// SetFailUpdate configures Update to fail with the given error.
func (b *InstrumentedBackend) SetFailUpdate(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failUpdate = err
}

// SetFailTouch configures Touch to fail with the given error.
func (b *InstrumentedBackend) SetFailTouch(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failTouch = err
}

// SetFailPublish configures Publish to fail with the given error.
func (b *InstrumentedBackend) SetFailPublish(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failPublish = err
}

// SetFailSubscribe configures Subscribe to fail immediately with the given error.
func (b *InstrumentedBackend) SetFailSubscribe(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failSubscribe = err
}

// SetFailGetAfterN configures Get to fail after N successful calls.
func (b *InstrumentedBackend) SetFailGetAfterN(n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failGetAfterN = n
	b.failGet = err
	b.getCount = 0
}

// SetFailUpdateAfterN configures Update to fail after N successful calls.
func (b *InstrumentedBackend) SetFailUpdateAfterN(n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failUpdateAfterN = n
	b.failUpdate = err
	b.updateCount = 0
}

// SetGetLatency configures latency for Get operations.
func (b *InstrumentedBackend) SetGetLatency(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.getLatency = d
}

// SetUpdateLatency configures latency for Update operations.
func (b *InstrumentedBackend) SetUpdateLatency(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.updateLatency = d
}

// ClearFailures removes all failure injection.
func (b *InstrumentedBackend) ClearFailures() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failGet = nil
	b.failUpdate = nil
	b.failTouch = nil
	b.failPublish = nil
	b.failSubscribe = nil
	b.failGetAfterN = -1
	b.failUpdateAfterN = -1
	b.getLatency = 0
	b.updateLatency = 0
}

func (b *InstrumentedBackend) Create(ctx context.Context, data *SessionData) (string, error) {
	id, err := b.MemorySessionBackend.Create(ctx, data)
	b.recordOp("Create", id, err)
	return id, err
}

func (b *InstrumentedBackend) Get(ctx context.Context, id string) (*SessionData, error) {
	b.mu.Lock()
	latency := b.getLatency
	failErr := b.failGet
	failAfterN := b.failGetAfterN
	b.getCount++
	count := b.getCount
	b.mu.Unlock()

	// Apply latency
	if latency > 0 {
		time.Sleep(latency)
	}

	// Check for conditional failure
	if failAfterN >= 0 && count > failAfterN && failErr != nil {
		b.recordOp("Get", id, failErr)
		return nil, failErr
	}

	// Check for unconditional failure
	if failAfterN < 0 && failErr != nil {
		b.recordOp("Get", id, failErr)
		return nil, failErr
	}

	data, err := b.MemorySessionBackend.Get(ctx, id)
	b.recordOp("Get", id, err)
	return data, err
}

func (b *InstrumentedBackend) Update(ctx context.Context, id string, data *SessionData) error {
	b.mu.Lock()
	latency := b.updateLatency
	failErr := b.failUpdate
	failAfterN := b.failUpdateAfterN
	b.updateCount++
	count := b.updateCount
	b.mu.Unlock()

	// Apply latency
	if latency > 0 {
		time.Sleep(latency)
	}

	// Check for conditional failure
	if failAfterN >= 0 && count > failAfterN && failErr != nil {
		b.recordOp("Update", id, failErr)
		return failErr
	}

	// Check for unconditional failure
	if failAfterN < 0 && failErr != nil {
		b.recordOp("Update", id, failErr)
		return failErr
	}

	err := b.MemorySessionBackend.Update(ctx, id, data)
	b.recordOp("Update", id, err)
	return err
}

func (b *InstrumentedBackend) Delete(ctx context.Context, id string) error {
	err := b.MemorySessionBackend.Delete(ctx, id)
	b.recordOp("Delete", id, err)
	return err
}

func (b *InstrumentedBackend) Touch(ctx context.Context, id string) error {
	b.mu.Lock()
	failErr := b.failTouch
	b.mu.Unlock()

	if failErr != nil {
		b.recordOp("Touch", id, failErr)
		return failErr
	}

	err := b.MemorySessionBackend.Touch(ctx, id)
	b.recordOp("Touch", id, err)
	return err
}

func (b *InstrumentedBackend) Publish(ctx context.Context, sessionID string, msg []byte) error {
	b.mu.Lock()
	failErr := b.failPublish
	b.mu.Unlock()

	if failErr != nil {
		b.recordOp("Publish", sessionID, failErr)
		return failErr
	}

	err := b.MemorySessionBackend.Publish(ctx, sessionID, msg)
	b.recordOp("Publish", sessionID, err)
	return err
}

func (b *InstrumentedBackend) Subscribe(ctx context.Context, sessionID string, handler MessageHandler) error {
	b.mu.Lock()
	failErr := b.failSubscribe
	b.mu.Unlock()

	if failErr != nil {
		b.recordOp("Subscribe", sessionID, failErr)
		return failErr
	}
	// Track subscriber count
	b.mu.Lock()
	b.subCount[sessionID]++
	count := b.subCount[sessionID]

	// If there's already a subscriber, signal it to close
	if count > 1 {
		if ch, ok := b.superseded[sessionID]; ok {
			close(ch)
		}
	}

	// Create our own supersede channel
	supersedeCh := make(chan struct{})
	b.superseded[sessionID] = supersedeCh
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.subCount[sessionID]--
		if b.superseded[sessionID] == supersedeCh {
			delete(b.superseded, sessionID)
		}
		b.mu.Unlock()
	}()

	// Wrap the context to also listen for supersede signal
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-supersedeCh:
			cancel() // Will cause Subscribe to return
		case <-ctx.Done():
		}
	}()

	err := b.MemorySessionBackend.Subscribe(ctx, sessionID, handler)

	// Check if we were superseded
	select {
	case <-supersedeCh:
		b.recordOp("Subscribe", sessionID, ErrSubscriptionSuperseded)
		return ErrSubscriptionSuperseded
	default:
		b.recordOp("Subscribe", sessionID, err)
		return err
	}
}

// GetSubscriberCount returns the number of active subscribers for a session.
func (b *InstrumentedBackend) GetSubscriberCount(sessionID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subCount[sessionID]
}

// AssertSingleSubscriber verifies only one subscriber exists for a session.
func (b *InstrumentedBackend) AssertSingleSubscriber(t *testing.T, sessionID string) {
	t.Helper()
	count := b.GetSubscriberCount(sessionID)
	if count > 1 {
		t.Errorf("expected at most 1 subscriber for session %q, got %d", sessionID, count)
	}
}

// GetOps returns all recorded operations.
func (b *InstrumentedBackend) GetOps() []BackendOp {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]BackendOp(nil), b.ops...)
}

// ClearOps clears the operation history.
func (b *InstrumentedBackend) ClearOps() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ops = nil
}

// TestClientOptions configures a test client.
type TestClientOptions struct {
	UserID string
}

// ConnectTestClient creates and connects a test client to a pod.
func ConnectTestClient(ctx context.Context, t *testing.T, pod *TestPod, opts *TestClientOptions) (*ClientSession, string) {
	t.Helper()

	transport := &StreamableClientTransport{
		Endpoint: pod.URL,
	}

	client := NewClient(testImpl, nil)

	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatalf("client.Connect() failed: %v", err)
	}

	sessionID := session.ID()
	if sessionID == "" {
		t.Fatal("empty session ID")
	}

	return session, sessionID
}

// SSEStream represents an open SSE connection for testing.
// It allows direct control over SSE stream lifecycle and event monitoring.
type SSEStream struct {
	t      *testing.T
	resp   *http.Response
	events chan Event
	errors chan error
	done   chan struct{}
	closed bool
	mu     sync.Mutex
	cancel context.CancelFunc
}

// OpenSSEStream opens an SSE stream to a pod for the given session.
// This simulates a client establishing the standalone SSE GET connection.
func OpenSSEStream(ctx context.Context, t *testing.T, pod *TestPod, sessionID string) *SSEStream {
	t.Helper()

	ctx, cancel := context.WithCancel(ctx)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pod.URL, nil)
	if err != nil {
		cancel()
		t.Fatalf("failed to create SSE request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(sessionIDHeader, sessionID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("SSE request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		t.Fatalf("SSE request got status %d: %s", resp.StatusCode, string(body))
	}

	stream := &SSEStream{
		t:      t,
		resp:   resp,
		events: make(chan Event, 100),
		errors: make(chan error, 1),
		done:   make(chan struct{}),
		cancel: cancel,
	}

	// Start goroutine to read events
	go stream.readEvents()

	return stream
}

// readEvents reads SSE events from the response body and sends them to the events channel.
func (s *SSEStream) readEvents() {
	defer func() {
		s.mu.Lock()
		if !s.closed {
			s.closed = true
			close(s.done)
		}
		s.mu.Unlock()
		s.resp.Body.Close()
	}()

	for evt, err := range scanEvents(s.resp.Body) {
		if err != nil {
			select {
			case s.errors <- err:
			default:
			}
			return
		}
		select {
		case s.events <- evt:
		case <-s.done:
			return
		}
	}
}

// NextEvent waits for and returns the next SSE event, or an error if timeout.
func (s *SSEStream) NextEvent(timeout time.Duration) (*Event, error) {
	select {
	case evt := <-s.events:
		return &evt, nil
	case err := <-s.errors:
		return nil, err
	case <-s.done:
		return nil, io.EOF
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for SSE event")
	}
}

// IsClosed returns true if the SSE stream has been closed.
func (s *SSEStream) IsClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// WaitClosed waits for the stream to close with a timeout.
func (s *SSEStream) WaitClosed(timeout time.Duration) bool {
	select {
	case <-s.done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Close closes the SSE stream.
func (s *SSEStream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.cancel()
		close(s.done)
	}
}

// CallToolOnPod sends a tool call to a specific pod using an existing session ID.
// This uses raw HTTP requests to send a request with an existing session ID.
func CallToolOnPod(ctx context.Context, t *testing.T, pod *TestPod, sessionID string, toolName string, args map[string]any) (*CallToolResult, error) {
	t.Helper()

	// Build the JSON-RPC request
	callReq := &CallToolParams{
		Name:      toolName,
		Arguments: args,
	}
	paramsData, err := json.Marshal(callReq)
	if err != nil {
		return nil, fmt.Errorf("marshal params: %w", err)
	}

	rpcReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params":  json.RawMessage(paramsData),
	}
	reqData, err := json.Marshal(rpcReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	// Make HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, pod.URL, bytes.NewReader(reqData))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")
	httpReq.Header.Set(sessionIDHeader, sessionID)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response - handle both JSON and SSE formats
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	// Check if this is SSE format (starts with "event:")
	bodyStr := string(body)
	if strings.HasPrefix(bodyStr, "event:") {
		// Extract the data from SSE format
		// Format: "event: message\ndata: {...}\n\n"
		lines := strings.Split(bodyStr, "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "data: ") {
				body = []byte(strings.TrimPrefix(line, "data: "))
				break
			}
		}
	}

	var rpcResp struct {
		Result *CallToolResult `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w (body: %s)", err, string(body))
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}
