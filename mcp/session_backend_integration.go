// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// FORK: distributed-sessions
// This file contains integration helpers for connecting the SessionBackend
// to the StreamableHTTPHandler.
//
// Design: Any pod can handle any request. No sticky sessions required.
//
// When a request arrives for a session not in local cache:
//  1. Load session data from backend
//  2. Create local session with backend's state
//  3. Handle request normally
//  4. Local session persists for subsequent requests (with timeout)
//
// For SSE (GET requests):
//  - The handling pod becomes the "SSE owner"
//  - It subscribes to backend messages and forwards to the client
//  - Previous owner's subscription ends (ErrSubscriptionSuperseded)

package mcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
)

// hasSessionBackend reports whether the handler has a SessionBackend configured.
func (h *StreamableHTTPHandler) hasSessionBackend() bool {
	return h.opts.SessionBackend != nil
}

// lookupSessionFromBackend looks up a session, checking the local cache first,
// then falling back to the SessionBackend if configured.
//
// Returns:
//   - Local sessionInfo if found in cache
//   - Remote marker if found in backend but not locally
//   - nil if not found anywhere
//   - error only for backend failures
func (h *StreamableHTTPHandler) lookupSessionFromBackend(ctx context.Context, sessionID string) (*sessionInfo, error) {
	// Check local cache first
	h.mu.Lock()
	sessInfo := h.sessions[sessionID]
	h.mu.Unlock()

	if sessInfo != nil {
		return sessInfo, nil
	}

	// If no backend configured, session doesn't exist
	if !h.hasSessionBackend() {
		return nil, nil
	}

	// Check backend
	data, err := h.opts.SessionBackend.Get(ctx, sessionID)
	if err != nil {
		if errors.Is(err, ErrSessionNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("backend lookup failed: %w", err)
	}

	// Session exists in backend but not locally.
	// Return a marker so the caller can create a local session.
	return &sessionInfo{
		remoteSession: &remoteSessionInfo{
			data: data,
		},
	}, nil
}

// remoteSessionInfo holds backend data for a session not yet local.
type remoteSessionInfo struct {
	data *SessionData
}

// createSessionInBackend persists a new session to the backend.
func (h *StreamableHTTPHandler) createSessionInBackend(ctx context.Context, sessInfo *sessionInfo) error {
	if !h.hasSessionBackend() {
		return nil
	}

	data := &SessionData{
		SessionID: sessInfo.transport.SessionID,
		UserID:    sessInfo.userID,
		// State will be updated after initialization completes
	}

	return h.opts.SessionBackend.Update(ctx, data.SessionID, data)
}

// deleteSessionFromBackend removes a session from the backend.
func (h *StreamableHTTPHandler) deleteSessionFromBackend(ctx context.Context, sessionID string) error {
	if !h.hasSessionBackend() {
		return nil
	}
	return h.opts.SessionBackend.Delete(ctx, sessionID)
}

// touchSessionInBackend updates the session's last activity timestamp.
func (h *StreamableHTTPHandler) touchSessionInBackend(ctx context.Context, sessionID string) error {
	if !h.hasSessionBackend() {
		return nil
	}
	return h.opts.SessionBackend.Touch(ctx, sessionID)
}

// subscribeFromBackend subscribes to messages for a session.
func (h *StreamableHTTPHandler) subscribeFromBackend(ctx context.Context, sessionID string, handler MessageHandler) error {
	if !h.hasSessionBackend() {
		return errors.New("no session backend configured")
	}
	return h.opts.SessionBackend.Subscribe(ctx, sessionID, handler)
}

// handleRemoteSession handles a request for a session that exists in the
// backend but not on this pod. It creates a local session from the backend
// state and then processes the request normally.
func (h *StreamableHTTPHandler) handleRemoteSession(w http.ResponseWriter, req *http.Request, remote *remoteSessionInfo) {
	ctx := req.Context()
	sessionID := remote.data.SessionID

	// Security: Verify user ID to prevent session hijacking
	if remote.data.UserID != "" {
		tokenInfo := auth.TokenInfoFromContext(ctx)
		if tokenInfo == nil || tokenInfo.UserID != remote.data.UserID {
			http.Error(w, "session user mismatch", http.StatusForbidden)
			return
		}
	}

	// DELETE can be handled without creating a local session
	if req.Method == http.MethodDelete {
		if err := h.deleteSessionFromBackend(ctx, sessionID); err != nil {
			h.opts.Logger.Error("failed to delete session from backend", "error", err, "sessionID", sessionID)
			http.Error(w, "failed to delete session", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// For GET/POST, create a local session from backend state
	server := h.getServer(req)
	if server == nil {
		http.Error(w, "no server available", http.StatusBadRequest)
		return
	}

	sessInfo, err := h.createLocalSessionFromBackend(ctx, server, remote.data)
	if err != nil {
		h.opts.Logger.Error("failed to create local session", "error", err, "sessionID", sessionID)
		http.Error(w, "failed to restore session", http.StatusInternalServerError)
		return
	}

	// Touch backend to extend TTL
	if err := h.touchSessionInBackend(ctx, sessionID); err != nil {
		h.opts.Logger.Warn("failed to touch session in backend", "error", err, "sessionID", sessionID)
	}

	// Now handle the request using the local session
	if req.Method == http.MethodPost {
		sessInfo.startPOST()
		defer sessInfo.endPOST()
	}
	sessInfo.transport.ServeHTTP(w, req)
}

// createLocalSessionFromBackend creates local runtime state for a session
// that exists in the backend. This allows any pod to handle requests for
// any session.
func (h *StreamableHTTPHandler) createLocalSessionFromBackend(ctx context.Context, server *Server, data *SessionData) (*sessionInfo, error) {
	sessionID := data.SessionID

	// Double-check local cache (race condition protection)
	h.mu.Lock()
	if existing := h.sessions[sessionID]; existing != nil {
		h.mu.Unlock()
		return existing, nil
	}
	h.mu.Unlock()

	// Create transport with the existing session ID
	transport := &StreamableServerTransport{
		SessionID:    sessionID,
		Stateless:    h.opts.Stateless,
		EventStore:   h.opts.EventStore,
		jsonResponse: h.opts.JSONResponse,
		logger:       h.opts.Logger,
	}

	// Set up message routing: when SSE starts, subscribe to backend messages
	transport.OnSSEStart = func(ctx context.Context, writer func(data []byte) error, closeSSE func()) {
		// This callback blocks - it runs in a goroutine spawned by the caller.
		err := h.subscribeFromBackend(ctx, sessionID, func(msgCtx context.Context, msg []byte) error {
			return writer(msg)
		})
		if err != nil && ctx.Err() == nil {
			h.opts.Logger.Error("backend subscription ended", "error", err, "sessionID", sessionID)
			// If subscription was superseded, close the SSE stream
			if errors.Is(err, ErrSubscriptionSuperseded) {
				closeSSE()
			}
		}
	}

	// Set up state persistence: when state changes, persist to backend
	transport.OnStateChange = func(ctx context.Context, state ServerSessionState) error {
		// This callback blocks - it runs in a goroutine spawned by the caller.
		// Use a timeout derived from the request context.
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return h.updateSessionStateInBackend(ctx, sessionID, &state)
	}

	// Set up message routing: route messages when not SSE owner
	transport.OnPublish = func(ctx context.Context, sid string, msg []byte) error {
		return h.opts.SessionBackend.Publish(ctx, sid, msg)
	}

	// Use backend state if available, otherwise create minimal initialized state
	var state *ServerSessionState
	if data.State != nil {
		state = data.State
	} else {
		// Session exists but state wasn't stored - create minimal state
		// This happens if the session was created but Initialize hasn't completed
		state = &ServerSessionState{}
	}

	// Connect with pre-initialized state
	connectOpts := &ServerSessionOptions{
		State: state,
		onClose: func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			if info, ok := h.sessions[sessionID]; ok {
				info.stopTimer()
				delete(h.sessions, sessionID)
				if h.onTransportDeletion != nil {
					h.onTransportDeletion(sessionID)
				}
				// Note: We don't delete from backend here - the session might
				// be accessed by another pod. Let TTL or explicit DELETE handle it.
			}
		},
	}

	session, err := server.Connect(ctx, transport, connectOpts)
	if err != nil {
		return nil, fmt.Errorf("server connect failed: %w", err)
	}

	sessInfo := &sessionInfo{
		session:   session,
		transport: transport,
		userID:    data.UserID,
	}

	// Set up timeout if configured
	if h.opts.SessionTimeout > 0 {
		sessInfo.timeout = h.opts.SessionTimeout
		sessInfo.timer = time.AfterFunc(sessInfo.timeout, func() {
			sessInfo.session.Close()
		})
	}

	// Store in local cache
	h.mu.Lock()
	// Final race check
	if existing := h.sessions[sessionID]; existing != nil {
		h.mu.Unlock()
		session.Close() // Clean up the one we just created
		return existing, nil
	}
	h.sessions[sessionID] = sessInfo
	h.mu.Unlock()

	h.opts.Logger.Debug("created local session from backend", "sessionID", sessionID)
	return sessInfo, nil
}

// updateSessionStateInBackend persists session state changes to the backend.
// This should be called when session state changes (e.g., after Initialize).
func (h *StreamableHTTPHandler) updateSessionStateInBackend(ctx context.Context, sessionID string, state *ServerSessionState) error {
	if !h.hasSessionBackend() {
		return nil
	}

	// Get current data
	data, err := h.opts.SessionBackend.Get(ctx, sessionID)
	if err != nil {
		return err
	}

	// Update state
	data.State = state
	return h.opts.SessionBackend.Update(ctx, sessionID, data)
}
