package finite

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	maxControlResponseBytes = 64 * 1024
	maxDeliveryPayloadBytes = 1024 * 1024
)

func validateBearerToken(token string) error {
	if len(token) < 32 || len(token) > 4096 || strings.IndexFunc(token, func(r rune) bool { return r <= 0x20 || r == 0x7f }) >= 0 {
		return fmt.Errorf("finite: bearer token must be 32-4096 non-whitespace bytes")
	}
	return nil
}

func bearerAuthorized(req *http.Request, token string) bool {
	const prefix = "Bearer "
	if len(req.Header.Get("Authorization")) <= len(prefix) || !strings.EqualFold(req.Header.Get("Authorization")[:len(prefix)], prefix) {
		return false
	}
	provided := req.Header.Get("Authorization")[len(prefix):]
	return subtle.ConstantTimeCompare([]byte(provided), []byte(token)) == 1
}

func validateLoopbackListener(listener net.Listener) error {
	if listener == nil {
		return fmt.Errorf("finite: loopback listener is required")
	}
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address.IP == nil || !address.IP.IsLoopback() {
		return fmt.Errorf("finite: control and receiver listeners must use a loopback IP")
	}
	return nil
}

func validateLoopbackEndpoint(endpoint string) (*url.URL, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, fmt.Errorf("finite: endpoint must be a plain HTTP loopback URL")
	}
	ip := net.ParseIP(parsed.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return nil, fmt.Errorf("finite: endpoint must use a loopback IP literal")
	}
	return parsed, nil
}

func remoteIsLoopback(req *http.Request) bool {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func serveLoopbackHTTP(ctx context.Context, listener net.Listener, handler http.Handler) error {
	if err := validateLoopbackListener(listener); err != nil {
		return err
	}
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 16 * 1024,
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		case <-done:
		}
	}()
	err := server.Serve(listener)
	close(done)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func decodeRequestJSON(w http.ResponseWriter, req *http.Request, destination any, limit int64) error {
	req.Body = http.MaxBytesReader(w, req.Body, limit)
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			return maxBytes
		}
		return fmt.Errorf("invalid JSON request")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("request must contain one JSON value")
	}
	return nil
}

func decodeResponseJSON(body io.Reader, destination any, limit int64) error {
	decoder := json.NewDecoder(io.LimitReader(body, limit+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("finite: invalid bounded owner response")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("finite: owner response must contain one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
