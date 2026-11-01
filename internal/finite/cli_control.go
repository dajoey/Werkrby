package finite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/fsx"
)

const (
	cliRendezvousVersion = 1
	maxCLIRendezvousSize = 16 * 1024
)

type cliRendezvous struct {
	SchemaVersion int    `json:"schema_version"`
	Endpoint      string `json:"endpoint"`
	Token         string `json:"token"`
}

func cleanCLIDisposableRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", fmt.Errorf("finite: explicit disposable root is required")
	}
	absolute, resolved, err := resolveStatePath(root)
	if err != nil {
		return "", fmt.Errorf("finite: resolve disposable root: %w", err)
	}
	if err := checkLegacyStateRoot(absolute, resolved); err != nil {
		return "", err
	}
	return absolute, nil
}

func writeCLIRendezvous(path, endpoint, token string) error {
	if _, err := validateLoopbackEndpoint(endpoint); err != nil {
		return err
	}
	if err := validateBearerToken(token); err != nil {
		return err
	}
	data, err := json.Marshal(cliRendezvous{SchemaVersion: cliRendezvousVersion, Endpoint: endpoint, Token: token})
	if err != nil {
		return fmt.Errorf("finite: encode private rendezvous: %w", err)
	}
	if err := fsx.AtomicWriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("finite: publish private rendezvous: %w", err)
	}
	return nil
}

func readCLIRendezvous(path string) (cliRendezvous, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return cliRendezvous{}, fmt.Errorf("finite: read private rendezvous: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxCLIRendezvousSize {
		return cliRendezvous{}, fmt.Errorf("finite: private rendezvous must be a bounded regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cliRendezvous{}, fmt.Errorf("finite: read private rendezvous: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var rendezvous cliRendezvous
	if err := decoder.Decode(&rendezvous); err != nil {
		return cliRendezvous{}, fmt.Errorf("finite: invalid private rendezvous")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return cliRendezvous{}, fmt.Errorf("finite: private rendezvous must contain one JSON value")
	}
	if rendezvous.SchemaVersion != cliRendezvousVersion {
		return cliRendezvous{}, fmt.Errorf("finite: unsupported private rendezvous schema")
	}
	if _, err := validateLoopbackEndpoint(rendezvous.Endpoint); err != nil {
		return cliRendezvous{}, err
	}
	if err := validateBearerToken(rendezvous.Token); err != nil {
		return cliRendezvous{}, err
	}
	return rendezvous, nil
}

func removeCLIRendezvous(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("finite: remove private rendezvous: %w", err)
	}
	return nil
}

func newCLIControlClient(root string) (cliControlClient, error) {
	cleanRoot, err := cleanCLIDisposableRoot(root)
	if err != nil {
		return nil, err
	}
	rendezvous, err := readCLIRendezvous(filepath.Join(cleanRoot, "control.json"))
	if err != nil {
		return nil, err
	}
	return ControlClient{
		Endpoint: rendezvous.Endpoint,
		Token:    rendezvous.Token,
		Client:   &http.Client{Timeout: 5 * time.Second},
	}, nil
}

func runFiniteReceiver(ctx context.Context, options receiverOptions) (returnErr error) {
	root, err := cleanCLIDisposableRoot(options.Root)
	if err != nil {
		return err
	}
	if options.Addr == "" {
		options.Addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", options.Addr)
	if err != nil {
		return fmt.Errorf("test-receiver: listen on loopback: %w", err)
	}
	if err := validateLoopbackListener(listener); err != nil {
		_ = listener.Close()
		return err
	}
	token, err := NewControlToken()
	if err != nil {
		_ = listener.Close()
		return err
	}
	config := ReceiverConfig{Root: root, Token: token, Fault: options.Fault}
	if options.Fault == ReceiverFaultCrashAfterEffect {
		config.Exit = func() { os.Exit(70) }
	}
	if _, err := NewFileReceiver(config); err != nil {
		_ = listener.Close()
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- RunFileReceiver(runCtx, listener, config) }()
	rendezvousPath := filepath.Join(root, "receiver.json")
	if err := writeCLIRendezvous(rendezvousPath, "http://"+listener.Addr().String(), token); err != nil {
		cancel()
		_ = listener.Close()
		<-done
		return err
	}
	finished := false
	defer func() {
		cancel()
		_ = listener.Close()
		if !finished {
			<-done
		}
		returnErr = errors.Join(returnErr, removeCLIRendezvous(rendezvousPath))
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-done:
		finished = true
		err = cliContextError(ctx, err)
		if err == nil {
			if ctx.Err() != nil {
				return nil
			}
			err = fmt.Errorf("unexpected shutdown")
		}
		return fmt.Errorf("test-receiver: receiver loop stopped: %w", err)
	}
}
