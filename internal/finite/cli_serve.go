package finite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const maxCLIRosterBytes = 64 * 1024

type cliOutboxProcessor interface {
	ProcessOnce(context.Context) (int, error)
}

type serveRuntimeDependencies struct {
	listen       func(string, string) (net.Listener, error)
	newToken     func() (string, error)
	executable   func() (string, error)
	serveControl func(context.Context, net.Listener, *Store, *WorkspaceLock, string, ControlWorkflow) error
	newOutbox    func(*Store, DeliveryTransport) cliOutboxProcessor
	stderr       io.Writer
}

func defaultServeRuntimeDependencies() serveRuntimeDependencies {
	return serveRuntimeDependencies{
		listen:       net.Listen,
		newToken:     NewControlToken,
		executable:   os.Executable,
		serveControl: ServeControlWithWorkflow,
		newOutbox: func(store *Store, transport DeliveryTransport) cliOutboxProcessor {
			return NewOutboxProcessor(store, transport)
		},
		stderr: os.Stderr,
	}
}

func runFiniteServe(ctx context.Context, options serveOptions) error {
	return runFiniteServeWithDeps(ctx, options, defaultServeRuntimeDependencies())
}

func runFiniteServeWithDeps(ctx context.Context, options serveOptions, deps serveRuntimeDependencies) (returnErr error) {
	config := Config{
		Root: options.Root, WorkerDir: options.Worker,
		ControlAddr: options.ControlAddr, RosterPath: options.RosterPath,
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if options.ControlAddr == "" {
		options.ControlAddr = "127.0.0.1:0"
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 250 * time.Millisecond
	}
	if options.StopGrace <= 0 {
		options.StopGrace = 5 * time.Second
	}
	seats, err := cliSeatProvider(options)
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	if _, err := seats(ctx); err != nil {
		return fmt.Errorf("serve: load seats: %w", err)
	}
	var transport DeliveryTransport
	if strings.TrimSpace(options.ReceiverConfig) != "" {
		receiverPath, err := cleanCLIDisposableRoot(options.ReceiverConfig)
		if err != nil {
			return fmt.Errorf("serve: receiver config: %w", err)
		}
		receiver, err := readCLIRendezvous(receiverPath)
		if err != nil {
			return fmt.Errorf("serve: receiver config: %w", err)
		}
		transport = HTTPDeliveryTransport{Endpoint: receiver.Endpoint, Token: receiver.Token}
	}
	listener, err := deps.listen("tcp", options.ControlAddr)
	if err != nil {
		return fmt.Errorf("serve: listen on loopback control: %w", err)
	}
	if err := validateLoopbackListener(listener); err != nil {
		_ = listener.Close()
		return err
	}
	token, err := deps.newToken()
	if err != nil {
		_ = listener.Close()
		return err
	}
	executable, err := deps.executable()
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("serve: resolve executable: %w", err)
	}
	report := func(err error) {
		if err != nil && deps.stderr != nil {
			_, _ = fmt.Fprintf(deps.stderr, "werkrby-finite: %v\n", err)
		}
	}
	engine, err := NewEngine(config, EngineOptions{
		Harness: options.Harness, Seats: seats, Executable: executable,
		PollInterval: options.PollInterval, StopGrace: options.StopGrace,
		Domains: options.Domains, OnError: report,
	})
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("serve: acquire finite owner: %w", err)
	}
	configureFiniteHermesOwner(engine, options.Harness)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var loops sync.WaitGroup
	controlDone := make(chan error, 1)
	schedulerDone := make(chan error, 1)
	rendezvousPath := filepath.Join(engine.cfg.Root, "control.json")
	published := false
	defer func() {
		cancel()
		_ = listener.Close()
		loops.Wait()
		if published {
			returnErr = errors.Join(returnErr, removeCLIRendezvous(rendezvousPath))
		}
		returnErr = errors.Join(returnErr, engine.Close())
	}()

	loops.Add(1)
	go func() {
		defer loops.Done()
		controlDone <- deps.serveControl(runCtx, listener, engine.store, engine.lock, token, engine)
	}()
	if err := writeCLIRendezvous(rendezvousPath, "http://"+listener.Addr().String(), token); err != nil {
		return err
	}
	published = true
	if transport != nil {
		processor := deps.newOutbox(engine.store, transport)
		if processor == nil {
			return fmt.Errorf("serve: outbox processor is not configured")
		}
		loops.Add(1)
		go func() {
			defer loops.Done()
			runCLIOutbox(runCtx, processor, report)
		}()
	}
	loops.Add(1)
	go func() {
		defer loops.Done()
		schedulerDone <- engine.Run(runCtx)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-controlDone:
		err = cliContextError(ctx, err)
		if err == nil {
			if ctx.Err() != nil {
				return nil
			}
			err = fmt.Errorf("unexpected shutdown")
		}
		return fmt.Errorf("serve: control loop stopped: %w", err)
	case err := <-schedulerDone:
		err = cliContextError(ctx, err)
		if err == nil {
			if ctx.Err() != nil {
				return nil
			}
			err = fmt.Errorf("unexpected shutdown")
		}
		return fmt.Errorf("serve: scheduler stopped: %w", err)
	}
}

func runCLIOutbox(ctx context.Context, processor cliOutboxProcessor, report func(error)) {
	for {
		processed, err := processor.ProcessOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			report(fmt.Errorf("outbox delivery: %w", err))
		}
		if processed > 0 && err == nil {
			continue
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

type cliRoster struct {
	Seats []cliRosterSeat `yaml:"seats"`
}

type cliRosterSeat struct {
	Name       string     `yaml:"name"`
	Available  bool       `yaml:"available"`
	QuotaReset *time.Time `yaml:"quota_reset,omitempty"`
}

func cliSeatProvider(options serveOptions) (func(context.Context) ([]Seat, error), error) {
	if strings.TrimSpace(options.RosterPath) == "" {
		if len(options.Seats) == 0 {
			options.Seats = []string{"default"}
		}
		seats, err := staticCLISeats(options.Seats)
		if err != nil {
			return nil, err
		}
		return func(context.Context) ([]Seat, error) {
			return append([]Seat(nil), seats...), nil
		}, nil
	}
	path, err := cleanCLIDisposableRoot(options.RosterPath)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) ([]Seat, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return loadCLIRoster(path)
	}, nil
}

func staticCLISeats(names []string) ([]Seat, error) {
	seats := make([]Seat, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if err := validateCLISeatName(name); err != nil {
			return nil, err
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("duplicate seat %q", name)
		}
		seen[name] = struct{}{}
		seats = append(seats, Seat{Name: name, Available: true})
	}
	if len(seats) == 0 {
		return nil, fmt.Errorf("at least one seat is required")
	}
	return seats, nil
}

func validateCLISeatName(name string) error {
	if strings.TrimSpace(name) == "" || len(name) > 128 || strings.IndexFunc(name, func(r rune) bool { return r <= 0x20 || r == 0x7f }) >= 0 {
		return fmt.Errorf("seat names must be nonempty single-line values of at most 128 bytes")
	}
	return nil
}

func loadCLIRoster(path string) ([]Seat, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("read finite roster: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxCLIRosterBytes {
		return nil, fmt.Errorf("finite roster must be a bounded regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read finite roster: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var roster cliRoster
	if err := decoder.Decode(&roster); err != nil {
		return nil, fmt.Errorf("decode finite roster: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("finite roster must contain one document")
	}
	if len(roster.Seats) == 0 {
		return nil, fmt.Errorf("finite roster requires at least one seat")
	}
	seats := make([]Seat, 0, len(roster.Seats))
	seen := make(map[string]struct{}, len(roster.Seats))
	for _, configured := range roster.Seats {
		if err := validateCLISeatName(configured.Name); err != nil {
			return nil, err
		}
		if _, exists := seen[configured.Name]; exists {
			return nil, fmt.Errorf("duplicate seat %q", configured.Name)
		}
		seen[configured.Name] = struct{}{}
		seats = append(seats, Seat{Name: configured.Name, Available: configured.Available, QuotaReset: configured.QuotaReset})
	}
	return seats, nil
}
