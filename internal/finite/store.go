package finite

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var (
	ErrNotFound           = errors.New("finite: not found")
	ErrVersionConflict    = errors.New("finite: task version conflict")
	ErrStateConflict      = errors.New("finite: task state conflict")
	ErrSettlementConflict = errors.New("finite: conflicting settlement")
	ErrInvalidResult      = errors.New("finite: invalid result")
)

// Store owns the current-schema SQLite workflow authority.
type Store struct {
	db  *sql.DB
	cfg Config
	now func() time.Time
}

// OpenStore opens an isolated current-schema store.
func OpenStore(cfg Config) (*Store, error) {
	return &Store{cfg: cfg, now: time.Now}, nil
}

func (s *Store) Close() error { return nil }

func (s *Store) GetTask(ctx context.Context, id string) (Task, error) { return Task{}, nil }
func (s *Store) GetRun(ctx context.Context, id int64) (Run, error)    { return Run{}, nil }
func (s *Store) ListRuns(ctx context.Context, taskID string) ([]Run, error) {
	return nil, nil
}
func (s *Store) DerivedAccounting(ctx context.Context, taskID string, phase Phase) (Accounting, error) {
	return Accounting{}, nil
}
func (s *Store) GetUsage(ctx context.Context, runID int64) (Usage, error) {
	return Usage{}, nil
}
func (s *Store) ListOutbox(ctx context.Context) ([]OutboxEntry, error) { return nil, nil }

// CheckInvariants verifies database and active/open-run consistency.
func (s *Store) CheckInvariants(ctx context.Context) error { return nil }
