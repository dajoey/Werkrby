package harness

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/fsx"
	"github.com/agent0ai/spynel/internal/roster"
)

// RunBoundarySpec opts one headless provider turn into finite run isolation.
// The zero value is never valid; WithRunBoundary is the only activation path.
type RunBoundarySpec struct {
	RunID        int64
	ProcessToken string
	RunDir       string
}

type runBoundaryContextKey struct{}

// WithRunBoundary marks ctx with the immutable finite run binding. Adapters
// retain their legacy behavior when this value is absent.
func WithRunBoundary(ctx context.Context, spec RunBoundarySpec) context.Context {
	return context.WithValue(ctx, runBoundaryContextKey{}, spec)
}

func runBoundaryFromContext(ctx context.Context) (RunBoundarySpec, bool) {
	spec, ok := ctx.Value(runBoundaryContextKey{}).(RunBoundarySpec)
	return spec, ok
}

// HermesTurnIdentity is the exact durable provider turn admitted for one run.
type HermesTurnIdentity struct {
	RunID        int64  `json:"run_id"`
	ProcessToken string `json:"process_token"`
	SessionID    string `json:"session_id"`
	RequestID    string `json:"request_id"`
	AdmissionID  string `json:"admission_id"`
	Generation   *int64 `json:"generation,omitempty"`
	OwnerEpoch   int64  `json:"owner_epoch"`
}

// BoundaryLiveness distinguishes proven absence from live or unknown work.
type BoundaryLiveness string

const (
	BoundaryAbsent  BoundaryLiveness = "absent"
	BoundaryLive    BoundaryLiveness = "live"
	BoundaryUnknown BoundaryLiveness = "unknown"
)

// HermesBoundaryObservation is one fail-closed provider-boundary reading.
type HermesBoundaryObservation struct {
	Liveness BoundaryLiveness
	Identity *HermesTurnIdentity
	Status   string
	Outcome  string
}

const (
	hermesBoundarySchema       = 1
	hermesBoundaryMetadataFile = "hermes-provider.json"
	finiteHermesMarker         = ".finite-hermes-disposable"
	hermesBoundaryPoll         = 50 * time.Millisecond
)

var errHermesBoundaryUnknown = errors.New("hermes provider-turn liveness is unknown")

type hermesBoundaryMetadata struct {
	SchemaVersion int                 `json:"schema_version"`
	RunID         int64               `json:"run_id"`
	ProcessToken  string              `json:"process_token"`
	SessionKey    string              `json:"session_key"`
	PromptSHA256  string              `json:"prompt_sha256"`
	Home          string              `json:"hermes_home"`
	GatewayHome   string              `json:"gateway_home"`
	Profile       string              `json:"profile"`
	LedgerPath    string              `json:"ledger_path"`
	ClientStarted bool                `json:"client_started"`
	ClientExited  bool                `json:"client_exited"`
	Identity      *HermesTurnIdentity `json:"identity,omitempty"`
}

type hermesRunBoundary struct {
	spec       RunBoundarySpec
	sessionKey string
	prompt     string
	home       string
	meta       hermesBoundaryMetadata

	mu      sync.Mutex
	stop    chan struct{}
	done    chan struct{}
	started bool
	close   sync.Once
}

func validateHermesBoundarySpec(spec RunBoundarySpec) error {
	if spec.RunID <= 0 || strings.TrimSpace(spec.ProcessToken) == "" {
		return fmt.Errorf("finite Hermes run ID and process token are required")
	}
	if spec.RunDir == "" || !filepath.IsAbs(spec.RunDir) {
		return fmt.Errorf("finite Hermes run directory must be absolute")
	}
	return nil
}

func validateDisposableHermesHome(home, profile string) error {
	if profile != "wk-finite-test" && profile != "wk-finite-wall" {
		return fmt.Errorf("finite Hermes profile %q is not disposable test staffing", profile)
	}
	if home == "" || !filepath.IsAbs(home) {
		return fmt.Errorf("finite Hermes requires an explicit absolute HERMES_HOME")
	}
	if userHome, err := os.UserHomeDir(); err == nil {
		if filepath.Clean(home) == filepath.Join(userHome, ".hermes") {
			return fmt.Errorf("finite Hermes refuses the default ~/.hermes home")
		}
	}
	marker := filepath.Join(home, finiteHermesMarker)
	info, err := os.Lstat(marker)
	if err != nil {
		return fmt.Errorf("finite Hermes disposable marker: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("finite Hermes disposable marker is not a regular file")
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		return err
	}
	if string(data) != profile+"\n" {
		return fmt.Errorf("finite Hermes disposable marker does not match profile %s", profile)
	}
	return nil
}

func newHermesRunBoundary(h *Herdr, spec RunBoundarySpec, key, prompt string, staff roster.Staff) (*hermesRunBoundary, error) {
	if err := validateHermesBoundarySpec(spec); err != nil {
		return nil, err
	}
	if h.hermesHome == "" && strings.TrimSpace(os.Getenv("HERMES_HOME")) == "" {
		return nil, fmt.Errorf("finite Hermes requires an explicit disposable HERMES_HOME")
	}
	home, err := hermesHomeDir(h.hermesHome)
	if err != nil {
		return nil, err
	}
	home, err = filepath.Abs(home)
	if err != nil {
		return nil, err
	}
	profile, explicit, err := hermesLaunchProfile(staff.Args)
	if err != nil {
		return nil, err
	}
	if !explicit {
		return nil, fmt.Errorf("finite Hermes requires an explicit disposable profile")
	}
	if err := validateDisposableHermesHome(home, profile); err != nil {
		return nil, err
	}
	ledgerPath, err := hermesLedgerPath(home, staff.Args)
	if err != nil {
		return nil, err
	}
	promptData, err := readSupervisorPromptForBoundary(filepath.Join(spec.RunDir, "prompt.txt"), prompt)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(promptData)
	gatewayHome := home
	if profile != "default" {
		gatewayHome = filepath.Join(home, "profiles", profile)
	}
	sessionKey := fmt.Sprintf("finite:%d", spec.RunID)
	if key != "" {
		// The finite session namespace is deliberately per run, never the legacy
		// task/phase conversation reused by ordinary Werkrby.
		sessionKey = fmt.Sprintf("finite:%d", spec.RunID)
	}
	boundary := &hermesRunBoundary{
		spec: spec, sessionKey: sessionKey, prompt: prompt, home: home,
		stop: make(chan struct{}), done: make(chan struct{}),
		meta: hermesBoundaryMetadata{
			SchemaVersion: hermesBoundarySchema, RunID: spec.RunID, ProcessToken: spec.ProcessToken,
			SessionKey: sessionKey, PromptSHA256: hex.EncodeToString(digest[:]), Home: home,
			GatewayHome: gatewayHome, Profile: profile, LedgerPath: ledgerPath,
		},
	}
	if err := boundary.writeMetadataLocked(); err != nil {
		return nil, err
	}
	return boundary, nil
}

func readSupervisorPromptForBoundary(path, expected string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("finite Hermes run prompt: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 1024*1024+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > 1024*1024 || string(data) != expected {
		return nil, fmt.Errorf("finite Hermes run prompt does not match the supervisor binding")
	}
	return data, nil
}

func (b *hermesRunBoundary) writeMetadataLocked() error {
	data, err := json.Marshal(b.meta)
	if err != nil {
		return err
	}
	return fsx.AtomicWriteFile(filepath.Join(b.spec.RunDir, hermesBoundaryMetadataFile), data, 0o600)
}

func (b *hermesRunBoundary) markClientStarted() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.meta.ClientStarted = true
	if err := b.writeMetadataLocked(); err != nil {
		return err
	}
	b.started = true
	go b.poll()
	return nil
}

func (b *hermesRunBoundary) poll() {
	defer close(b.done)
	ticker := time.NewTicker(hermesBoundaryPoll)
	defer ticker.Stop()
	for {
		_ = b.refreshIdentity()
		select {
		case <-b.stop:
			return
		case <-ticker.C:
		}
	}
}

func (b *hermesRunBoundary) refreshIdentity() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.meta.Identity != nil && b.meta.Identity.Generation != nil {
		return nil
	}
	db, err := openHermesLedger(b.meta.LedgerPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if b.meta.Identity == nil {
		identity, err := findExactHermesAdmission(context.Background(), db, b.spec, b.prompt)
		if err != nil {
			return err
		}
		b.meta.Identity = identity
		return b.writeMetadataLocked()
	}
	row, err := readExactHermesAdmission(context.Background(), db, *b.meta.Identity, b.prompt)
	if err != nil {
		return err
	}
	identity, err := row.identity(b.spec)
	if err != nil {
		return err
	}
	if identity.Generation == nil {
		return nil
	}
	b.meta.Identity = &identity
	return b.writeMetadataLocked()
}

func (b *hermesRunBoundary) closeBoundary() error {
	var closeErr error
	b.close.Do(func() {
		if b.started {
			close(b.stop)
			<-b.done
		}
		_ = b.refreshIdentity()
		b.mu.Lock()
		defer b.mu.Unlock()
		b.meta.ClientExited = true
		closeErr = b.writeMetadataLocked()
	})
	return closeErr
}

type hermesAdmissionRow struct {
	admissionID string
	requestID   string
	sessionID   string
	lineageJSON string
	payloadJSON string
	status      string
	ownerEpoch  int64
	generation  *int64
	outcome     string
}

const hermesAdmissionColumns = `admission_id,request_id,target_session_id,lineage_json,payload_json,status,owner_epoch,generation,COALESCE(outcome,'')`

func scanHermesAdmission(rows interface{ Scan(...any) error }) (hermesAdmissionRow, error) {
	var row hermesAdmissionRow
	var generation sql.NullInt64
	err := rows.Scan(&row.admissionID, &row.requestID, &row.sessionID, &row.lineageJSON, &row.payloadJSON,
		&row.status, &row.ownerEpoch, &generation, &row.outcome)
	if generation.Valid {
		value := generation.Int64
		row.generation = &value
	}
	return row, err
}

func (row hermesAdmissionRow) identity(spec RunBoundarySpec) (HermesTurnIdentity, error) {
	identity := HermesTurnIdentity{
		RunID: spec.RunID, ProcessToken: spec.ProcessToken, SessionID: row.sessionID,
		RequestID: row.requestID, AdmissionID: row.admissionID, Generation: row.generation, OwnerEpoch: row.ownerEpoch,
	}
	if identity.SessionID == "" || identity.RequestID == "" || identity.AdmissionID == "" || identity.OwnerEpoch < 0 {
		return HermesTurnIdentity{}, fmt.Errorf("invalid Hermes admission identity")
	}
	return identity, nil
}

func (row hermesAdmissionRow) prompt() (string, error) {
	var payload struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(row.payloadJSON), &payload); err != nil {
		return "", err
	}
	return payload.Text, nil
}

func findExactHermesAdmission(ctx context.Context, db *sql.DB, spec RunBoundarySpec, prompt string) (*HermesTurnIdentity, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+hermesAdmissionColumns+` FROM session_admissions WHERE json_extract(payload_json,'$.text')=? ORDER BY seq`, prompt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var identities []HermesTurnIdentity
	for rows.Next() {
		row, err := scanHermesAdmission(rows)
		if err != nil {
			return nil, err
		}
		text, err := row.prompt()
		if err != nil {
			return nil, err
		}
		if text != prompt {
			return nil, fmt.Errorf("Hermes admission prompt canonicalization mismatch")
		}
		identity, err := row.identity(spec)
		if err != nil {
			return nil, err
		}
		identities = append(identities, identity)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(identities) == 0 {
		return nil, nil
	}
	if len(identities) != 1 {
		return nil, fmt.Errorf("multiple Hermes admissions match the exact finite run prompt")
	}
	return &identities[0], nil
}

func readExactHermesAdmission(ctx context.Context, db *sql.DB, identity HermesTurnIdentity, prompt string) (hermesAdmissionRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+hermesAdmissionColumns+` FROM session_admissions WHERE admission_id=?`, identity.AdmissionID)
	if err != nil {
		return hermesAdmissionRow{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return hermesAdmissionRow{}, fmt.Errorf("exact Hermes admission is missing")
	}
	row, err := scanHermesAdmission(rows)
	if err != nil {
		return row, err
	}
	if rows.Next() {
		return row, fmt.Errorf("duplicate Hermes admission identity")
	}
	if err := rows.Err(); err != nil {
		return row, err
	}
	text, err := row.prompt()
	if err != nil {
		return row, err
	}
	if row.sessionID != identity.SessionID || row.requestID != identity.RequestID || text != prompt {
		return row, fmt.Errorf("Hermes admission identity or prompt changed")
	}
	if identity.Generation != nil && (row.generation == nil || *identity.Generation != *row.generation) {
		return row, fmt.Errorf("Hermes execution generation changed")
	}
	return row, nil
}

func readHermesBoundaryMetadata(spec RunBoundarySpec) (hermesBoundaryMetadata, string, error) {
	var meta hermesBoundaryMetadata
	if err := validateHermesBoundarySpec(spec); err != nil {
		return meta, "", err
	}
	path := filepath.Join(spec.RunDir, hermesBoundaryMetadataFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return meta, "", err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&meta); err != nil {
		return meta, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return meta, "", fmt.Errorf("finite Hermes metadata has trailing data")
	}
	if meta.SchemaVersion != hermesBoundarySchema || meta.RunID != spec.RunID || meta.ProcessToken != spec.ProcessToken {
		return meta, "", fmt.Errorf("finite Hermes metadata run binding mismatch")
	}
	if err := validateDisposableHermesHome(meta.Home, meta.Profile); err != nil {
		return meta, "", err
	}
	promptData, err := os.ReadFile(filepath.Join(spec.RunDir, "prompt.txt"))
	if err != nil {
		return meta, "", err
	}
	digest := sha256.Sum256(promptData)
	if meta.PromptSHA256 != hex.EncodeToString(digest[:]) {
		return meta, "", fmt.Errorf("finite Hermes metadata prompt binding mismatch")
	}
	if meta.Identity != nil && (meta.Identity.RunID != spec.RunID || meta.Identity.ProcessToken != spec.ProcessToken) {
		return meta, "", fmt.Errorf("finite Hermes identity run binding mismatch")
	}
	return meta, string(promptData), nil
}

func linkedHermesSession(lineageJSON, target, sessionID string) (bool, error) {
	if target == sessionID {
		return true, nil
	}
	var lineage []string
	if err := json.Unmarshal([]byte(lineageJSON), &lineage); err != nil {
		return false, err
	}
	for _, id := range lineage {
		if id == sessionID {
			return true, nil
		}
	}
	return false, nil
}

func terminalHermesOutcome(outcome string) bool {
	switch outcome {
	case "completed", "failed", "interrupted", "rejected", "cancelled", "steered":
		return true
	}
	return false
}

func observeExactHermesAdmission(ctx context.Context, db *sql.DB, meta hermesBoundaryMetadata, runDir, prompt string) (HermesBoundaryObservation, error) {
	unknown := func(err error) (HermesBoundaryObservation, error) {
		return HermesBoundaryObservation{Liveness: BoundaryUnknown, Identity: meta.Identity}, errors.Join(errHermesBoundaryUnknown, err)
	}
	if meta.Identity == nil {
		identity, err := findExactHermesAdmission(ctx, db, specFromMetadata(meta), prompt)
		if err != nil {
			return unknown(err)
		}
		if identity == nil {
			if meta.ClientExited || !meta.ClientStarted {
				return HermesBoundaryObservation{Liveness: BoundaryAbsent}, nil
			}
			return unknown(fmt.Errorf("exact finite prompt has no Hermes admission"))
		}
		meta.Identity = identity
		data, _ := json.Marshal(meta)
		if err := fsx.AtomicWriteFile(filepath.Join(runDir, hermesBoundaryMetadataFile), data, 0o600); err != nil {
			return unknown(err)
		}
	}
	row, err := readExactHermesAdmission(ctx, db, *meta.Identity, prompt)
	if err != nil {
		return unknown(err)
	}
	identity, err := row.identity(specFromMetadata(meta))
	if err != nil {
		return unknown(err)
	}
	observation := HermesBoundaryObservation{Identity: &identity, Status: row.status, Outcome: row.outcome}
	switch row.status {
	case "queued", "started":
		observation.Liveness = BoundaryLive
		return observation, nil
	case "unknown":
		observation.Liveness = BoundaryUnknown
		return observation, errHermesBoundaryUnknown
	case "terminal":
		if !terminalHermesOutcome(row.outcome) {
			observation.Liveness = BoundaryLive
			return observation, nil
		}
	default:
		return unknown(fmt.Errorf("unrecognized Hermes admission status %q", row.status))
	}

	pending, err := db.QueryContext(ctx, `SELECT `+hermesAdmissionColumns+` FROM session_admissions WHERE status IN ('queued','started','unknown') ORDER BY seq`)
	if err != nil {
		return unknown(err)
	}
	defer pending.Close()
	for pending.Next() {
		other, err := scanHermesAdmission(pending)
		if err != nil {
			return unknown(err)
		}
		linked, err := linkedHermesSession(other.lineageJSON, other.sessionID, identity.SessionID)
		if err != nil {
			return unknown(err)
		}
		if !linked {
			continue
		}
		if other.status == "unknown" {
			return unknown(fmt.Errorf("linked Hermes admission %s is unknown", other.admissionID))
		}
		observation.Liveness = BoundaryLive
		return observation, nil
	}
	if err := pending.Err(); err != nil {
		return unknown(err)
	}

	if identity.Generation != nil {
		workers, err := db.QueryContext(ctx, `SELECT session_id,status FROM worker_executions WHERE owner_epoch=? AND generation=? AND status!='terminal'`, identity.OwnerEpoch, *identity.Generation)
		if err != nil {
			return unknown(err)
		}
		defer workers.Close()
		for workers.Next() {
			var sessionID, status string
			if err := workers.Scan(&sessionID, &status); err != nil {
				return unknown(err)
			}
			linked, err := linkedHermesSession(row.lineageJSON, sessionID, identity.SessionID)
			if err != nil {
				return unknown(err)
			}
			if !linked {
				continue
			}
			switch status {
			case "registered", "running":
				observation.Liveness = BoundaryLive
				return observation, nil
			case "unknown":
				return unknown(fmt.Errorf("linked Hermes worker is unknown"))
			default:
				return unknown(fmt.Errorf("unrecognized Hermes worker status %q", status))
			}
		}
		if err := workers.Err(); err != nil {
			return unknown(err)
		}
	}
	observation.Liveness = BoundaryAbsent
	return observation, nil
}

func specFromMetadata(meta hermesBoundaryMetadata) RunBoundarySpec {
	return RunBoundarySpec{RunID: meta.RunID, ProcessToken: meta.ProcessToken}
}

// HermesRunHasAdmission checks for an exact prompt admission when the metadata
// file is absent. Setup failures that precede any client start have no admission;
// an unreadable or ambiguous ledger always fails closed.
func HermesRunHasAdmission(ctx context.Context, spec RunBoundarySpec) (bool, error) {
	data, err := os.ReadFile(filepath.Join(spec.RunDir, "supervisor.json"))
	if err != nil {
		return false, err
	}
	var request struct {
		Harness HarnessConfig `json:"harness"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		return false, err
	}
	staff, ok, err := roster.ExpandStaff(filepath.Join(request.Harness.Cwd, roster.StateDirName), strings.TrimSpace(request.Harness.Model))
	if err != nil || !ok || !staff.Headless || staff.Runner != "hermes" {
		return false, nil
	}
	if strings.TrimSpace(os.Getenv("HERMES_HOME")) == "" {
		return false, errors.New("Hermes admission check requires an explicit disposable HERMES_HOME")
	}
	home, err := hermesHomeDir("")
	if err != nil {
		return false, err
	}
	profile, explicit, err := hermesLaunchProfile(staff.Args)
	if err != nil || !explicit {
		return false, nil
	}
	if err := validateDisposableHermesHome(home, profile); err != nil {
		// Profile/home validation happens before the client starts. A terminal run
		// that failed at this fence has no provider admission.
		return false, nil
	}
	ledger, err := hermesLedgerPath(home, staff.Args)
	if err != nil {
		return false, err
	}
	prompt, err := os.ReadFile(filepath.Join(spec.RunDir, "prompt.txt"))
	if err != nil {
		return false, err
	}
	db, err := openHermesLedger(ledger)
	if err != nil {
		return false, err
	}
	defer db.Close()
	identity, err := findExactHermesAdmission(ctx, db, spec, string(prompt))
	if err != nil {
		return false, err
	}
	return identity != nil, nil
}

// InspectHermesRunBoundary reconstructs the exact provider turn from run-private
// metadata and the disposable profile ledger. It never infers absence from a
// client PID or process exit.
func InspectHermesRunBoundary(ctx context.Context, spec RunBoundarySpec) (HermesBoundaryObservation, error) {
	meta, prompt, err := readHermesBoundaryMetadata(spec)
	if err != nil {
		return HermesBoundaryObservation{Liveness: BoundaryUnknown}, errors.Join(errHermesBoundaryUnknown, err)
	}
	db, err := openHermesLedger(meta.LedgerPath)
	if err != nil {
		return HermesBoundaryObservation{Liveness: BoundaryUnknown, Identity: meta.Identity}, errors.Join(errHermesBoundaryUnknown, err)
	}
	defer db.Close()
	return observeExactHermesAdmission(ctx, db, meta, spec.RunDir, prompt)
}

// StopHermesRunBoundary stops the exact admitted turn and returns only after
// the same complete inspection used by recovery proves its absence.
func StopHermesRunBoundary(ctx context.Context, spec RunBoundarySpec) (BoundaryLiveness, error) {
	observation, err := InspectHermesRunBoundary(ctx, spec)
	if err != nil || observation.Liveness != BoundaryLive {
		return observation.Liveness, err
	}
	if observation.Identity == nil {
		return BoundaryUnknown, errHermesBoundaryUnknown
	}
	meta, _, err := readHermesBoundaryMetadata(spec)
	if err != nil {
		return BoundaryUnknown, errors.Join(errHermesBoundaryUnknown, err)
	}
	if err := stopHermesGatewayAdmission(ctx, meta.GatewayHome, *observation.Identity); err != nil {
		return BoundaryUnknown, errors.Join(errHermesBoundaryUnknown, err)
	}
	for {
		observation, err = InspectHermesRunBoundary(ctx, spec)
		if err != nil {
			return BoundaryUnknown, err
		}
		if observation.Liveness == BoundaryAbsent {
			return BoundaryAbsent, nil
		}
		select {
		case <-ctx.Done():
			return BoundaryUnknown, errors.Join(errHermesBoundaryUnknown, ctx.Err())
		case <-time.After(hermesBoundaryPoll):
		}
	}
}

const hermesCancelRPC = `
import asyncio, sys
from hermes_cli.gateway_client import connect_gateway

async def main():
    session_id, admission_id = sys.argv[1], sys.argv[2]
    generation = int(sys.argv[3]) if sys.argv[3] else None
    async with connect_gateway() as client:
        try:
            await client.rpc("prompt.cancel", session_id=session_id, admission_id=admission_id)
        except Exception as exc:
            if "stale_generation" not in str(exc):
                raise
        snapshot = await client.rpc("session.resume", session_id=session_id)
        generation = snapshot.get("execution_generation") or generation
        if snapshot.get("running") and generation is not None:
            try:
                await client.rpc("session.interrupt", session_id=session_id, execution_generation=generation)
            except Exception as exc:
                if "stale_generation" not in str(exc):
                    raise
                snapshot = await client.rpc("session.resume", session_id=session_id)
                if snapshot.get("running"):
                    await client.rpc("session.interrupt", session_id=session_id,
                                     execution_generation=snapshot.get("execution_generation"))

asyncio.run(main())
`

// stopHermesGatewayAdmission is replaced only by controlled tests. Production
// calls the real gateway's exact admission cancellation RPC.
var stopHermesGatewayAdmission = func(ctx context.Context, gatewayHome string, identity HermesTurnIdentity) error {
	interpreter := "python3"
	if _, err := os.Stat(hermesVenvPython); err == nil {
		interpreter = hermesVenvPython
	}
	generation := ""
	if identity.Generation != nil {
		generation = strconv.FormatInt(*identity.Generation, 10)
	}
	command := exec.CommandContext(ctx, interpreter, "-c", hermesCancelRPC, identity.SessionID, identity.AdmissionID, generation)
	environment := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "HERMES_") {
			environment = append(environment, entry)
		}
	}
	command.Env = append(environment, "HERMES_HOME="+gatewayHome)
	if output, err := command.CombinedOutput(); err != nil {
		detail := strings.TrimSpace(string(output))
		if len(detail) > 500 {
			detail = detail[len(detail)-500:]
		}
		if detail == "" {
			detail = err.Error()
		}
		return fmt.Errorf("exact Hermes gateway cancellation failed: %s", detail)
	}
	return nil
}
