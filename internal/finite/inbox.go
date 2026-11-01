package finite

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/agent0ai/spynel/internal/fsx"
)

// The run inbox is the engine's channel into one live provider turn: an
// append-only per-run file the supervised worker reads between steps. It is
// run-private control state beside prompt.txt and supervisor.json — SQLite
// stays the workflow authority, the provider is never called for delivery,
// and each message id is delivered at most once.

const (
	inboxSchemaVersion     = 1
	inboxFile              = "inbox.jsonl"
	inboxAckFile           = "inbox-ack.jsonl"
	inboxKindMessage       = "message"
	inboxKindInterrupt     = "interrupt"
	maxInboxTextRunes      = 8000
	maxInboxTextBytes      = 15 * 1024
	maxInboxIDBytes        = 128
	maxInboxMessages       = 256
	maxInboxActivityRead   = 8 * 1024
	maxInboxActivityLine   = 512
	maxInboxAckFile  int64 = 64 * 1024
)

// InboxMessage is one engine-delivered operator message in a run's inbox.
type InboxMessage struct {
	SchemaVersion int       `json:"schema_version"`
	ID            string    `json:"id"`
	Kind          string    `json:"kind"`
	Text          string    `json:"text"`
	CreatedAt     time.Time `json:"created_at"`
}

// InboxReply is the bounded reply of the inbox control routes.
type InboxReply struct {
	OK           bool           `json:"ok"`
	TaskID       string         `json:"task_id"`
	State        State          `json:"state,omitempty"`
	RunID        int64          `json:"run_id,omitempty"`
	Phase        Phase          `json:"phase,omitempty"`
	MessageID    string         `json:"message_id,omitempty"`
	Kind         string         `json:"kind,omitempty"`
	Queued       bool           `json:"queued,omitempty"`
	Delivered    bool           `json:"delivered,omitempty"`
	Duplicate    bool           `json:"duplicate,omitempty"`
	Upgraded     bool           `json:"upgraded,omitempty"`
	Cancelled    bool           `json:"cancelled,omitempty"`
	Messages     []InboxMessage `json:"messages,omitempty"`
	AckedIDs     []string       `json:"acked_ids,omitempty"`
	LastActivity string         `json:"last_activity,omitempty"`
}

// OpenRunForTask returns the task's single open run, if any.
func (s *Store) OpenRunForTask(ctx context.Context, taskID string) (Run, error) {
	run, err := scanRun(s.db.QueryRowContext(ctx, runSelect+` WHERE task_id=? AND ended_at IS NULL ORDER BY id DESC LIMIT 1`, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, fmt.Errorf("%w: open run for task %s", ErrNotFound, taskID)
	}
	return run, err
}

func validateInboxKind(kind string) bool {
	return kind == inboxKindMessage || kind == inboxKindInterrupt
}

func validateInboxText(text string) error {
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("message text must not be empty")
	}
	if utf8.RuneCountInString(text) > maxInboxTextRunes || len(text) > maxInboxTextBytes {
		return fmt.Errorf("message text is too long (maximum %d characters)", maxInboxTextRunes)
	}
	for _, symbol := range text {
		if symbol < 0x20 && symbol != '\n' && symbol != '\r' && symbol != '\t' {
			return fmt.Errorf("message text must not contain control characters")
		}
	}
	return nil
}

func validateInboxID(id string) error {
	if id == "" {
		return nil
	}
	if len(id) > maxInboxIDBytes {
		return fmt.Errorf("message id is too long (maximum %d bytes)", maxInboxIDBytes)
	}
	if strings.ContainsAny(id, "\n\r\t") {
		return fmt.Errorf("message id must be a single line")
	}
	for _, symbol := range id {
		if symbol < 0x20 || symbol == 0x7f {
			return fmt.Errorf("message id must not contain control characters")
		}
	}
	return nil
}

// parseInboxLines decodes whole lines; a torn trailing line from a crash is
// skipped rather than failing the read.
func parseInboxLines(data []byte) ([]InboxMessage, error) {
	var messages []InboxMessage
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var message InboxMessage
		if err := strictDecodeJSON(line, &message); err != nil {
			continue
		}
		if message.SchemaVersion != inboxSchemaVersion || !validateInboxKind(message.Kind) ||
			message.ID == "" || message.Text == "" {
			continue
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func strictDecodeJSON(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	return decoder.Decode(out)
}

func jsonMarshalLine(value any) ([]byte, error) {
	line, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append(line, '\n'), nil
}

// newInboxID mints an unguessable single-line id for an anonymous send.
func newInboxID() string {
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	return "msg-" + hex.EncodeToString(nonce)
}

func sortStrings(values []string) { sort.Strings(values) }

func readInboxFile(runDir string) ([]InboxMessage, error) {
	data, err := os.ReadFile(filepath.Join(runDir, inboxFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseInboxLines(data)
}

// appendInboxMessage appends one newline-terminated line. A crash-torn tail
// from an earlier append is sealed with a newline first so the new message
// never fuses into an unreadable line.
func appendInboxMessage(runDir string, message InboxMessage) error {
	line, err := marshalInboxLine(message)
	if err != nil {
		return err
	}
	path := filepath.Join(runDir, inboxFile)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	payload := line
	info, err := file.Stat()
	if err == nil && info.Size() > 0 {
		var tail [1]byte
		if _, readErr := file.ReadAt(tail[:], info.Size()-1); readErr == nil && tail[0] != '\n' {
			payload = append([]byte("\n"), line...)
		}
	}
	if _, err := file.Write(payload); err != nil {
		return err
	}
	return file.Sync()
}

func rewriteInboxFile(runDir string, messages []InboxMessage) error {
	var buffer bytes.Buffer
	for _, message := range messages {
		line, err := marshalInboxLine(message)
		if err != nil {
			return err
		}
		buffer.Write(line)
	}
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return err
	}
	return fsx.AtomicWriteFile(filepath.Join(runDir, inboxFile), buffer.Bytes(), 0o600)
}

func marshalInboxLine(message InboxMessage) ([]byte, error) {
	message.SchemaVersion = inboxSchemaVersion
	line, err := jsonMarshalLine(message)
	if err != nil {
		return nil, fmt.Errorf("encode inbox message: %w", err)
	}
	return line, nil
}

// readInboxAcks returns the message ids the worker has handled, one per line.
func readInboxAcks(runDir string) (map[string]struct{}, error) {
	data, err := readBoundedTail(filepath.Join(runDir, inboxAckFile), maxInboxAckFile, maxInboxAckFile)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]struct{}{}, nil
		}
		return nil, err
	}
	acked := map[string]struct{}{}
	for _, line := range strings.Split(string(data), "\n") {
		id := strings.TrimSpace(line)
		if id != "" && len(id) <= maxInboxIDBytes {
			acked[id] = struct{}{}
		}
	}
	return acked, nil
}

// readInboxActivity returns the run's last bounded provider event line, the
// same status text the supervisor emits while the turn works.
func readInboxActivity(runDir string) string {
	data, err := readBoundedTail(filepath.Join(runDir, "events.jsonl"), maxInboxActivityRead, maxInboxActivityRead)
	if err != nil {
		return ""
	}
	lines := strings.Split(string(data), "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		line := strings.TrimSpace(lines[index])
		if line == "" {
			continue
		}
		var event struct {
			Text string `json:"text"`
		}
		if err := strictDecodeJSON([]byte(line), &event); err != nil || event.Text == "" {
			continue
		}
		if len(event.Text) > maxInboxActivityLine {
			return event.Text[:maxInboxActivityLine]
		}
		return event.Text
	}
	return ""
}

func readBoundedTail(path string, readLimit, seekLimit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	offset := int64(0)
	if info.Size() > seekLimit {
		offset = info.Size() - seekLimit
	}
	if _, err := file.Seek(offset, 0); err != nil {
		return nil, err
	}
	data, err := readAllBounded(file, readLimit)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		// Drop the partial leading line the tail window may have cut.
		if cut := bytes.IndexByte(data, '\n'); cut >= 0 && cut < len(data)-1 {
			data = data[cut+1:]
		}
	}
	return data, nil
}

func readAllBounded(reader io.Reader, limit int64) ([]byte, error) {
	buffer := make([]byte, 0, 1024)
	chunk := make([]byte, 4096)
	for int64(len(buffer)) < limit {
		readCount, err := reader.Read(chunk)
		buffer = append(buffer, chunk[:readCount]...)
		if err != nil {
			return buffer, nil // io.EOF and rare read ends both bound the tail
		}
	}
	return buffer, nil
}

// inboxSender is the owner-side inbox boundary the control handler uses.
type inboxSender struct {
	store *Store
	mu    sync.Mutex
}

func newInboxSender(store *Store) *inboxSender {
	return &inboxSender{store: store}
}

// resolve returns the task and its open run; a task without one refuses.
func (s *inboxSender) resolve(ctx context.Context, taskID string) (Task, Run, error) {
	task, err := s.store.GetTask(ctx, taskID)
	if err != nil {
		return Task{}, Run{}, err
	}
	if task.State != StateWorking && task.State != StateReviewing {
		return Task{}, Run{}, fmt.Errorf("task %s has no open run (state %s)", taskID, task.State)
	}
	run, err := s.store.OpenRunForTask(ctx, taskID)
	if err != nil {
		return Task{}, Run{}, fmt.Errorf("task %s has no open run", taskID)
	}
	if run.RunDir == "" {
		return Task{}, Run{}, fmt.Errorf("task %s open run %d has no run directory", taskID, run.ID)
	}
	return task, run, nil
}

// Send appends one operator message to the run inbox. A repeated id never
// delivers twice; an interrupt with a queued message's id upgrades that line
// in place unless the worker has already acknowledged it.
func (s *inboxSender) Send(ctx context.Context, taskID, kind, id, text string) (InboxReply, error) {
	if !validateInboxKind(kind) {
		return InboxReply{}, fmt.Errorf("unknown message kind %q (message or interrupt)", kind)
	}
	if err := validateInboxText(text); err != nil {
		return InboxReply{}, err
	}
	if err := validateInboxID(id); err != nil {
		return InboxReply{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, run, err := s.resolve(ctx, taskID)
	if err != nil {
		return InboxReply{}, err
	}
	reply := InboxReply{OK: true, TaskID: taskID, State: task.State, RunID: run.ID, Phase: run.Phase}
	messages, err := readInboxFile(run.RunDir)
	if err != nil {
		return InboxReply{}, err
	}
	acked, err := readInboxAcks(run.RunDir)
	if err != nil {
		return InboxReply{}, err
	}
	for index, existing := range messages {
		if existing.ID != id {
			continue
		}
		_, handled := acked[id]
		reply.MessageID, reply.Kind = existing.ID, existing.Kind
		if handled || kind != inboxKindInterrupt || existing.Kind == inboxKindInterrupt {
			// Already delivered to the worker, or nothing to change: the send is
			// a rejected duplicate, never a second delivery.
			reply.Duplicate, reply.Delivered = true, handled
			return reply, nil
		}
		messages[index].Kind = inboxKindInterrupt
		messages[index].Text = text
		if err := rewriteInboxFile(run.RunDir, messages); err != nil {
			return InboxReply{}, err
		}
		reply.Upgraded, reply.Kind, reply.Queued = true, inboxKindInterrupt, true
		return reply, nil
	}
	if len(messages) >= maxInboxMessages {
		return InboxReply{}, fmt.Errorf("run %d inbox is full (%d messages)", run.ID, len(messages))
	}
	if id == "" {
		id = newInboxID()
	}
	if err := appendInboxMessage(run.RunDir, InboxMessage{
		ID: id, Kind: kind, Text: text, CreatedAt: time.Now().UTC(),
	}); err != nil {
		return InboxReply{}, err
	}
	reply.MessageID, reply.Kind, reply.Queued = id, kind, true
	return reply, nil
}

// Withdraw removes an undelivered message so it never reaches the worker.
func (s *inboxSender) Withdraw(ctx context.Context, taskID, messageID string) (InboxReply, error) {
	if err := validateInboxID(messageID); err != nil {
		return InboxReply{}, err
	}
	if messageID == "" {
		return InboxReply{}, fmt.Errorf("message id is required to withdraw")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, run, err := s.resolve(ctx, taskID)
	if err != nil {
		return InboxReply{}, err
	}
	reply := InboxReply{OK: true, TaskID: taskID, State: task.State, RunID: run.ID, Phase: run.Phase, MessageID: messageID}
	messages, err := readInboxFile(run.RunDir)
	if err != nil {
		return InboxReply{}, err
	}
	acked, err := readInboxAcks(run.RunDir)
	if err != nil {
		return InboxReply{}, err
	}
	if _, handled := acked[messageID]; handled {
		reply.Delivered = true
		return reply, nil
	}
	kept := messages[:0]
	found := false
	for _, existing := range messages {
		if existing.ID == messageID {
			found = true
			continue
		}
		kept = append(kept, existing)
	}
	if !found {
		return reply, nil
	}
	if err := rewriteInboxFile(run.RunDir, kept); err != nil {
		return InboxReply{}, err
	}
	reply.Cancelled = true
	return reply, nil
}

// Read reports the run's inbox, acknowledgements, and last activity line.
func (s *inboxSender) Read(ctx context.Context, taskID string) (InboxReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, err := s.store.GetTask(ctx, taskID)
	if err != nil {
		return InboxReply{}, err
	}
	reply := InboxReply{OK: true, TaskID: taskID, State: task.State}
	run, err := s.store.OpenRunForTask(ctx, taskID)
	if err != nil {
		return reply, nil
	}
	if run.RunDir == "" {
		return reply, nil
	}
	reply.RunID, reply.Phase = run.ID, run.Phase
	messages, err := readInboxFile(run.RunDir)
	if err != nil {
		return InboxReply{}, err
	}
	reply.Messages = messages
	acked, err := readInboxAcks(run.RunDir)
	if err != nil {
		return InboxReply{}, err
	}
	ids := make([]string, 0, len(acked))
	for id := range acked {
		ids = append(ids, id)
	}
	sortStrings(ids)
	reply.AckedIDs = ids
	reply.LastActivity = readInboxActivity(run.RunDir)
	return reply, nil
}
