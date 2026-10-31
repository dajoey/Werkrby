package finite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxResultBytes   = 256 * 1024
	MaxProgressRunes = 16000
)

// ReadResult reads and validates the result bound to run.
func ReadResult(run Run) (Result, error) {
	path := filepath.Join(run.RunDir, "result.json")
	file, err := openResultFile(path)
	if err != nil {
		return Result{}, invalidResult(err.Error())
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > MaxResultBytes {
		return Result{}, invalidResult("result is not a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, MaxResultBytes+1))
	if err != nil {
		return Result{}, invalidResult(err.Error())
	}
	return DecodeResult(run, data)
}

// DecodeResult decodes one closed result document.
func DecodeResult(run Run, data []byte) (Result, error) {
	if len(data) == 0 || len(data) > MaxResultBytes || !utf8.Valid(data) {
		return Result{}, invalidResult("result is empty, oversized, or not UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result Result
	if err := decoder.Decode(&result); err != nil {
		return Result{}, invalidResult(err.Error())
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Result{}, invalidResult("result contains trailing JSON")
	}
	if err := ValidateResult(run, result); err != nil {
		return Result{}, err
	}
	if result.Wait != nil && result.Wait.Kind == WaitJoey {
		ask, err := decodeJoeyAsk(result.Wait.Ask)
		if err != nil {
			return Result{}, err
		}
		canonical, _ := json.Marshal(ask)
		result.Wait.Ask = canonical
	}
	return result, nil
}

// ValidateResult checks schema, binding, outcome, and wait shape.
func ValidateResult(run Run, result Result) error {
	if result.SchemaVersion != 1 || result.RunID != run.ID || result.TaskID != run.TaskID || result.Phase != run.Phase {
		return invalidResult("schema version or task/run/phase binding mismatch")
	}
	if strings.TrimSpace(result.Progress) == "" || len([]rune(result.Progress)) > MaxProgressRunes {
		return invalidResult("progress is empty or exceeds the rune bound")
	}
	validOutcome := false
	if run.Phase == PhaseImplementation {
		validOutcome = result.Outcome == ResultDone || result.Outcome == ResultReview || result.Outcome == ResultTodo || result.Outcome == ResultWait || result.Outcome == ResultFailed
	} else if run.Phase == PhaseReview {
		validOutcome = result.Outcome == ResultAccepted || result.Outcome == ResultRejected || result.Outcome == ResultWait || result.Outcome == ResultFailed
	}
	if !validOutcome {
		return invalidResult("outcome is invalid for the run phase")
	}
	if result.Outcome != ResultWait {
		if result.Wait != nil {
			return invalidResult("non-wait outcome carries wait data")
		}
		return nil
	}
	if result.Wait == nil || strings.TrimSpace(result.Wait.Reason) == "" {
		return invalidResult("wait requires a reason and typed wait data")
	}
	switch result.Wait.Kind {
	case WaitTime:
		if result.Wait.WakeAt == "" || len(result.Wait.Ask) != 0 {
			return invalidResult("time wait requires wake_at and no Joey ask")
		}
		wake, err := time.Parse(time.RFC3339Nano, result.Wait.WakeAt)
		if err != nil {
			return invalidResult("invalid wake_at")
		}
		_, offset := wake.Zone()
		if offset != 0 {
			return invalidResult("wake_at must be UTC")
		}
	case WaitJoey:
		if result.Wait.WakeAt != "" {
			return invalidResult("Joey wait cannot have wake_at")
		}
		if _, err := decodeJoeyAsk(result.Wait.Ask); err != nil {
			return err
		}
	default:
		return invalidResult("unknown wait kind")
	}
	return nil
}

func decodeJoeyAsk(data []byte) (JoeyAsk, error) {
	if len(data) == 0 {
		return JoeyAsk{}, invalidResult("Joey wait requires an ask")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var ask JoeyAsk
	if err := decoder.Decode(&ask); err != nil {
		return JoeyAsk{}, invalidResult("invalid Joey ask: "+err.Error())
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return JoeyAsk{}, invalidResult("Joey ask contains trailing JSON")
	}
	if strings.TrimSpace(ask.Question) == "" || len(ask.Options) == 0 {
		return JoeyAsk{}, invalidResult("Joey ask requires one question and explicit options")
	}
	seen := map[string]bool{}
	for _, option := range ask.Options {
		if strings.TrimSpace(option.ID) == "" || strings.TrimSpace(option.Label) == "" || seen[option.ID] {
			return JoeyAsk{}, invalidResult("Joey ask options must be nonempty and unique")
		}
		seen[option.ID] = true
	}
	return ask, nil
}

func invalidResult(detail string) error {
	return fmt.Errorf("%w: %s", ErrInvalidResult, detail)
}
