package admission

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"time"
)

// Compat reading rules, taken from the quota gate so the shadow verdict is comparable with today's.
// Q1 moves them into policy data.
const (
	defaultFullAt = 100.0
	claudeFullAt  = 95.0 // Claude Code blocks at 100 and the collector reads every 15 minutes
	unbenchBelow  = 90.0 // a window that read full stays full until it reads under this or resets
	staleMax      = 2 * time.Hour
)

type window struct {
	Pct      *float64
	ResetsAt time.Time
}

// reading is one plan's entry in the usage collector's snapshot.
type reading struct {
	OK      bool
	AsOf    time.Time
	Closed  bool // the collector's own state says the plan is closed ("closed: credit spent")
	Windows map[string]window
}

type snapshot struct {
	GeneratedAt string                    `json:"generated_at"`
	Plans       map[string]map[string]any `json:"plans"`
	Claude      map[string]any            `json:"claude"`
}

// loadReadings reads the collector snapshot. The Claude plan sits beside "plans" in the file;
// its seven_day window is called weekly, as the gate calls it.
func loadReadings(path string) (map[string]reading, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	fallbackAsOf := parseTime(snap.GeneratedAt)
	out := map[string]reading{}
	for name, entry := range snap.Plans {
		out[name] = parseReading(entry, fallbackAsOf)
	}
	if snap.Claude != nil {
		entry := map[string]any{"ok": snap.Claude["ok"], "as_of": snap.Claude["as_of"], "five_hour": snap.Claude["five_hour"], "weekly": snap.Claude["seven_day"]}
		out["claude"] = parseReading(entry, fallbackAsOf)
	}
	return out, nil
}

func parseReading(entry map[string]any, fallbackAsOf time.Time) reading {
	r := reading{Windows: map[string]window{}}
	r.OK, _ = entry["ok"].(bool)
	if s, _ := entry["as_of"].(string); s != "" {
		r.AsOf = parseTime(s)
	}
	if r.AsOf.IsZero() {
		r.AsOf = fallbackAsOf
	}
	if state, _ := entry["state"].(string); strings.HasPrefix(state, "closed") {
		r.Closed = true
	}
	for name, v := range entry {
		w, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if _, has := w["pct"]; !has {
			continue
		}
		win := window{}
		if pct, ok := w["pct"].(float64); ok {
			win.Pct = &pct
		}
		if s, _ := w["resets_at"].(string); s != "" {
			win.ResetsAt = parseTime(s)
		}
		r.Windows[name] = win
	}
	return r
}

func parseTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func fullAtFor(plan string) float64 {
	if plan == "claude" {
		return claudeFullAt
	}
	return defaultFullAt
}

// planState is what the readings say about one plan now.
type planState struct {
	Full    bool
	Until   time.Time // latest reset among the full windows; zero when none is known
	Unknown bool      // no usable reading: collector failed, nothing numeric, or older than staleMax
	AsOf    time.Time
	Why     string // one line for Full: which windows
}

// state judges a plan. held carries each window that read full on an earlier call (plan/window ->
// resets_at); it is updated in place.
func state(plan string, readings map[string]reading, held map[string]time.Time, now time.Time) planState {
	rd, ok := readings[plan]
	if !ok {
		return planState{Unknown: true}
	}
	st := planState{AsOf: rd.AsOf}
	if !rd.AsOf.IsZero() && now.Sub(rd.AsOf) > staleMax {
		st.Unknown = true
		return st
	}
	fullAt := fullAtFor(plan)
	var windows []string
	numeric := false
	for name, w := range rd.Windows {
		key := plan + "/" + name
		reset, wasHeld := held[key]
		if w.Pct != nil {
			numeric = true
		}
		full := false
		switch {
		case w.Pct != nil && *w.Pct >= fullAt:
			full = true
			held[key] = w.ResetsAt
			reset = w.ResetsAt
		case wasHeld:
			over := !reset.IsZero() && !now.Before(reset)
			switch {
			case over, w.Pct != nil && *w.Pct < unbenchBelow:
				delete(held, key)
			case w.Pct == nil && reset.IsZero():
				delete(held, key)
			default:
				full = true
			}
		}
		if full {
			windows = append(windows, name)
			if reset.After(st.Until) {
				st.Until = reset
			}
		}
	}
	if rd.Closed {
		st.Full, st.Why = true, "collector state is closed"
		return st
	}
	switch {
	case len(windows) > 0:
		sort.Strings(windows)
		st.Full = true
		st.Why = "window " + strings.Join(windows, ", ") + " full"
	case !numeric:
		st.Unknown = true
	}
	return st
}
