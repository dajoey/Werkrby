package config

import (
	"strings"
	"testing"
	"time"
)

func newYork(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	return loc
}

func officeHours() WorkWindow {
	return WorkWindow{Enabled: true, Start: "09:00", End: "18:00", Timezone: "America/New_York", ReservedSlots: 1}
}

func TestWorkWindowActiveBoundaries(t *testing.T) {
	loc := newYork(t)
	window := officeHours()
	cases := []struct {
		name string
		at   time.Time
		want bool
	}{
		{"thursday before opening", time.Date(2026, 10, 15, 8, 59, 0, 0, loc), false},
		{"thursday at opening", time.Date(2026, 10, 15, 9, 0, 0, 0, loc), true},
		{"thursday midday", time.Date(2026, 10, 15, 12, 0, 0, 0, loc), true},
		{"thursday last minute", time.Date(2026, 10, 15, 17, 59, 0, 0, loc), true},
		{"thursday at close", time.Date(2026, 10, 15, 18, 0, 0, 0, loc), false},
		{"saturday midday", time.Date(2026, 10, 17, 12, 0, 0, 0, loc), false},
		{"sunday midday", time.Date(2026, 10, 18, 12, 0, 0, 0, loc), false},
		{"monday at opening", time.Date(2026, 10, 19, 9, 0, 0, 0, loc), true},
	}
	for _, tc := range cases {
		if got := window.Active(tc.at); got != tc.want {
			t.Errorf("%s: Active = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The window is wall-clock time in its own zone: the same UTC hour is inside
// the window in summer and outside it after the clocks go back.
func TestWorkWindowFollowsDaylightSavingTime(t *testing.T) {
	newYork(t)
	window := officeHours()
	// 2026-11-02 is the Monday after DST ended: 09:00 EST is 14:00 UTC.
	if !window.Active(time.Date(2026, 11, 2, 14, 0, 0, 0, time.UTC)) {
		t.Error("09:00 EST (14:00 UTC) should be inside the window")
	}
	if window.Active(time.Date(2026, 11, 2, 13, 59, 0, 0, time.UTC)) {
		t.Error("08:59 EST (13:59 UTC) should be outside the window")
	}
	// Before DST ended the same 13:00 UTC is 09:00 EDT.
	if !window.Active(time.Date(2026, 10, 15, 13, 0, 0, 0, time.UTC)) {
		t.Error("09:00 EDT (13:00 UTC) should be inside the window")
	}
}

func TestWorkWindowDisabledIsNeverActive(t *testing.T) {
	loc := newYork(t)
	window := officeHours()
	window.Enabled = false
	if window.Active(time.Date(2026, 10, 15, 12, 0, 0, 0, loc)) {
		t.Error("a disabled window must never be active")
	}
	if (WorkWindow{}).Active(time.Date(2026, 10, 15, 12, 0, 0, 0, loc)) {
		t.Error("the zero value must never be active")
	}
}

func TestWorkWindowCustomDays(t *testing.T) {
	loc := newYork(t)
	window := officeHours()
	window.Days = []string{"Sat", " sun "}
	if window.Active(time.Date(2026, 10, 15, 12, 0, 0, 0, loc)) {
		t.Error("thursday must be outside a sat/sun window")
	}
	if !window.Active(time.Date(2026, 10, 17, 12, 0, 0, 0, loc)) {
		t.Error("saturday must be inside a sat/sun window")
	}
}

func TestWorkWindowDefaultsFillBlankFields(t *testing.T) {
	loc := newYork(t)
	window := WorkWindow{Enabled: true, Timezone: "America/New_York"}
	if window.Reserved() != 1 {
		t.Errorf("Reserved() = %d, want the default of 1", window.Reserved())
	}
	if !window.Active(time.Date(2026, 10, 15, 12, 0, 0, 0, loc)) {
		t.Error("blank days/start/end should default to Mon-Fri 09:00-18:00")
	}
	if window.Active(time.Date(2026, 10, 17, 12, 0, 0, 0, loc)) {
		t.Error("blank days should exclude Saturday")
	}
}

func TestWorkWindowValidationRejectsBadValues(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*WorkWindow)
		want   string
	}{
		{"bad start", func(w *WorkWindow) { w.Start = "9am" }, "work_window.start"},
		{"bad end", func(w *WorkWindow) { w.End = "25:00" }, "work_window.end"},
		{"end not after start", func(w *WorkWindow) { w.Start = "18:00"; w.End = "09:00" }, "work_window.end"},
		{"bad timezone", func(w *WorkWindow) { w.Timezone = "Mars/Olympus" }, "work_window.timezone"},
		{"bad day", func(w *WorkWindow) { w.Days = []string{"monday-ish"} }, "work_window.days"},
		{"negative reserve", func(w *WorkWindow) { w.ReservedSlots = -1 }, "work_window.reserved_slots"},
	}
	for _, tc := range cases {
		cfg := Default()
		window := officeHours()
		tc.mutate(&window)
		cfg.Orchestrator.WorkWindow = window
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Validate() = %v, want an error naming %s", tc.name, err, tc.want)
		}
	}
}

// A disabled window is never validated, so a half-edited block cannot stop the
// workspace from loading.
func TestWorkWindowDisabledSkipsValidation(t *testing.T) {
	cfg := Default()
	cfg.Orchestrator.WorkWindow = WorkWindow{Enabled: false, Start: "nonsense", Timezone: "Mars/Olympus"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a disabled window must not fail validation: %v", err)
	}
}

func TestWorkWindowLoadsFromYAML(t *testing.T) {
	cfg, err := decode([]byte(`version: 1
orchestrator:
  enabled: true
  interval_seconds: 10
  max_parallel: 6
  work_window:
    enabled: true
    days: [mon, tue, wed, thu, fri]
    start: "09:00"
    end: "18:00"
    timezone: America/New_York
    reserved_slots: 1
`), "/tmp/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	window := cfg.Orchestrator.WorkWindow
	if !window.Enabled || window.Start != "09:00" || window.End != "18:00" || window.Timezone != "America/New_York" || window.ReservedSlots != 1 || len(window.Days) != 5 {
		t.Fatalf("work_window did not load: %+v", window)
	}
}
