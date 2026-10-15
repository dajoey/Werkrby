package config

import (
	"fmt"
	"strings"
	"time"
	_ "time/tzdata" // the window's zone must resolve on a host with no zoneinfo
)

// WorkWindow keeps capacity free for work Claude cards during working hours.
// While it is active, claims that are not work cards (tasks the Helm pump marks
// claude_only) may use at most max_parallel minus ReservedSlots slots, and work
// cards claim ahead of everything else. Running work is never stopped. A blank
// field takes its default; a disabled window has no effect and is not validated.
type WorkWindow struct {
	Enabled       bool     `yaml:"enabled"`
	Days          []string `yaml:"days,omitempty"`           // mon..sun; blank means Mon-Fri
	Start         string   `yaml:"start,omitempty"`          // HH:MM, inclusive; blank means 09:00
	End           string   `yaml:"end,omitempty"`            // HH:MM, exclusive; blank means 18:00
	Timezone      string   `yaml:"timezone,omitempty"`       // IANA name; required while enabled
	ReservedSlots int      `yaml:"reserved_slots,omitempty"` // blank or 0 means 1
}

const (
	workWindowDefaultStart = "09:00"
	workWindowDefaultEnd   = "18:00"
)

var workWindowWeekdays = map[string]time.Weekday{
	"sun": time.Sunday, "mon": time.Monday, "tue": time.Tuesday, "wed": time.Wednesday,
	"thu": time.Thursday, "fri": time.Friday, "sat": time.Saturday,
}

// Reserved is the number of slots kept free for work cards while the window is active.
func (w WorkWindow) Reserved() int {
	if w.ReservedSlots <= 0 {
		return 1
	}
	return w.ReservedSlots
}

// Active reports whether now falls inside the window. A disabled or invalid
// window is never active.
func (w WorkWindow) Active(now time.Time) bool {
	if !w.Enabled {
		return false
	}
	location, startMinutes, endMinutes, days, err := w.resolve()
	if err != nil {
		return false
	}
	local := now.In(location)
	if !days[local.Weekday()] {
		return false
	}
	minutes := local.Hour()*60 + local.Minute()
	return minutes >= startMinutes && minutes < endMinutes
}

func (w WorkWindow) validate() []string {
	if !w.Enabled {
		return nil
	}
	var problems []string
	if w.ReservedSlots < 0 {
		problems = append(problems, "orchestrator.work_window.reserved_slots cannot be negative")
	}
	if strings.TrimSpace(w.Timezone) == "" {
		problems = append(problems, "orchestrator.work_window.timezone is required (an IANA name such as America/New_York)")
	}
	if _, _, _, _, err := w.resolve(); err != nil {
		problems = append(problems, err.Error())
	}
	return problems
}

func (w WorkWindow) resolve() (*time.Location, int, int, map[time.Weekday]bool, error) {
	location := time.Local
	if zone := strings.TrimSpace(w.Timezone); zone != "" {
		loaded, err := time.LoadLocation(zone)
		if err != nil {
			return nil, 0, 0, nil, fmt.Errorf("orchestrator.work_window.timezone %q is not a known time zone", zone)
		}
		location = loaded
	}
	start, err := clockMinutes(withDefault(w.Start, workWindowDefaultStart))
	if err != nil {
		return nil, 0, 0, nil, fmt.Errorf("orchestrator.work_window.start must be HH:MM")
	}
	end, err := clockMinutes(withDefault(w.End, workWindowDefaultEnd))
	if err != nil {
		return nil, 0, 0, nil, fmt.Errorf("orchestrator.work_window.end must be HH:MM")
	}
	if end <= start {
		return nil, 0, 0, nil, fmt.Errorf("orchestrator.work_window.end must be after orchestrator.work_window.start")
	}
	days := map[time.Weekday]bool{}
	if len(w.Days) == 0 {
		for _, name := range []string{"mon", "tue", "wed", "thu", "fri"} {
			days[workWindowWeekdays[name]] = true
		}
	}
	for _, name := range w.Days {
		day, ok := workWindowWeekdays[strings.ToLower(strings.TrimSpace(name))]
		if !ok {
			return nil, 0, 0, nil, fmt.Errorf("orchestrator.work_window.days has %q, want mon, tue, wed, thu, fri, sat or sun", name)
		}
		days[day] = true
	}
	return location, start, end, days, nil
}

func withDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func clockMinutes(value string) (int, error) {
	parsed, err := time.Parse("15:04", value)
	if err != nil {
		return 0, err
	}
	return parsed.Hour()*60 + parsed.Minute(), nil
}
