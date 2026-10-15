package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/runs"
)

func (f *shadowFixture) attemptLines() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, line := range f.lines {
		if strings.HasPrefix(line, "runs-shadow: attempts ") {
			out = append(out, line)
		}
	}
	return out
}

// seedRun puts a finished run of the document into the store, as an earlier dispatch left it.
func (f *shadowFixture) seedRun(name string, charged int, outcome runs.Outcome) {
	f.t.Helper()
	store, err := runs.Open(f.runsPath())
	if err != nil {
		f.t.Fatal(err)
	}
	defer store.Close()
	start := time.Now().Add(-time.Hour)
	id, err := store.Begin(runs.Run{Doc: name, Phase: phaseTaskImplementation, Charged: charged, Start: start})
	if err != nil {
		f.t.Fatal(err)
	}
	if err := store.End(id, outcome, 0, start.Add(time.Minute)); err != nil {
		f.t.Fatal(err)
	}
}

// Shadow logs, for every dispatch, the attempt the claim charged beside the attempt the runs imply,
// and says whether they agree.
func TestRunsShadowLogsDerivedBesideChargedAttempts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		front map[string]any
		seed  func(f *shadowFixture, doc string)
		want  string
	}{
		{
			name:  "first run seen",
			front: nil,
			want:  "attempts unseeded doc=%s phase=task_implementation charged=1 derived=n/a previous=none",
		},
		{
			name:  "failed attempt then the next",
			front: map[string]any{"attempt": 1},
			seed:  func(f *shadowFixture, doc string) { f.seedRun(doc, 1, runs.Fail) },
			want:  "attempts match doc=%s phase=task_implementation charged=2 derived=2 previous=fail",
		},
		{
			name:  "interrupted run charged anyway",
			front: map[string]any{"attempt": 1},
			seed:  func(f *shadowFixture, doc string) { f.seedRun(doc, 1, runs.Interrupted) },
			want:  "attempts disagree doc=%s phase=task_implementation charged=2 derived=1 previous=interrupted",
		},
		{
			name:  "park credit",
			front: map[string]any{"attempt": 1, "resume_credit": true},
			seed:  func(f *shadowFixture, doc string) { f.seedRun(doc, 1, runs.OK) },
			want:  "attempts refund doc=%s phase=task_implementation charged=1 derived=2 previous=ok",
		},
		{
			name:  "previous run never ended",
			front: map[string]any{"attempt": 1},
			seed: func(f *shadowFixture, doc string) {
				store, err := runs.Open(f.runsPath())
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				if _, err := store.Begin(runs.Run{Doc: doc, Phase: phaseTaskImplementation, Charged: 1, Start: time.Now()}); err != nil {
					t.Fatal(err)
				}
			},
			// the new owner closes the run it finds open as lost before the dispatch, which costs nothing
			want: "attempts disagree doc=%s phase=task_implementation charged=2 derived=1 previous=lost",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newShadowFixture(t, "")
			f.cfg.Orchestrator.RunsMode = "shadow"
			name := f.newTask("derived "+tc.name, tc.front)
			if tc.seed != nil {
				tc.seed(f, name)
			}
			m := f.manager()
			f.scanTodo(m)
			m.Wait()
			lines := f.attemptLines()
			want := strings.Replace(tc.want, "%s", name, 1)
			if len(lines) != 1 || !strings.Contains(lines[0], want) {
				t.Fatalf("attempt lines = %q, want one containing %q", lines, want)
			}
		})
	}
}

// With runs_mode off nothing is compared or logged.
func TestRunsShadowOffLogsNoAttemptComparison(t *testing.T) {
	f := newShadowFixture(t, "")
	f.newTask("off", nil)
	m := f.manager()
	f.scanTodo(m)
	m.Wait()
	if lines := f.attemptLines(); len(lines) != 0 {
		t.Fatalf("runs_mode off logged %q", lines)
	}
}
