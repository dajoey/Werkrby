package finite

import (
	"encoding/json"
	"testing"
)

// Owner clients that report a task to a human need its title, what it waits
// for, and the exact typed question; none of these are secret.
func TestControlStatusCarriesTitleWaitAndAsk(t *testing.T) {
	ask := `{"question":"Ship it?","options":[{"id":"approve","label":"Approve"},{"id":"deny","label":"Deny"}]}`
	got := controlStatus(Task{ID: "t1", Title: "Fix the thing", State: StateWaiting, WaitKind: WaitJoey, WaitingFor: "needs a go", JoeyAskJSON: ask})
	if got.Title != "Fix the thing" || got.WaitingFor != "needs a go" {
		t.Fatalf("status = %+v", got)
	}
	var decoded JoeyAsk
	if err := json.Unmarshal(got.JoeyAsk, &decoded); err != nil || decoded.Question != "Ship it?" || len(decoded.Options) != 2 {
		t.Fatalf("joey ask = %s, %v", got.JoeyAsk, err)
	}
	if plain := controlStatus(Task{ID: "t2", Title: "x", State: StateTodo}); len(plain.JoeyAsk) != 0 || plain.WaitingFor != "" {
		t.Fatalf("non-waiting status carried wait data: %+v", plain)
	}
}
