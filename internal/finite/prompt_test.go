package finite

import (
	"strings"
	"testing"
)

func TestFinitePromptBundleDelta(t *testing.T) {
	base := PromptRequest{
		TaskID: "task-prompt", RunID: 42, Phase: PhaseImplementation,
		Domains: []string{"testing", "code", "code"}, Dynamic: "Implement the parser correction.",
	}
	full, err := BuildPrompt(base)
	if err != nil {
		t.Fatalf("new prompt: %v", err)
	}
	if !full.FullBundle || full.BundleHash == "" {
		t.Fatalf("new session did not receive a full hashed bundle: %+v", full)
		return
	}
	for _, want := range []string{"FINITE CORE", "IMPLEMENTATION", "PACK: code", "PACK: testing", "TYPED RESULT", "Implement the parser correction."} {
		if !strings.Contains(full.Text, want) {
			t.Errorf("full bundle lacks %q", want)
		}
	}
	if strings.Count(full.Text, "FINITE CORE") != 1 || strings.Count(full.Text, "PACK: code") != 1 {
		t.Errorf("static sections were duplicated")
	}

	resume := base
	resume.RunID = 43
	resume.Dynamic = "Apply the reviewer finding."
	resume.PreviousBundleHash = full.BundleHash
	delta, err := BuildPrompt(resume)
	if err != nil {
		t.Fatalf("resume prompt: %v", err)
	}
	if delta.FullBundle || delta.BundleHash != full.BundleHash {
		t.Errorf("unchanged resume was not a delta: %+v", delta)
	}
	if strings.Contains(delta.Text, "FINITE CORE") || strings.Contains(delta.Text, "TYPED RESULT") || !strings.Contains(delta.Text, "Apply the reviewer finding.") {
		t.Errorf("delta resent static context or omitted dynamics: %q", delta.Text)
	}

	for name, mutate := range map[string]func(*PromptRequest){
		"changed_hash": func(r *PromptRequest) { r.PreviousBundleHash = "different" },
		"reset":        func(r *PromptRequest) { r.ResetContext = true },
		"uncertain":    func(r *PromptRequest) { r.UncertainContext = true },
	} {
		t.Run(name, func(t *testing.T) {
			req := resume
			mutate(&req)
			got, err := BuildPrompt(req)
			if err != nil {
				t.Fatal(err)
			}
			if !got.FullBundle || !strings.Contains(got.Text, "FINITE CORE") {
				t.Errorf("context did not force full bundle: %+v", got)
			}
		})
	}
}
