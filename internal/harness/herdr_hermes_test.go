package harness

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHermesComposerDetectionStartupFixtures(t *testing.T) {
	testDir := filepath.Join("testdata", "hermes-startup")

	welcomeBytes, err := os.ReadFile(filepath.Join(testDir, "welcome-line-no-composer.txt"))
	if err != nil {
		t.Fatalf("failed to read welcome-line-no-composer.txt: %v", err)
	}
	if isRunnerComposerVisible("hermes", string(welcomeBytes)) {
		t.Errorf("isRunnerComposerVisible returned true for welcome-line-no-composer.txt, want false (welcome line is not the composer)")
	}

	tipBytes, err := os.ReadFile(filepath.Join(testDir, "tip-line-no-composer.txt"))
	if err != nil {
		t.Fatalf("failed to read tip-line-no-composer.txt: %v", err)
	}
	if isRunnerComposerVisible("hermes", string(tipBytes)) {
		t.Errorf("isRunnerComposerVisible returned true for tip-line-no-composer.txt, want false (tip line without composer is not ready)")
	}

	composerBytes, err := os.ReadFile(filepath.Join(testDir, "composer-up.txt"))
	if err != nil {
		t.Fatalf("failed to read composer-up.txt: %v", err)
	}
	if !isRunnerComposerVisible("hermes", string(composerBytes)) {
		t.Errorf("isRunnerComposerVisible returned false for composer-up.txt, want true (composer is on screen)")
	}
}

func TestHermesBoxPatternGlyphsAndCleanup(t *testing.T) {
	// Current Hermes (v0.21.5+) draws U+2624 (☤)
	newBox := "╭─ ☤ Hermes ───────────────────────────────────────────────────────────────────────────────────────────────────────────╮\nTask completed successfully.\n╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯"
	if !hermesBoxPattern.MatchString(newBox) {
		t.Errorf("hermesBoxPattern did not match modern Hermes box with glyph ☤ (U+2624)")
	}
	gotClean := cleanHerdrTerminalOutput(newBox)
	wantClean := "Task completed successfully."
	if gotClean != wantClean {
		t.Errorf("cleanHerdrTerminalOutput(newBox) = %q, want %q", gotClean, wantClean)
	}

	// Legacy Hermes drew U+2695 (⚕)
	legacyBox := "╭─ ⚕ Hermes ───────────────────────────────────────────────────────────────────────────────────────────────────────────╮\nLegacy response.\n╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯"
	if !hermesBoxPattern.MatchString(legacyBox) {
		t.Errorf("hermesBoxPattern did not match legacy Hermes box with glyph ⚕ (U+2695)")
	}
	gotLegacyClean := cleanHerdrTerminalOutput(legacyBox)
	wantLegacyClean := "Legacy response."
	if gotLegacyClean != wantLegacyClean {
		t.Errorf("cleanHerdrTerminalOutput(legacyBox) = %q, want %q", gotLegacyClean, wantLegacyClean)
	}
}

func TestHermesComposerWorkingVsIdle(t *testing.T) {
	workingPlaceholder := `
 ☤ z-ai-glm-5-3-flash │ 56.4K/1.3M │ [░░░░░░░░░░] 4% │ ◎ 73.6% │ ◷ 33.8s │ ↑ 114 t/s │ . ─ Harness timing test sequence
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
☤ ❯ msg=interrupt · /queue · /bg · /steer · Ctrl+C cancel
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────`
	if isRunnerComposerVisible("hermes", workingPlaceholder) {
		t.Errorf("isRunnerComposerVisible returned true for working placeholder screen with msg=interrupt, want false")
	}

	idlePastedPrompt := `
 ☤ z-ai-glm-5-3-flash │ ctx -- │ [░░░░░░░░░░] -- │ 21s │ ⏲ 0s
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
❯ This is a harness timing test in a scratch directory; do exactly this and nothing else.
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────`
	if !isRunnerComposerVisible("hermes", idlePastedPrompt) {
		t.Errorf("isRunnerComposerVisible returned false for idle pasted prompt, want true")
	}

	scrolledInput := `
 ☤ glm-5.3-flash │ ctx -- │ [░░░░░░░░░░] -- │ 56s │ ⏲ 0s
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
- filler line 395: this line only makes the prompt long, like a Werkrby task prompt; ignore it.
- filler line 396: this line only makes the prompt long, like a Werkrby task prompt; ignore it.
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────`
	if !isRunnerComposerVisible("hermes", scrolledInput) {
		t.Errorf("isRunnerComposerVisible returned false for scrolled input composer, want true")
	}
}
