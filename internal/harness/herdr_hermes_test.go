package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Screen pieces in the shapes Hermes draws (hermes_cli/cli_stream_mixin.py and
// cli_chat_turn_mixin.py): each submitted message sits between two 40-wide accent rules,
// a streamed reply is a ╭─ ☤ Hermes ─╮ box, an error reply is a corner-less Rich panel,
// and the status bar plus composer close the screen.
var (
	hermesTestTurnRule = strings.Repeat("─", 40)
	hermesTestWideRule = strings.Repeat("─", 120)
	hermesTestChrome   = " ☤ z-ai-glm-5-3-flash │ ~56.1K/1.3M │ [░░░░░░░░░░] ~4% │ ◎ 67.6% │ ◷ 40.2s │ ↑ 115 t/s.. ─ Werkrby chat\n" +
		hermesTestWideRule + "\n❯ \n" + hermesTestWideRule + "\n"
)

func hermesTestBox(body string) string {
	return "\n╭─ ☤ Hermes " + strings.Repeat("─", 106) + "╮\n" + body + "\n╰" + strings.Repeat("─", 118) + "╯\n"
}

func hermesTestTurn(prompt string, output ...string) string {
	return "\n" + hermesTestTurnRule + "\n● " + prompt + "\n" + hermesTestTurnRule + "\n\n" + strings.Join(output, "")
}

func TestCleanHerdrTerminalOutputHermesWarmPaneReturnsNewestReply(t *testing.T) {
	// A warm chat pane keeps the earlier turn's reply box on screen above the new one.
	screen := hermesTestTurn("What is on my plate today?", hermesTestBox("Older reply: three tasks are open.")) +
		hermesTestTurn("And the dentist?",
			"┌─ Reasoning "+strings.Repeat("─", 105)+"┐\nThe card wakes in January.\n└"+strings.Repeat("─", 118)+"┘\n",
			hermesTestBox("Newer reply: the dentist card wakes on January 14.")) +
		hermesTestChrome
	got := cleanHerdrTerminalOutput(screen)
	if want := "Newer reply: the dentist card wakes on January 14."; got != want {
		t.Fatalf("cleanHerdrTerminalOutput(warm pane) = %q, want %q", got, want)
	}
}

func TestCleanHerdrTerminalOutputHermesTurnAnswerIsLastBox(t *testing.T) {
	// One turn draws a box before a tool call and another for the answer.
	screen := hermesTestTurn("Check the disk.",
		hermesTestBox("Let me look at the disk first."),
		"  ┊ 💻 $         df -h /  0.4s\n",
		hermesTestBox("The root disk is 41% full.")) +
		hermesTestChrome
	got := cleanHerdrTerminalOutput(screen)
	if want := "The root disk is 41% full."; got != want {
		t.Fatalf("cleanHerdrTerminalOutput(two boxes in one turn) = %q, want %q", got, want)
	}
}

func TestCleanHerdrTerminalOutputHermesSingleReplyWithChrome(t *testing.T) {
	screen := hermesTestTurn("Reply with the single word DONE.", hermesTestBox("DONE")) + hermesTestChrome
	if got := cleanHerdrTerminalOutput(screen); got != "DONE" {
		t.Fatalf("cleanHerdrTerminalOutput(single reply) = %q, want %q", got, "DONE")
	}
}

func TestCleanHerdrTerminalOutputHermesTurnWithoutBoxNeverServesOlderReply(t *testing.T) {
	// The newest turn failed: Hermes drew its error as a corner-less panel, not a reply box.
	errorPanel := " ─ ☤ Hermes " + strings.Repeat("─", 104) + " \n\n API call failed after 3 retries: HTTP 402 insufficient credits\n\n " +
		strings.Repeat("─", 116) + " \n"
	screen := hermesTestTurn("What is on my plate today?", hermesTestBox("Older reply: three tasks are open.")) +
		hermesTestTurn("And the dentist?", errorPanel) +
		hermesTestChrome
	got := cleanHerdrTerminalOutput(screen)
	if strings.Contains(got, "Older reply") {
		t.Fatalf("cleanHerdrTerminalOutput served the previous turn's reply for a turn that drew no reply box:\n%s", got)
	}
	if !strings.Contains(got, "HTTP 402 insufficient credits") {
		t.Fatalf("cleanHerdrTerminalOutput dropped the newest turn's own output:\n%s", got)
	}
}

func TestCleanHerdrTerminalOutputHermesTurnRuleInsideReplyIsNotATurn(t *testing.T) {
	// A reply that itself prints a 40-wide rule is still one reply.
	body := "Summary first.\n" + hermesTestTurnRule + "\nDetails second."
	screen := hermesTestTurn("Summarize.", hermesTestBox(body)) + hermesTestChrome
	if got := cleanHerdrTerminalOutput(screen); got != body {
		t.Fatalf("cleanHerdrTerminalOutput(rule inside reply) = %q, want %q", got, body)
	}
}

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
