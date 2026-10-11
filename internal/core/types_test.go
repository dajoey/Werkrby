package core

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSpynelASCIIUsesCanonicalFiveRowWerkrbyWelcomeLogo(t *testing.T) {
	want := `██     ██ ███████ ██████  ██   ██ ██████  ██████  ██    ██
██     ██ ██      ██   ██ ██  ██  ██   ██ ██   ██  ██  ██ 
██  █  ██ █████   ██████  █████   ██████  ██████    ████  
██ ███ ██ ██      ██   ██ ██  ██  ██   ██ ██   ██    ██   
 ███ ███  ███████ ██   ██ ██   ██ ██   ██ ██████     ██   `
	if SpynelASCII != want {
		t.Fatalf("welcome logo changed:\n%s", SpynelASCII)
	}
	rows := strings.Split(SpynelASCII, "\n")
	if len(rows) != 5 {
		t.Fatalf("welcome logo has %d rows, want 5", len(rows))
	}
	for index, row := range rows {
		if width := utf8.RuneCountInString(row); width != utf8.RuneCountInString(rows[0]) || width > 63 {
			t.Fatalf("welcome logo row %d is %d cells; rows must share one width of at most 63", index, width)
		}
	}
}
