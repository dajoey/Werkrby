package admission

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Explain renders a decision for `werkrby admission explain`: the verdict first, then why.
func (d Decision) Explain(asked string) string {
	var b strings.Builder
	if d.Verdict == Run {
		fmt.Fprintf(&b, "%s: RUN on %s", asked, d.Seat)
	} else {
		fmt.Fprintf(&b, "%s: WAIT at %s", asked, d.Seat)
	}
	if d.Plan != "" {
		fmt.Fprintf(&b, " (plan %s)", d.Plan)
	}
	b.WriteString("\n  reason: " + d.Reason + "\n")
	if d.Verdict == Wait && !d.Until.IsZero() {
		fmt.Fprintf(&b, "  asked again after: %s\n", d.Until.UTC().Format(time.RFC3339))
	}
	if len(d.Trail) > 0 {
		b.WriteString("  passed over:\n")
		for _, h := range d.Trail {
			plan := h.Plan
			if plan == "" {
				plan = "-"
			}
			fmt.Fprintf(&b, "    %s [%s] %s\n", h.Seat, plan, h.Note)
		}
	}
	if len(d.Inputs) > 0 {
		plans := make([]string, 0, len(d.Inputs))
		for p := range d.Inputs {
			plans = append(plans, p)
		}
		sort.Strings(plans)
		b.WriteString("  readings used:")
		for _, p := range plans {
			fmt.Fprintf(&b, " %s@%s", p, d.Inputs[p])
		}
		b.WriteString("\n")
	}
	if len(d.Unknown) > 0 {
		fmt.Fprintf(&b, "  unknown readings (treated as open in shadow): %s\n", strings.Join(d.Unknown, ", "))
	}
	return b.String()
}
