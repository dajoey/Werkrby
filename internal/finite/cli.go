package finite

import "fmt"

// Execute dispatches the experimental finite-task command.
func Execute(args []string, version string) error {
	_ = version

	if len(args) == 0 {
		return fmt.Errorf("command required: serve, intake, status, stop, answer, supervise, or test-receiver")
	}

	switch args[0] {
	case "serve", "intake", "status", "stop", "answer", "supervise", "test-receiver":
		return fmt.Errorf("%s: not implemented", args[0])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
