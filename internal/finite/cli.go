package finite

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

const cliControlTimeout = 10 * time.Second

type serveOptions struct {
	Root           string
	Worker         string
	Seats          []string
	Harness        harness.HarnessConfig
	RosterPath     string
	ControlAddr    string
	ReceiverConfig string
	Domains        []string
	PollInterval   time.Duration
	StopGrace      time.Duration
	MaxOpenRuns    int
}

type receiverOptions struct {
	Root  string
	Addr  string
	Fault ReceiverFault
}

type cliControlClient interface {
	Intake(context.Context, string) (ControlStatus, error)
	Status(context.Context, string) (ControlStatus, error)
	Stop(context.Context, string, int64) (ControlStatus, error)
	Answer(context.Context, string, int64, string) (ControlStatus, error)
	Message(context.Context, string, string, string, string) (InboxReply, error)
	Withdraw(context.Context, string, string) (InboxReply, error)
	Ping(context.Context, string) (InboxReply, error)
}

type cliDependencies struct {
	stdout        io.Writer
	serve         func(context.Context, serveOptions) error
	controlClient func(string) (cliControlClient, error)
	runSupervisor func(context.Context, []string, harness.Factory) error
	newRunner     harness.Factory
	runReceiver   func(context.Context, receiverOptions) error
}

func defaultCLIDependencies() cliDependencies {
	return cliDependencies{
		stdout:        os.Stdout,
		serve:         runFiniteServe,
		controlClient: newCLIControlClient,
		runSupervisor: runSupervisorWithBoundary,
		newRunner:     NewRunner,
		runReceiver:   runFiniteReceiver,
	}
}

// Execute dispatches the experimental finite-task command.
func Execute(args []string, version string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return executeWithDeps(ctx, args, version, defaultCLIDependencies())
}

func executeWithDeps(ctx context.Context, args []string, version string, deps cliDependencies) error {
	_ = version
	if len(args) == 0 {
		return fmt.Errorf("command required: serve, intake, status, stop, answer, message, withdraw, ping, supervise, or test-receiver")
	}
	switch args[0] {
	case "serve":
		options, err := parseServeOptions(args[1:])
		if err != nil {
			return err
		}
		return deps.serve(ctx, options)
	case "intake", "status", "stop", "answer", "message", "withdraw", "ping":
		return executeControlCommand(ctx, args[0], args[1:], deps)
	case "supervise":
		return deps.runSupervisor(ctx, args[1:], deps.newRunner)
	case "test-receiver":
		options, err := parseReceiverOptions(args[1:])
		if err != nil {
			return err
		}
		return deps.runReceiver(ctx, options)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func newCLIFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func parseCLIFlags(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("%s: %w", flags.Name(), err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("%s accepts no positional arguments: %s", flags.Name(), strings.Join(flags.Args(), " "))
	}
	return nil
}

func cliFlagWasSet(flags *flag.FlagSet, name string) bool {
	set := false
	flags.Visit(func(current *flag.Flag) {
		if current.Name == name {
			set = true
		}
	})
	return set
}

func splitCLIOptionList(value string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("list must not be empty")
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || strings.IndexFunc(part, func(r rune) bool { return r <= 0x20 || r == 0x7f }) >= 0 {
			return nil, fmt.Errorf("list entries must be nonempty single-line names")
		}
		if _, exists := seen[part]; exists {
			return nil, fmt.Errorf("duplicate list entry %q", part)
		}
		seen[part] = struct{}{}
		result = append(result, part)
	}
	return result, nil
}

func parseServeOptions(args []string) (serveOptions, error) {
	flags := newCLIFlagSet("serve")
	options := serveOptions{ControlAddr: "127.0.0.1:0", PollInterval: 250 * time.Millisecond, StopGrace: 5 * time.Second}
	var seats, domains string
	flags.StringVar(&options.Root, "root", "", "explicit disposable finite state root")
	flags.StringVar(&options.Worker, "worker", "", "worker target directory")
	flags.StringVar(&seats, "seats", "", "comma-separated available seat names")
	flags.StringVar(&options.Harness.Name, "harness", "herdr", "harness profile name")
	flags.StringVar(&options.Harness.Model, "model", "", "harness model")
	flags.StringVar(&options.RosterPath, "roster", "", "finite seat roster YAML or JSON")
	flags.StringVar(&options.ControlAddr, "control-listen", options.ControlAddr, "loopback control listen address")
	flags.StringVar(&options.ReceiverConfig, "receiver-config", "", "private receiver rendezvous file")
	flags.StringVar(&domains, "domains", "", "comma-separated prompt domain packs")
	flags.DurationVar(&options.PollInterval, "poll-interval", options.PollInterval, "scheduler scan interval")
	flags.DurationVar(&options.StopGrace, "stop-grace", options.StopGrace, "supervisor stop grace period")
	flags.IntVar(&options.MaxOpenRuns, "max-open", 6, "most provider runs open at once (0 = no limit)")
	if err := parseCLIFlags(flags, args); err != nil {
		return serveOptions{}, err
	}
	if options.MaxOpenRuns < 0 {
		return serveOptions{}, fmt.Errorf("serve: --max-open must not be negative")
	}
	if strings.TrimSpace(options.Root) == "" || strings.TrimSpace(options.Worker) == "" {
		return serveOptions{}, fmt.Errorf("serve: --root and --worker are required")
	}
	if cliFlagWasSet(flags, "seats") && strings.TrimSpace(options.RosterPath) != "" {
		return serveOptions{}, fmt.Errorf("serve: --seats and --roster cannot be used together")
	}
	var err error
	if cliFlagWasSet(flags, "seats") {
		options.Seats, err = splitCLIOptionList(seats)
		if err != nil {
			return serveOptions{}, fmt.Errorf("serve: --seats: %w", err)
		}
	} else if strings.TrimSpace(options.RosterPath) == "" {
		options.Seats = []string{"default"}
	}
	if domains != "" {
		options.Domains, err = splitCLIOptionList(domains)
		if err != nil {
			return serveOptions{}, fmt.Errorf("serve: --domains: %w", err)
		}
	}
	if options.PollInterval <= 0 {
		return serveOptions{}, fmt.Errorf("serve: --poll-interval must be positive")
	}
	if options.StopGrace <= 0 {
		return serveOptions{}, fmt.Errorf("serve: --stop-grace must be positive")
	}
	if strings.TrimSpace(options.Harness.Name) == "" {
		return serveOptions{}, fmt.Errorf("serve: --harness must not be empty")
	}
	return options, nil
}

func parseReceiverOptions(args []string) (receiverOptions, error) {
	flags := newCLIFlagSet("test-receiver")
	options := receiverOptions{Addr: "127.0.0.1:0"}
	var fault string
	flags.StringVar(&options.Root, "root", "", "explicit disposable receiver root")
	flags.StringVar(&options.Addr, "listen", options.Addr, "loopback receiver listen address")
	flags.StringVar(&fault, "fault", "", "test-only receiver fault")
	if err := parseCLIFlags(flags, args); err != nil {
		return receiverOptions{}, err
	}
	if strings.TrimSpace(options.Root) == "" {
		return receiverOptions{}, fmt.Errorf("test-receiver: --root is required")
	}
	options.Fault = ReceiverFault(fault)
	if options.Fault != ReceiverFaultNone && options.Fault != ReceiverFaultACKLost && options.Fault != ReceiverFaultCrashAfterEffect {
		return receiverOptions{}, fmt.Errorf("test-receiver: unknown receiver fault %q", fault)
	}
	return options, nil
}

type requiredCLIVersion struct {
	value int64
	set   bool
}

func (v *requiredCLIVersion) String() string {
	if v == nil {
		return ""
	}
	return strconv.FormatInt(v.value, 10)
}

func (v *requiredCLIVersion) Set(text string) error {
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value < 0 {
		return fmt.Errorf("expected a nonnegative integer")
	}
	v.value = value
	v.set = true
	return nil
}

func executeControlCommand(ctx context.Context, command string, args []string, deps cliDependencies) error {
	flags := newCLIFlagSet(command)
	var root, taskID, sourcePath, optionID, kind, messageID, text string
	var version requiredCLIVersion
	flags.StringVar(&root, "root", "", "explicit disposable finite state root")
	switch command {
	case "intake":
		flags.StringVar(&sourcePath, "brief", "", "Markdown brief path")
	case "status":
		flags.StringVar(&taskID, "task", "", "task ID")
	case "stop":
		flags.StringVar(&taskID, "task", "", "task ID")
		flags.Var(&version, "expected-version", "expected task version")
	case "answer":
		flags.StringVar(&taskID, "task", "", "task ID")
		flags.Var(&version, "expected-version", "expected task version")
		flags.StringVar(&optionID, "option", "", "Joey answer option ID")
	case "message":
		flags.StringVar(&taskID, "task", "", "task ID")
		flags.StringVar(&kind, "kind", "message", "message kind: message or interrupt")
		flags.StringVar(&messageID, "id", "", "stable message id (deduplicated; withdrawable)")
		flags.StringVar(&text, "text", "", "message text delivered into the live run")
	case "withdraw":
		flags.StringVar(&taskID, "task", "", "task ID")
		flags.StringVar(&messageID, "id", "", "message id to withdraw before delivery")
	case "ping":
		flags.StringVar(&taskID, "task", "", "task ID")
	}
	if err := parseCLIFlags(flags, args); err != nil {
		return err
	}
	if strings.TrimSpace(root) == "" {
		return fmt.Errorf("%s: --root is required", command)
	}
	if command == "intake" && strings.TrimSpace(sourcePath) == "" {
		return fmt.Errorf("intake: --brief is required")
	}
	if command != "intake" && strings.TrimSpace(taskID) == "" {
		return fmt.Errorf("%s: --task is required", command)
	}
	if (command == "stop" || command == "answer") && !version.set {
		return fmt.Errorf("%s: --expected-version is required", command)
	}
	if command == "answer" && strings.TrimSpace(optionID) == "" {
		return fmt.Errorf("answer: --option is required")
	}
	if command == "message" && strings.TrimSpace(text) == "" {
		return fmt.Errorf("message: --text is required")
	}
	if command == "withdraw" && strings.TrimSpace(messageID) == "" {
		return fmt.Errorf("withdraw: --id is required")
	}
	client, err := deps.controlClient(root)
	if err != nil {
		return fmt.Errorf("%s: %w", command, err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, cliControlTimeout)
	defer cancel()
	var payload any
	switch command {
	case "intake":
		var status ControlStatus
		status, err = client.Intake(requestCtx, sourcePath)
		payload = status
	case "status":
		var status ControlStatus
		status, err = client.Status(requestCtx, taskID)
		payload = status
	case "stop":
		var status ControlStatus
		status, err = client.Stop(requestCtx, taskID, version.value)
		payload = status
	case "answer":
		var status ControlStatus
		status, err = client.Answer(requestCtx, taskID, version.value, optionID)
		payload = status
	case "message":
		payload, err = client.Message(requestCtx, taskID, kind, messageID, text)
	case "withdraw":
		payload, err = client.Withdraw(requestCtx, taskID, messageID)
	case "ping":
		payload, err = client.Ping(requestCtx, taskID)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", command, err)
	}
	if deps.stdout == nil {
		return fmt.Errorf("%s: output writer is not configured", command)
	}
	if err := json.NewEncoder(deps.stdout).Encode(payload); err != nil {
		return fmt.Errorf("%s: write status: %w", command, err)
	}
	return nil
}

func cliContextError(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
		return nil
	}
	return err
}
