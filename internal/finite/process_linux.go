//go:build linux

package finite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

const processBoundarySupported = true

var (
	errLinuxProcessGone = errors.New("linux process exited")
	// linuxDeletedSuffix marks /proc/<pid>/exe of a process whose executable
	// was unlinked or replaced, as an upgrade does.
	linuxDeletedSuffix = []byte(" (deleted)")
)

type linuxProcess struct {
	pid        int
	pgrp       int
	start      string
	startTicks uint64
	state      byte
	// finite reports that /proc/<pid>/exe is the finite executable; only such
	// processes have their argv interpreted as supervisor or provider markers.
	finite bool
	args   []string
}

func (p *linuxProcess) zombie() bool {
	return p.state == 'Z' || p.state == 'X' || p.state == 'x'
}

func (p *linuxProcess) identity(runID int64, token string) ProcessIdentity {
	return ProcessIdentity{RunID: runID, Token: token, PID: p.pid, Start: "linux:" + p.start}
}

type linuxSupervisorLaunch struct{}
type linuxSupervisorContainment struct{}

func newSupervisorLaunch(command *exec.Cmd, _ string) (supervisorLaunch, error) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return linuxSupervisorLaunch{}, nil
}

func (linuxSupervisorLaunch) Started(*exec.Cmd) error { return nil }
func (linuxSupervisorLaunch) Close() error            { return nil }

func openSupervisorContainment(string) (supervisorContainment, error) {
	return linuxSupervisorContainment{}, nil
}

func (linuxSupervisorContainment) Close() error { return nil }

func identifyProcess(pid int, runID int64, token string) (ProcessIdentity, error) {
	process, err := readLinuxStatAt(unix.AT_FDCWD, filepath.Join("/proc", strconv.Itoa(pid), "stat"), pid)
	if errors.Is(err, errLinuxProcessGone) {
		return ProcessIdentity{}, os.ErrProcessDone
	}
	if err != nil {
		return ProcessIdentity{}, err
	}
	if process.zombie() {
		return ProcessIdentity{}, os.ErrProcessDone
	}
	return process.identity(runID, token), nil
}

// linuxRecordedProcess is what the scan found at the recorded supervisor PID.
type linuxRecordedProcess struct {
	present bool
	foreign bool
	process *linuxProcess
}

type linuxSupervisorMatch struct {
	process *linuxProcess
	runDir  string
}

// inspectRunBoundary performs one complete /proc scan. Processes of other
// users are skipped after one fstatat; for the rest, /proc/<pid>/stat gives
// the process group and start generation, and argv is interpreted only when
// /proc/<pid>/exe is the finite executable or the PID is the recorded
// supervisor's.
func inspectRunBoundary(ctx context.Context, boundary RunBoundary) (RunProcesses, error) {
	unknown := func(err error) (RunProcesses, error) {
		return RunProcesses{Liveness: LivenessUnknown}, err
	}
	runID, token := boundary.RunID, boundary.Token
	executables := linuxFiniteExecutables(boundary.Executable)
	recordedPID, recordedStart := boundary.Supervisor.PID, boundary.Supervisor.Start
	uid := os.Getuid()
	proc, err := os.Open("/proc")
	if err != nil {
		return unknown(err)
	}
	defer proc.Close()
	names, err := proc.Readdirnames(-1)
	if err != nil {
		return unknown(err)
	}
	procFD := int(proc.Fd())

	var recorded linuxRecordedProcess
	var processes []*linuxProcess
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return unknown(err)
		}
		pid, err := strconv.Atoi(name)
		if err != nil || pid <= 0 {
			continue
		}
		isRecorded := pid == recordedPID
		var status unix.Stat_t
		if err := unix.Fstatat(procFD, name, &status, 0); err != nil {
			if linuxProcessReadError(err) == errLinuxProcessGone {
				continue
			}
			return unknown(fmt.Errorf("inspect PID %d: %w", pid, err))
		}
		if int(status.Uid) != uid {
			if isRecorded {
				recorded = linuxRecordedProcess{present: true, foreign: true}
			}
			continue
		}
		process, err := readLinuxStatAt(procFD, name+"/stat", pid)
		if errors.Is(err, errLinuxProcessGone) {
			continue
		}
		if err != nil {
			return unknown(fmt.Errorf("inspect PID %d: %w", pid, err))
		}
		if isRecorded {
			recorded = linuxRecordedProcess{present: true, process: process}
		}
		if process.zombie() {
			continue
		}
		process.finite, err = linuxRunsExecutable(procFD, name, executables)
		if err != nil {
			return unknown(fmt.Errorf("inspect PID %d executable: %w", pid, err))
		}
		if !process.finite && !isRecorded {
			processes = append(processes, process)
			continue
		}
		process.args, err = readLinuxCmdline(pid)
		if errors.Is(err, errLinuxProcessGone) {
			continue
		}
		if err != nil {
			return unknown(fmt.Errorf("inspect PID %d arguments: %w", pid, err))
		}
		processes = append(processes, process)
	}

	var supervisors []linuxSupervisorMatch
	var otherRunDirs []string
	for _, process := range processes {
		if !process.finite {
			continue
		}
		binding, isSupervisor, err := linuxSupervisorBinding(process.args)
		if !isSupervisor {
			continue
		}
		if err != nil {
			return unknown(fmt.Errorf("inspect PID %d supervisor arguments: %w", process.pid, err))
		}
		if binding.token != token {
			// Another run, possibly in another state root whose run IDs also
			// start at 1. It conflicts only if it claims this run's directory.
			otherRunDirs = append(otherRunDirs, binding.runDir)
			continue
		}
		if binding.runID != runID || boundary.RunDir != "" && binding.runDir != boundary.RunDir {
			return unknown(ErrProcessTokenMismatch)
		}
		supervisors = append(supervisors, linuxSupervisorMatch{process: process, runDir: binding.runDir})
	}
	if candidate := recorded.process; candidate != nil && !candidate.zombie() && !candidate.finite {
		// The recorded supervisor generation is recognised by its run binding
		// even when its executable was moved since it started.
		binding, isSupervisor, err := linuxSupervisorBinding(candidate.args)
		if isSupervisor && err == nil && binding.token == token && binding.runID == runID && (boundary.RunDir == "" || binding.runDir == boundary.RunDir) {
			supervisors = append(supervisors, linuxSupervisorMatch{process: candidate, runDir: binding.runDir})
		}
	}
	if len(supervisors) > 1 {
		return unknown(ErrDuplicateSupervisor)
	}
	runDir := boundary.RunDir
	var supervisor *ProcessIdentity
	if len(supervisors) == 1 {
		identity := supervisors[0].process.identity(runID, token)
		supervisor = &identity
		if runDir == "" {
			runDir = supervisors[0].runDir
		}
	}
	if runDir != "" {
		for _, other := range otherRunDirs {
			if other == runDir {
				return unknown(ErrProcessTokenMismatch)
			}
		}
	}

	group := 0
	if recordedPID > 0 {
		switch {
		case supervisor != nil:
			if supervisor.PID != recordedPID || recordedStart != "" && supervisor.Start != recordedStart {
				return unknown(ErrProcessIdentityMismatch)
			}
			group = recordedPID
		case !recorded.present:
			// The supervisor exited and was reaped. Its PID cannot be reused
			// while its process group still has members.
			group = recordedPID
		case recorded.foreign:
			// Another user's process holds the PID: the original group ended.
		case recorded.process.zombie():
			if recordedStart == "" || "linux:"+recorded.process.start == recordedStart {
				group = recordedPID
			}
		default:
			// A live process holds the PID without presenting this run's
			// binding. An exiting process has already released its memory,
			// so its argv reads empty.
			exiting := len(recorded.process.args) == 0
			switch {
			case recordedStart != "" && "linux:"+recorded.process.start == recordedStart:
				if !exiting {
					return unknown(fmt.Errorf("%w: recorded supervisor PID %d no longer presents run %d", ErrProcessIdentityMismatch, recordedPID, runID))
				}
				identity := recorded.process.identity(runID, token)
				supervisor, group = &identity, recordedPID
			case recordedStart == "" && exiting:
				// Without a recorded generation an exiting holder cannot be
				// told apart from the supervisor; keep its group in the run.
				group = recordedPID
			}
			// Otherwise an unrelated live process reused the PID and the
			// original group has ended.
		}
	} else if supervisor != nil {
		group = supervisor.PID
	}
	if group <= 1 || group == unix.Getpgrp() {
		group = 0
	}
	minimumStart := linuxStartTicks(recordedStart)

	observation := RunProcesses{Liveness: LivenessAbsent, Supervisor: supervisor, group: group}
	grouped := false
	for _, process := range processes {
		matches := supervisor != nil && supervisor.PID == process.pid
		inGroup := group > 0 && process.pgrp == group && process.startTicks >= minimumStart
		if process.finite {
			provider, marked, err := linuxProviderBinding(process.args)
			if err != nil {
				return unknown(fmt.Errorf("inspect PID %d provider arguments: %w", process.pid, err))
			}
			if marked && provider.token == token {
				if provider.runID != runID {
					return unknown(ErrProcessTokenMismatch)
				}
				matches = true
			}
		}
		if inGroup {
			matches, grouped = true, true
		}
		if matches {
			observation.Processes = append(observation.Processes, process.identity(runID, token))
			if !inGroup {
				observation.ungrouped = true
			}
		}
	}
	if group > 0 && !grouped && !processGroupGone(group) {
		// The group still has members this scan did not list: zombies awaiting
		// their reaper, or members whose /proc entries are not owned by this
		// user. Only a complete pass over every process can prove absence.
		members, err := linuxUnlistedGroupMembers(ctx, procFD, names, group, minimumStart, uid)
		if err != nil {
			return unknown(err)
		}
		for _, member := range members {
			observation.Processes = append(observation.Processes, member.identity(runID, token))
		}
	}
	sort.Slice(observation.Processes, func(i, j int) bool {
		if observation.Processes[i].PID == observation.Processes[j].PID {
			return observation.Processes[i].Start < observation.Processes[j].Start
		}
		return observation.Processes[i].PID < observation.Processes[j].PID
	})
	if len(observation.Processes) != 0 {
		observation.Liveness = LivenessLive
	}
	return observation, nil
}

// linuxUnlistedGroupMembers reads every process's stat to find live members
// of group that the owner-filtered pass skipped. A member running as another
// real user cannot be identified or stopped, so it is never treated as absent.
func linuxUnlistedGroupMembers(ctx context.Context, procFD int, names []string, group int, minimumStart uint64, uid int) ([]*linuxProcess, error) {
	var members []*linuxProcess
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pid, err := strconv.Atoi(name)
		if err != nil || pid <= 0 {
			continue
		}
		process, err := readLinuxStatAt(procFD, name+"/stat", pid)
		if errors.Is(err, errLinuxProcessGone) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect PID %d: %w", pid, err)
		}
		if process.zombie() || process.pgrp != group || process.startTicks < minimumStart {
			continue
		}
		realUID, err := readLinuxRealUID(pid)
		if errors.Is(err, errLinuxProcessGone) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect PID %d owner: %w", pid, err)
		}
		if realUID != uid {
			return nil, fmt.Errorf("process group %d member PID %d runs as another user", group, pid)
		}
		members = append(members, process)
	}
	return members, nil
}

func linuxSupervisorBinding(args []string) (supervisorBinding, bool, error) {
	if len(args) < 2 || args[1] != "supervise" {
		return supervisorBinding{}, false, nil
	}
	binding, err := parseSupervisorArgs(args[1:])
	return binding, true, err
}

// linuxProviderBinding recognizes only explicit run-boundary markers. A
// provider adapter that can outlive its supervisor must preserve these exact
// arguments; an unmarked daemon or external gateway is not an admitted Linux
// provider boundary.
func linuxProviderBinding(args []string) (supervisorBinding, bool, error) {
	var binding supervisorBinding
	var runIDText, token string
	seenRunID, seenToken := false, false
	for index := 1; index < len(args); index++ {
		switch args[index] {
		case finiteProviderRunIDArg:
			if seenRunID || index+1 >= len(args) {
				return supervisorBinding{}, true, fmt.Errorf("invalid provider run-ID marker")
			}
			seenRunID = true
			index++
			runIDText = args[index]
		case finiteProviderTokenArg:
			if seenToken || index+1 >= len(args) {
				return supervisorBinding{}, true, fmt.Errorf("invalid provider token marker")
			}
			seenToken = true
			index++
			token = args[index]
		}
	}
	if !seenRunID && !seenToken {
		return supervisorBinding{}, false, nil
	}
	if !seenRunID || !seenToken {
		return supervisorBinding{}, true, fmt.Errorf("incomplete provider run marker")
	}
	runID, err := strconv.ParseInt(runIDText, 10, 64)
	if err != nil {
		return supervisorBinding{}, true, err
	}
	if err := validateRunBinding(runID, token); err != nil {
		return supervisorBinding{}, true, err
	}
	binding.runID, binding.token = runID, token
	return binding, true, nil
}

var linuxSelfExecutable struct {
	once  sync.Once
	paths []string
}

// linuxFiniteExecutables lists the paths /proc/<pid>/exe may show for the
// finite binary: the configured executable and this process's own, each as
// given and with symlinks resolved.
func linuxFiniteExecutables(configured string) []string {
	linuxSelfExecutable.once.Do(func() {
		if self, err := os.Executable(); err == nil {
			linuxSelfExecutable.paths = linuxExecutablePaths(self)
		}
	})
	paths := append([]string(nil), linuxSelfExecutable.paths...)
	if configured != "" {
		paths = append(paths, linuxExecutablePaths(configured)...)
	}
	return paths
}

func linuxExecutablePaths(path string) []string {
	path = strings.TrimSuffix(path, string(linuxDeletedSuffix))
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	paths := []string{filepath.Clean(path)}
	if resolved, err := filepath.EvalSymlinks(path); err == nil && resolved != paths[0] {
		paths = append(paths, resolved)
	}
	return paths
}

// linuxRunsExecutable reports whether /proc/<pid>/exe is one of executables.
// A replaced binary reads "<path> (deleted)" and still matches its path. A
// process whose executable link is unreadable is not a finite process.
func linuxRunsExecutable(procFD int, name string, executables []string) (bool, error) {
	var buffer [unix.PathMax]byte
	length, err := unix.Readlinkat(procFD, name+"/exe", buffer[:])
	if err != nil {
		if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ESRCH) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
			return false, nil
		}
		return false, err
	}
	if length >= len(buffer) {
		return false, nil
	}
	link := bytes.TrimSuffix(buffer[:length], linuxDeletedSuffix)
	for _, executable := range executables {
		if string(link) == executable {
			return true, nil
		}
	}
	return false, nil
}

// readLinuxStatAt reads /proc/<pid>/stat relative to dirFD. A proc stat file
// is generated whole, so one read shorter than the buffer is complete.
func readLinuxStatAt(dirFD int, path string, pid int) (*linuxProcess, error) {
	fd, err := unix.Openat(dirFD, path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, linuxProcessReadError(err)
	}
	defer unix.Close(fd)
	var buffer [2048]byte
	data := buffer[:0]
	for {
		if len(data) == cap(data) {
			if cap(data) >= 64*1024 {
				return nil, fmt.Errorf("/proc stat exceeds %d bytes", 64*1024)
			}
			grown := make([]byte, len(data), 2*cap(data))
			copy(grown, data)
			data = grown
		}
		count, err := unix.Read(fd, data[len(data):cap(data)])
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return nil, linuxProcessReadError(err)
		}
		data = data[:len(data)+count]
		if count == 0 || len(data) < cap(data) {
			break
		}
	}
	return parseLinuxStat(pid, data)
}

func parseLinuxStat(pid int, stat []byte) (*linuxProcess, error) {
	closing := bytes.LastIndexByte(stat, ')')
	if closing < 0 || closing+2 >= len(stat) {
		return nil, fmt.Errorf("malformed /proc stat")
	}
	rest := stat[closing+2:]
	process := &linuxProcess{pid: pid, state: rest[0]}
	field, begin := 0, 0
	for index := 0; index <= len(rest); index++ {
		if index < len(rest) && rest[index] != ' ' && rest[index] != '\n' {
			continue
		}
		if index > begin {
			value := rest[begin:index]
			switch field {
			case 2:
				pgrp, err := strconv.Atoi(string(value))
				if err != nil {
					return nil, fmt.Errorf("malformed /proc stat process group: %w", err)
				}
				process.pgrp = pgrp
			case 19:
				ticks, err := strconv.ParseUint(string(value), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("malformed /proc stat start time: %w", err)
				}
				process.start, process.startTicks = string(value), ticks
				return process, nil
			}
			field++
		}
		begin = index + 1
	}
	return nil, fmt.Errorf("short /proc stat")
}

// linuxStartTicks parses a recorded "linux:<ticks>" start, or returns zero.
func linuxStartTicks(start string) uint64 {
	ticks, err := strconv.ParseUint(strings.TrimPrefix(start, "linux:"), 10, 64)
	if err != nil || !strings.HasPrefix(start, "linux:") {
		return 0
	}
	return ticks
}

func readLinuxCmdline(pid int) ([]string, error) {
	cmdline, err := readLinuxBounded(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"), 4*1024*1024)
	if err != nil {
		return nil, linuxProcessReadError(err)
	}
	var args []string
	for _, argument := range bytes.Split(cmdline, []byte{0}) {
		if len(argument) != 0 {
			args = append(args, string(argument))
		}
	}
	return args, nil
}

func readLinuxRealUID(pid int) (int, error) {
	status, err := readLinuxBounded(filepath.Join("/proc", strconv.Itoa(pid), "status"), 1024*1024)
	if err != nil {
		return -1, linuxProcessReadError(err)
	}
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "Uid:") {
			values := strings.Fields(line)
			if len(values) < 2 {
				return -1, fmt.Errorf("malformed /proc status UID")
			}
			return strconv.Atoi(values[1])
		}
	}
	return -1, fmt.Errorf("missing /proc status UID")
}

func readLinuxBounded(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, limit)
	}
	return data, nil
}

func linuxProcessReadError(err error) error {
	if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) {
		return errLinuxProcessGone
	}
	return err
}

// processGroupGone reports reliable absence of every member of group,
// zombies included.
func processGroupGone(group int) bool {
	if group <= 1 {
		return false
	}
	return errors.Is(unix.Kill(-group, 0), unix.ESRCH)
}

// signalProcessGroup signals every member of a verified run process group,
// including members forked after the scan. It never signals this owner's own
// group or the kill(-1) broadcast.
func signalProcessGroup(group int, force bool) error {
	if group <= 1 || group == unix.Getpgrp() {
		return fmt.Errorf("refusing to signal process group %d", group)
	}
	signal := unix.SIGTERM
	if force {
		signal = unix.SIGKILL
	}
	err := unix.Kill(-group, signal)
	if err == nil || errors.Is(err, unix.ESRCH) || errors.Is(err, unix.EPERM) {
		// EPERM: no member could be signalled; the next complete scan decides.
		return nil
	}
	return err
}

func signalProcess(identity ProcessIdentity, force bool) error {
	if identity.PID <= 0 || !strings.HasPrefix(identity.Start, "linux:") {
		return ErrProcessIdentityMismatch
	}
	fd, err := unix.PidfdOpen(identity.PID, 0)
	if errors.Is(err, unix.ESRCH) {
		return os.ErrProcessDone
	}
	if err != nil {
		return fmt.Errorf("open safe process handle: %w", err)
	}
	unix.CloseOnExec(fd)
	defer unix.Close(fd)
	process, err := readLinuxStatAt(unix.AT_FDCWD, filepath.Join("/proc", strconv.Itoa(identity.PID), "stat"), identity.PID)
	if errors.Is(err, errLinuxProcessGone) {
		return os.ErrProcessDone
	}
	if err != nil {
		return err
	}
	if process.zombie() {
		return os.ErrProcessDone
	}
	if identity.Start != "linux:"+process.start {
		return ErrProcessIdentityMismatch
	}
	signal := unix.SIGTERM
	if force {
		signal = unix.SIGKILL
	}
	if err := unix.PidfdSendSignal(fd, signal, nil, 0); err != nil {
		if errors.Is(err, unix.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	return nil
}
