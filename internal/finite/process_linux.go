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
	"syscall"

	"golang.org/x/sys/unix"
)

const processBoundarySupported = true

var errLinuxProcessGone = errors.New("linux process exited")

type linuxProcess struct {
	pid    int
	pgrp   int
	start  string
	state  byte
	uid    int
	args   []string
	env    map[string]string
	envErr error
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
	process, err := readLinuxProcess(pid, false)
	if errors.Is(err, errLinuxProcessGone) {
		return ProcessIdentity{}, os.ErrProcessDone
	}
	if err != nil {
		return ProcessIdentity{}, err
	}
	if process.state == 'Z' || process.state == 'X' || process.state == 'x' {
		return ProcessIdentity{}, os.ErrProcessDone
	}
	return ProcessIdentity{RunID: runID, Token: token, PID: pid, Start: "linux:" + process.start}, nil
}

func inspectRun(ctx context.Context, runID int64, token string) (RunProcesses, error) {
	unknown := func(err error) (RunProcesses, error) {
		return RunProcesses{Liveness: LivenessUnknown}, err
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return unknown(err)
	}
	processes := make([]*linuxProcess, 0, len(entries))
	var supervisors []ProcessIdentity
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return unknown(err)
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		process, err := readLinuxProcess(pid, true)
		if errors.Is(err, errLinuxProcessGone) {
			continue
		}
		if err != nil {
			return unknown(fmt.Errorf("inspect PID %d: %w", pid, err))
		}
		if process == nil || process.uid != os.Getuid() || process.state == 'Z' || process.state == 'X' || process.state == 'x' {
			continue
		}
		processes = append(processes, process)
		if process.envErr != nil {
			return unknown(fmt.Errorf("inspect PID %d environment: %w", pid, process.envErr))
		}
		binding, isSupervisor, err := linuxSupervisorBinding(process.args)
		if err != nil {
			return unknown(fmt.Errorf("inspect PID %d supervisor arguments: %w", pid, err))
		}
		if !isSupervisor {
			continue
		}
		if binding.runID == runID && binding.token != token || binding.token == token && binding.runID != runID {
			return unknown(ErrProcessTokenMismatch)
		}
		if binding.runID == runID && binding.token == token {
			supervisors = append(supervisors, ProcessIdentity{RunID: runID, Token: token, PID: pid, Start: "linux:" + process.start})
		}
	}
	if len(supervisors) > 1 {
		return unknown(ErrDuplicateSupervisor)
	}
	observation := RunProcesses{Liveness: LivenessAbsent}
	if len(supervisors) == 1 {
		supervisor := supervisors[0]
		observation.Supervisor = &supervisor
	}
	for _, process := range processes {
		identity := ProcessIdentity{RunID: runID, Token: token, PID: process.pid, Start: "linux:" + process.start}
		matches := observation.Supervisor != nil && observation.Supervisor.PID == process.pid
		if observation.Supervisor != nil && process.pgrp == observation.Supervisor.PID {
			matches = true
		}
		envRunID, hasRunID := process.env[finiteRunIDEnv]
		envToken, hasToken := process.env[finiteRunTokenEnv]
		if hasRunID || hasToken {
			runMatches := hasRunID && envRunID == strconv.FormatInt(runID, 10)
			tokenMatches := hasToken && envToken == token
			if runMatches != tokenMatches {
				return unknown(ErrProcessTokenMismatch)
			}
			if runMatches && tokenMatches {
				matches = true
			}
		}
		if matches {
			observation.Processes = append(observation.Processes, identity)
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

func linuxSupervisorBinding(args []string) (supervisorBinding, bool, error) {
	if len(args) < 2 || args[1] != "supervise" {
		return supervisorBinding{}, false, nil
	}
	binding, err := parseSupervisorArgs(args[1:])
	return binding, true, err
}

func readLinuxProcess(pid int, withFiles bool) (*linuxProcess, error) {
	root := filepath.Join("/proc", strconv.Itoa(pid))
	stat, err := readLinuxBounded(filepath.Join(root, "stat"), 64*1024)
	if err != nil {
		return nil, linuxProcessReadError(err)
	}
	closing := bytes.LastIndexByte(stat, ')')
	if closing < 0 || closing+2 >= len(stat) {
		return nil, fmt.Errorf("malformed /proc stat")
	}
	fields := strings.Fields(string(stat[closing+2:]))
	if len(fields) <= 19 {
		return nil, fmt.Errorf("short /proc stat")
	}
	pgrp, err := strconv.Atoi(fields[2])
	if err != nil {
		return nil, err
	}
	process := &linuxProcess{pid: pid, pgrp: pgrp, start: fields[19], state: stat[closing+2], env: map[string]string{}}
	status, err := readLinuxBounded(filepath.Join(root, "status"), 1024*1024)
	if err != nil {
		return nil, linuxProcessReadError(err)
	}
	process.uid = -1
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "Uid:") {
			values := strings.Fields(line)
			if len(values) < 2 {
				return nil, fmt.Errorf("malformed /proc status UID")
			}
			process.uid, err = strconv.Atoi(values[1])
			if err != nil {
				return nil, err
			}
			break
		}
	}
	if process.uid < 0 {
		return nil, fmt.Errorf("missing /proc status UID")
	}
	if !withFiles {
		return process, nil
	}
	cmdline, err := readLinuxBounded(filepath.Join(root, "cmdline"), 4*1024*1024)
	if err != nil {
		return nil, linuxProcessReadError(err)
	}
	for _, argument := range bytes.Split(cmdline, []byte{0}) {
		if len(argument) != 0 {
			process.args = append(process.args, string(argument))
		}
	}
	// PID 1 cannot be a process spawned by a finite run. Its protected
	// environment is not run evidence, but its arguments remain available for
	// the exact supervisor check above. Every eligible run process still fails
	// closed on an environment read error.
	if pid == 1 {
		return process, nil
	}
	environ, err := readLinuxBounded(filepath.Join(root, "environ"), 4*1024*1024)
	if err != nil {
		return nil, linuxProcessReadError(err)
	}
	for _, variable := range bytes.Split(environ, []byte{0}) {
		equals := bytes.IndexByte(variable, '=')
		if equals <= 0 {
			continue
		}
		key, value := string(variable[:equals]), string(variable[equals+1:])
		if key != finiteRunIDEnv && key != finiteRunTokenEnv && key != finiteRunDirEnv && key != finiteJobNameEnv {
			continue
		}
		if previous, exists := process.env[key]; exists && previous != value {
			process.envErr = fmt.Errorf("conflicting %s values", key)
			continue
		}
		process.env[key] = value
	}
	return process, nil
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
	process, err := readLinuxProcess(identity.PID, false)
	if errors.Is(err, errLinuxProcessGone) {
		return os.ErrProcessDone
	}
	if err != nil {
		return err
	}
	if process.state == 'Z' || process.state == 'X' || process.state == 'x' {
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
