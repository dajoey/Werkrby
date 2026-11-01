//go:build windows

package finite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

const processBoundarySupported = true

const (
	jobObjectAssignProcess  = 0x0001
	jobObjectQuery          = 0x0004
	windowsErrorPartialCopy = 299
)

var (
	openJobObjectW  = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")
	isProcessInJobW = windows.NewLazySystemDLL("kernel32.dll").NewProc("IsProcessInJob")
)

type windowsSupervisorLaunch struct {
	job windows.Handle
}

type windowsSupervisorContainment struct {
	job windows.Handle
}

func newSupervisorLaunch(command *exec.Cmd, token string) (supervisorLaunch, error) {
	name, err := windows.UTF16PtrFromString(finiteJobObjectName(token))
	if err != nil {
		return nil, err
	}
	job, err := windows.CreateJobObject(nil, name)
	if err != nil {
		return nil, err
	}
	// No BREAKAWAY or SILENT_BREAKAWAY limit is enabled. The job also
	// deliberately does not use KILL_ON_JOB_CLOSE: provider-only recovery after
	// wrapper exit must remain observable instead of being hidden by job teardown.
	// Go inherits only the explicitly configured stdio handles here. The owner
	// lock remains non-inheritable through its own HANDLE_FLAG_INHERIT setting.
	command.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_SUSPENDED,
	}
	return &windowsSupervisorLaunch{job: job}, nil
}

func (l *windowsSupervisorLaunch) Started(command *exec.Cmd) error {
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(command.Process.Pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(l.job, process); err != nil {
		return fmt.Errorf("assign supervisor to run job: %w", err)
	}
	return resumeWindowsSupervisor(uint32(command.Process.Pid))
}

func (l *windowsSupervisorLaunch) Close() error {
	if l.job == 0 {
		return nil
	}
	// Owner-side containment drain: once the supervisor wrapper exits, this is
	// the last handle keeping the run job alive. Hold it until the job reports
	// no members so a provider that outlived a crashed supervisor stays inside
	// the boundary and observable while recovery finds and stops it. A query
	// error falls back to closing the handle; job absence then decides
	// liveness as before.
	for {
		pids, err := windowsJobProcessIDs(l.job)
		if err != nil || len(pids) == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	err := windows.CloseHandle(l.job)
	l.job = 0
	return err
}

func openSupervisorContainment(token string) (supervisorContainment, error) {
	// The supervisor role survives the graceful console stop aimed at its own
	// process group (Ctrl-C and Ctrl-Break both arrive as Interrupt for Go
	// processes) so the provider can drain and completion.json is still
	// written before any termination. A provider that opts in re-arms
	// delivery with signal.Notify afterwards.
	signal.Ignore(os.Interrupt)
	assignment, err := openOrCreateWindowsJob(finiteJobObjectName(token), jobObjectQuery|jobObjectAssignProcess)
	if err != nil {
		return nil, fmt.Errorf("open supervisor job containment: %w", err)
	}
	if err := ensureWindowsProcessInJob(assignment); err != nil {
		_ = windows.CloseHandle(assignment)
		return nil, fmt.Errorf("join supervisor job containment: %w", err)
	}
	// Keep the assignment handle open until the query handle exists so a
	// direct child never loses the named job between these two operations.
	job, err := openWindowsJob(finiteJobObjectName(token), jobObjectQuery)
	if err != nil {
		_ = windows.CloseHandle(assignment)
		return nil, fmt.Errorf("open supervisor job containment: %w", err)
	}
	if err := windows.CloseHandle(assignment); err != nil {
		_ = windows.CloseHandle(job)
		return nil, err
	}
	// Mark the handle inheritable for spawn paths that list it in
	// SysProcAttr.AdditionalInheritedHandles: Go children inherit only stdio
	// plus that list (syscall/exec_windows.go), so this mark alone carries no
	// handle into a provider. What contains descendants is inherited job
	// membership, and the object stays observable because the owner holds its
	// handle until the job reports no members (windowsSupervisorLaunch.Close).
	// Handing this handle to the provider spawn itself is the deferred
	// follow-up (harness spawn change).
	if err := windows.SetHandleInformation(job, windows.HANDLE_FLAG_INHERIT, windows.HANDLE_FLAG_INHERIT); err != nil {
		_ = windows.CloseHandle(job)
		return nil, fmt.Errorf("make supervisor job containment inheritable: %w", err)
	}
	return &windowsSupervisorContainment{job: job}, nil
}

func openOrCreateWindowsJob(name string, access uint32) (windows.Handle, error) {
	job, err := openWindowsJob(name, access)
	if !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		return job, err
	}
	namePointer, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}
	// CreateJobObject is idempotent for an existing name: concurrent wrappers
	// still share one job and duplicate-wrapper discovery remains authoritative.
	return windows.CreateJobObject(nil, namePointer)
}

func ensureWindowsProcessInJob(job windows.Handle) error {
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(os.Getpid()))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(process)
	var inJob int32
	result, _, callErr := isProcessInJobW.Call(uintptr(process), uintptr(job), uintptr(unsafe.Pointer(&inJob)))
	if result == 0 {
		return callErr
	}
	if inJob != 0 {
		return nil
	}
	return windows.AssignProcessToJobObject(job, process)
}

// windowsProcessInJob opens pid and checks job membership on that opened
// generation. The job PID list is a snapshot: between reading it and opening
// a PID, the listed process can exit and the PID can be reused by a process
// outside the boundary, so every opened generation is confirmed here before
// it is reported or signalled.
func windowsProcessInJob(job windows.Handle, pid int) (bool, error) {
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if windowsProcessOpenGone(err) {
		return false, os.ErrProcessDone
	}
	if err != nil {
		return false, err
	}
	defer windows.CloseHandle(process)
	var inJob int32
	result, _, callErr := isProcessInJobW.Call(uintptr(process), uintptr(job), uintptr(unsafe.Pointer(&inJob)))
	if result == 0 {
		return false, callErr
	}
	return inJob != 0, nil
}

func (c *windowsSupervisorContainment) Close() error {
	if c.job == 0 {
		return nil
	}
	err := windows.CloseHandle(c.job)
	c.job = 0
	return err
}

func resumeWindowsSupervisor(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot)
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	var threadID uint32
	for err := windows.Thread32First(snapshot, &entry); ; err = windows.Thread32Next(snapshot, &entry) {
		if err != nil {
			if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
				break
			}
			return err
		}
		if entry.OwnerProcessID != pid {
			continue
		}
		if threadID != 0 {
			return fmt.Errorf("suspended supervisor has multiple initial threads")
		}
		threadID = entry.ThreadID
	}
	if threadID == 0 {
		return fmt.Errorf("suspended supervisor thread not found")
	}
	thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, threadID)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(thread)
	previous, err := windows.ResumeThread(thread)
	if err != nil {
		return err
	}
	if previous != 1 {
		return fmt.Errorf("unexpected supervisor suspend count %d", previous)
	}
	return nil
}

func identifyProcess(pid int, runID int64, token string) (ProcessIdentity, error) {
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(pid))
	if windowsProcessOpenGone(err) {
		return ProcessIdentity{}, os.ErrProcessDone
	}
	if err != nil {
		return ProcessIdentity{}, err
	}
	defer windows.CloseHandle(process)
	start, err := windowsProcessStart(process)
	if err != nil {
		return ProcessIdentity{}, err
	}
	return ProcessIdentity{RunID: runID, Token: token, PID: pid, Start: start}, nil
}

func inspectRun(ctx context.Context, runID int64, token string) (RunProcesses, error) {
	unknown := func(err error) (RunProcesses, error) {
		return RunProcesses{Liveness: LivenessUnknown}, err
	}
	observation := RunProcesses{Liveness: LivenessAbsent}
	seen := map[ProcessIdentity]struct{}{}
	addProcess := func(identity ProcessIdentity, supervisor bool) error {
		if supervisor {
			if observation.Supervisor != nil && !observation.Supervisor.Equal(identity) {
				return ErrDuplicateSupervisor
			}
			matched := identity
			observation.Supervisor = &matched
		}
		if _, exists := seen[identity]; !exists {
			seen[identity] = struct{}{}
			observation.Processes = append(observation.Processes, identity)
		}
		return nil
	}
	job, err := openWindowsJob(finiteJobObjectName(token), jobObjectQuery)
	if err == nil {
		defer windows.CloseHandle(job)
		pids, err := windowsJobProcessIDs(job)
		if err != nil {
			return unknown(err)
		}
		for _, pid := range pids {
			if err := ctx.Err(); err != nil {
				return unknown(err)
			}
			identity, err := identifyProcess(int(pid), runID, token)
			if errors.Is(err, os.ErrProcessDone) {
				continue
			}
			if err != nil {
				return unknown(fmt.Errorf("inspect job PID %d: %w", pid, err))
			}
			// Confirm the opened generation on the shared job handle before it
			// can join the observation: a reused PID never becomes a run
			// process, so it can never be signalled later.
			inJob, err := windowsProcessInJob(job, int(pid))
			if errors.Is(err, os.ErrProcessDone) {
				continue
			}
			if err != nil {
				return unknown(fmt.Errorf("inspect job PID %d membership: %w", pid, err))
			}
			if !inJob {
				continue
			}
			args, err := readWindowsCommandLine(int(pid))
			if errors.Is(err, os.ErrProcessDone) {
				continue
			}
			if err != nil {
				return unknown(fmt.Errorf("read job PID %d command line: %w", pid, err))
			}
			supervisor, err := windowsExactSupervisor(args, runID, token)
			if err != nil {
				return unknown(err)
			}
			if err := addProcess(identity, supervisor); err != nil {
				return unknown(err)
			}
		}
	} else if !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		return unknown(err)
	}
	wrappers, err := windowsSupervisorProcesses(ctx, runID, token)
	if err != nil {
		return unknown(err)
	}
	for _, identity := range wrappers {
		if err := addProcess(identity, true); err != nil {
			return unknown(err)
		}
	}
	if len(observation.Processes) != 0 {
		observation.Liveness = LivenessLive
	}
	return observation, nil
}

func windowsExactSupervisor(args []string, runID int64, token string) (bool, error) {
	if len(args) < 2 || args[1] != "supervise" {
		return false, nil
	}
	binding, err := parseSupervisorArgs(args[1:])
	if err != nil {
		return false, err
	}
	if binding.runID == runID && binding.token != token || binding.token == token && binding.runID != runID {
		return false, ErrProcessTokenMismatch
	}
	return binding.runID == runID && binding.token == token, nil
}

func windowsSupervisorProcesses(ctx context.Context, runID int64, token string) ([]ProcessIdentity, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	base := filepath.Base(executable)
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(snapshot)
	entry := windows.ProcessEntry32{Size: uint32(unsafe.Sizeof(windows.ProcessEntry32{}))}
	var identities []ProcessIdentity
	for err := windows.Process32First(snapshot, &entry); ; err = windows.Process32Next(snapshot, &entry) {
		if err != nil {
			if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
				break
			}
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !strings.EqualFold(windows.UTF16ToString(entry.ExeFile[:]), base) {
			continue
		}
		pid := int(entry.ProcessID)
		identity, err := identifyProcess(pid, runID, token)
		if errors.Is(err, os.ErrProcessDone) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect supervisor candidate PID %d: %w", pid, err)
		}
		args, err := readWindowsCommandLine(pid)
		if errors.Is(err, os.ErrProcessDone) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read supervisor candidate PID %d command line: %w", pid, err)
		}
		supervisor, err := windowsExactSupervisor(args, runID, token)
		if err != nil {
			return nil, err
		}
		if supervisor {
			identities = append(identities, identity)
		}
	}
	return identities, nil
}

func windowsJobProcessIDs(job windows.Handle) ([]uint32, error) {
	count := 16
	for attempt := 0; attempt < 8; attempt++ {
		headerSize := 8
		buffer := make([]byte, headerSize+count*int(unsafe.Sizeof(uintptr(0))))
		var returned uint32
		err := windows.QueryInformationJobObject(job, windows.JobObjectBasicProcessIdList, uintptr(unsafe.Pointer(&buffer[0])), uint32(len(buffer)), &returned)
		if err != nil {
			return nil, err
		}
		assigned := *(*uint32)(unsafe.Pointer(&buffer[0]))
		listed := *(*uint32)(unsafe.Pointer(&buffer[4]))
		if assigned > listed {
			count = int(assigned) + 16
			continue
		}
		pids := make([]uint32, 0, listed)
		for index := 0; index < int(listed); index++ {
			offset := headerSize + index*int(unsafe.Sizeof(uintptr(0)))
			value := *(*uintptr)(unsafe.Pointer(&buffer[offset]))
			if value != 0 && value <= uintptr(^uint32(0)) {
				pids = append(pids, uint32(value))
			}
		}
		return pids, nil
	}
	return nil, fmt.Errorf("job process list kept growing during discovery")
}

func readWindowsCommandLine(pid int) ([]string, error) {
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.PROCESS_VM_READ|windows.SYNCHRONIZE, false, uint32(pid))
	if windowsProcessOpenGone(err) {
		return nil, os.ErrProcessDone
	}
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(process)
	var basic windows.PROCESS_BASIC_INFORMATION
	var returned uint32
	if err := windows.NtQueryInformationProcess(process, 0, unsafe.Pointer(&basic), uint32(unsafe.Sizeof(basic)), &returned); err != nil {
		return nil, windowsCommandLineReadError(process, err)
	}
	var peb windows.PEB
	if err := readWindowsMemory(process, uintptr(unsafe.Pointer(basic.PebBaseAddress)), unsafe.Pointer(&peb), unsafe.Sizeof(peb)); err != nil {
		return nil, windowsCommandLineReadError(process, err)
	}
	var parameters windows.RTL_USER_PROCESS_PARAMETERS
	if err := readWindowsMemory(process, uintptr(unsafe.Pointer(peb.ProcessParameters)), unsafe.Pointer(&parameters), unsafe.Sizeof(parameters)); err != nil {
		return nil, windowsCommandLineReadError(process, err)
	}
	commandLine := parameters.CommandLine
	if commandLine.Length == 0 {
		event, waitErr := windows.WaitForSingleObject(process, 0)
		if waitErr == nil && event == windows.WAIT_OBJECT_0 {
			return nil, os.ErrProcessDone
		}
		// Early process initialization can expose an empty command line before
		// the supervisor protocol has opened its job. That is never absence.
		return nil, fmt.Errorf("live process command line is not initialized")
	}
	if commandLine.Length > 64*1024-2 || commandLine.Length%2 != 0 || commandLine.Buffer == nil {
		return nil, fmt.Errorf("invalid Windows command-line length %d", commandLine.Length)
	}
	units := make([]uint16, int(commandLine.Length)/2)
	if err := readWindowsMemory(process, uintptr(unsafe.Pointer(commandLine.Buffer)), unsafe.Pointer(&units[0]), uintptr(commandLine.Length)); err != nil {
		return nil, windowsCommandLineReadError(process, err)
	}
	text := windows.UTF16ToString(units)
	textPointer, err := windows.UTF16PtrFromString(text)
	if err != nil {
		return nil, err
	}
	var argc int32
	argv, err := windows.CommandLineToArgv(textPointer, &argc)
	if err != nil {
		return nil, err
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(argv)))
	if argc < 0 || argc > 4096 {
		return nil, fmt.Errorf("invalid Windows command-line argument count %d", argc)
	}
	args := make([]string, 0, argc)
	for _, pointer := range argv[:argc] {
		args = append(args, windows.UTF16ToString(pointer[:]))
	}
	return args, nil
}

func readWindowsMemory(process windows.Handle, address uintptr, destination unsafe.Pointer, size uintptr) error {
	if address == 0 || size == 0 {
		return fmt.Errorf("invalid remote process memory address")
	}
	var read uintptr
	if err := windows.ReadProcessMemory(process, address, (*byte)(destination), size, &read); err != nil {
		return err
	}
	if read != size {
		return fmt.Errorf("short remote process memory read: %d of %d", read, size)
	}
	return nil
}

func windowsCommandLineReadError(process windows.Handle, err error) error {
	if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return err
	}
	waitMilliseconds := uint32(0)
	if errors.Is(err, windows.Errno(windowsErrorPartialCopy)) {
		// An exiting process can tear down its address space after the job list
		// is read. Wait briefly for confirmed exit; a live unreadable process
		// remains an explicit discovery error.
		waitMilliseconds = 25
	}
	event, waitErr := windows.WaitForSingleObject(process, waitMilliseconds)
	if waitErr == nil && event == windows.WAIT_OBJECT_0 {
		return os.ErrProcessDone
	}
	return err
}

// windowsProcessStart requires a handle opened with SYNCHRONIZE and
// PROCESS_QUERY_LIMITED_INFORMATION. The wait right is separate from query.
func windowsProcessStart(process windows.Handle) (string, error) {
	event, err := windows.WaitForSingleObject(process, 0)
	if err != nil {
		return "", err
	}
	if event == windows.WAIT_OBJECT_0 {
		return "", os.ErrProcessDone
	}
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(process, &creation, &exit, &kernel, &user); err != nil {
		return "", err
	}
	value := uint64(creation.HighDateTime)<<32 | uint64(creation.LowDateTime)
	return "windows:" + strconv.FormatUint(value, 10), nil
}

func windowsProcessOpenGone(err error) bool {
	return errors.Is(err, windows.ERROR_INVALID_PARAMETER)
}

func signalProcess(identity ProcessIdentity, force bool) error {
	if identity.PID <= 0 || !strings.HasPrefix(identity.Start, "windows:") {
		return ErrProcessIdentityMismatch
	}
	job, err := openWindowsJob(finiteJobObjectName(identity.Token), jobObjectQuery)
	if err != nil {
		// A run job that no longer exists has no members: this identity is
		// not inside any live boundary and must never be signalled.
		return fmt.Errorf("%w: run job %s is gone", ErrProcessIdentityMismatch, finiteJobObjectName(identity.Token))
	}
	defer windows.CloseHandle(job)
	process, err := windows.OpenProcess(windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(identity.PID))
	if windowsProcessOpenGone(err) {
		return os.ErrProcessDone
	}
	if err != nil {
		return err
	}
	defer windows.CloseHandle(process)
	start, err := windowsProcessStart(process)
	if errors.Is(err, os.ErrProcessDone) {
		return os.ErrProcessDone
	}
	if err != nil {
		return err
	}
	if start != identity.Start {
		return ErrProcessIdentityMismatch
	}
	// The start identity was recorded before this open: confirm the opened
	// generation is a member of this run job before any signal leaves here.
	inJob, err := windowsProcessInJob(job, identity.PID)
	if err != nil {
		return err
	}
	if !inJob {
		return ErrProcessIdentityMismatch
	}
	if !force {
		// Best effort: deliver the graceful console stop once per poll. When
		// no shared console exists the stop loop keeps polling and terminates
		// at the grace deadline instead.
		_ = gracefulWindowsStop(identity)
		return nil
	}
	return windows.TerminateProcess(process, 1)
}

// gracefulWindowsStop sends one console Ctrl-Break to the run's process
// group. The supervisor is the group leader (CREATE_NEW_PROCESS_GROUP); the
// group event reaches every provider sharing the group and console. The
// supervisor ignores SIGBREAK (openSupervisorContainment) so the provider can
// drain and completion.json is still written before any termination.
func gracefulWindowsStop(identity ProcessIdentity) error {
	args, err := readWindowsCommandLine(identity.PID)
	if err != nil {
		return err
	}
	supervisor, err := windowsExactSupervisor(args, identity.RunID, identity.Token)
	if err != nil || !supervisor {
		if err != nil {
			return err
		}
		return fmt.Errorf("graceful stop needs the run's process group leader")
	}
	return windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, uint32(identity.PID))
}

func openWindowsJob(name string, access uint32) (windows.Handle, error) {
	namePointer, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}
	handle, _, callErr := openJobObjectW.Call(uintptr(access), 0, uintptr(unsafe.Pointer(namePointer)))
	if handle == 0 {
		return 0, callErr
	}
	return windows.Handle(handle), nil
}
