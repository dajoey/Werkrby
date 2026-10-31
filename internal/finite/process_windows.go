//go:build windows

package finite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const processBoundarySupported = true

const jobObjectQuery = 0x0004

var (
	openJobObjectW     = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")
	commandLineToArgvW = windows.NewLazySystemDLL("shell32.dll").NewProc("CommandLineToArgvW")
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
	command.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags:    windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_SUSPENDED,
		NoInheritHandles: true,
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
	err := windows.CloseHandle(l.job)
	l.job = 0
	return err
}

func openSupervisorContainment(token string) (supervisorContainment, error) {
	job, err := openWindowsJob(finiteJobObjectName(token), jobObjectQuery)
	if err != nil {
		return nil, err
	}
	return &windowsSupervisorContainment{job: job}, nil
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
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
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
	job, err := openWindowsJob(finiteJobObjectName(token), jobObjectQuery)
	if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		return RunProcesses{Liveness: LivenessAbsent}, nil
	}
	if err != nil {
		return unknown(err)
	}
	defer windows.CloseHandle(job)
	pids, err := windowsJobProcessIDs(job)
	if err != nil {
		return unknown(err)
	}
	observation := RunProcesses{Liveness: LivenessAbsent}
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
		args, err := readWindowsCommandLine(int(pid))
		if errors.Is(err, os.ErrProcessDone) {
			continue
		}
		if err != nil {
			return unknown(fmt.Errorf("read job PID %d command line: %w", pid, err))
		}
		if len(args) >= 2 && args[1] == "supervise" {
			binding, err := parseSupervisorArgs(args[1:])
			if err != nil {
				return unknown(err)
			}
			if binding.runID == runID && binding.token != token || binding.token == token && binding.runID != runID {
				return unknown(ErrProcessTokenMismatch)
			}
			if binding.runID == runID && binding.token == token {
				if observation.Supervisor != nil {
					return unknown(ErrDuplicateSupervisor)
				}
				supervisor := identity
				observation.Supervisor = &supervisor
			}
		}
		observation.Processes = append(observation.Processes, identity)
	}
	if len(observation.Processes) != 0 {
		observation.Liveness = LivenessLive
	}
	return observation, nil
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
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.PROCESS_VM_READ, false, uint32(pid))
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
		return nil, nil
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
	argv, _, callErr := commandLineToArgvW.Call(uintptr(unsafe.Pointer(textPointer)), uintptr(unsafe.Pointer(&argc)))
	if argv == 0 {
		return nil, callErr
	}
	defer windows.LocalFree(windows.Handle(argv))
	if argc < 0 || argc > 4096 {
		return nil, fmt.Errorf("invalid Windows command-line argument count %d", argc)
	}
	pointers := unsafe.Slice((**uint16)(unsafe.Pointer(argv)), int(argc))
	args := make([]string, 0, argc)
	for _, pointer := range pointers {
		args = append(args, windows.UTF16PtrToString(pointer))
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
	event, waitErr := windows.WaitForSingleObject(process, 0)
	if waitErr == nil && event == windows.WAIT_OBJECT_0 {
		return os.ErrProcessDone
	}
	return err
}

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

func signalProcess(identity ProcessIdentity, _ bool) error {
	if identity.PID <= 0 || !strings.HasPrefix(identity.Start, "windows:") {
		return ErrProcessIdentityMismatch
	}
	process, err := windows.OpenProcess(windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(identity.PID))
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
	return windows.TerminateProcess(process, 1)
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
