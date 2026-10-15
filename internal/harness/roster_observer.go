package harness

import "sync/atomic"

// RosterObserver watches the staffing of chat, notification and heartbeat turns (rosterModelForKey)
// without changing it. It is asked once per turn, before the roster counts the session, with the role,
// the session key and the seat the roster maps the role to; the function it returns, if any, is told
// the seat the roster then chose. It must not block.
type RosterObserver func(role, key, asked string) (record func(assigned string))

var rosterObserver atomic.Pointer[RosterObserver]

// SetRosterObserver installs the observer; nil removes it.
func SetRosterObserver(observer RosterObserver) {
	if observer == nil {
		rosterObserver.Store(nil)
		return
	}
	rosterObserver.Store(&observer)
}

// observeRoster asks the observer; a fault in it never reaches the turn it watches.
func observeRoster(role, key, asked string) (record func(assigned string)) {
	observer := rosterObserver.Load()
	if observer == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			record = nil
		}
	}()
	return (*observer)(role, key, asked)
}

func recordRoster(record func(assigned string), assigned string) {
	if record == nil {
		return
	}
	defer func() { _ = recover() }()
	record(assigned)
}
