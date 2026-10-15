package config

// OpsLane keeps capacity for normal operations: standalone tasks, the ones no goal filed. Goal work
// claims first, so a goal that files more tasks than it settles used to hold every slot and leave
// the rest of the board waiting. While ops work is waiting, goal claims may not take the last
// ReservedSlots slots the lane is short of. No slot is held idle: with no ops work waiting, goals
// fill every slot. Running work is never stopped, and a running ops task inside its reserve is not
// paused for goal work. A disabled lane has no effect.
type OpsLane struct {
	Enabled       bool `yaml:"enabled"`
	ReservedSlots int  `yaml:"reserved_slots,omitempty"` // blank or 0 means 2
}

const opsLaneDefaultReserved = 2

// Reserved is the number of slots goal claims leave to ops work while it waits.
func (o OpsLane) Reserved() int {
	if o.ReservedSlots <= 0 {
		return opsLaneDefaultReserved
	}
	return o.ReservedSlots
}

func (o OpsLane) validate() []string {
	if !o.Enabled {
		return nil
	}
	if o.ReservedSlots < 0 {
		return []string{"orchestrator.ops_lane.reserved_slots cannot be negative"}
	}
	return nil
}
