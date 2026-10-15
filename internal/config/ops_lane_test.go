package config

import (
	"strings"
	"testing"
)

func TestOpsLaneReservedDefaultsToTwo(t *testing.T) {
	if got := (OpsLane{Enabled: true}).Reserved(); got != 2 {
		t.Fatalf("blank reserved_slots = %d, want 2", got)
	}
	if got := (OpsLane{Enabled: true, ReservedSlots: 3}).Reserved(); got != 3 {
		t.Fatalf("reserved_slots 3 = %d", got)
	}
}

func TestOpsLaneValidation(t *testing.T) {
	if problems := (OpsLane{Enabled: true, ReservedSlots: -1}).validate(); len(problems) != 1 || !strings.Contains(problems[0], "reserved_slots") {
		t.Fatalf("negative reserved_slots problems = %v", problems)
	}
	if problems := (OpsLane{ReservedSlots: -1}).validate(); problems != nil {
		t.Fatalf("a disabled lane is not validated, got %v", problems)
	}
	if problems := (OpsLane{Enabled: true}).validate(); problems != nil {
		t.Fatalf("a blank enabled lane is valid, got %v", problems)
	}
}

func TestOpsLaneReadsFromTheConfigFile(t *testing.T) {
	cfg, err := decode([]byte("version: 1\norchestrator:\n  max_parallel: 6\n  ops_lane:\n    enabled: true\n    reserved_slots: 3\n"), "config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if lane := cfg.Orchestrator.OpsLane; !lane.Enabled || lane.Reserved() != 3 {
		t.Fatalf("ops_lane = %+v, want enabled with 3 reserved", lane)
	}
	off, err := decode([]byte("version: 1\norchestrator:\n  max_parallel: 6\n"), "config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if off.Orchestrator.OpsLane.Enabled {
		t.Fatal("the lane must be off unless the config turns it on")
	}
}
