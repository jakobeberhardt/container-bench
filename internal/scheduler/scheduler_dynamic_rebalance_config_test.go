package scheduler

import (
	"container-bench/internal/config"
	"testing"
)

func boolPtr(v bool) *bool { return &v }

func TestDynamicScheduler_RebalanceConfig_DefaultDisabled(t *testing.T) {
	s := &DynamicScheduler{config: &config.SchedulerConfig{}}
	enabled, batch := s.rebalanceModeLocked()
	if enabled || batch {
		t.Fatalf("expected rebalancing disabled by default, got enabled=%v batch=%v", enabled, batch)
	}
}

func TestDynamicScheduler_RebalanceConfig_RebalanceBatchFalse_Disables(t *testing.T) {
	s := &DynamicScheduler{config: &config.SchedulerConfig{RebalanceBatch: boolPtr(false)}}
	enabled, batch := s.rebalanceModeLocked()
	if enabled || batch {
		t.Fatalf("expected rebalance_batch=false to disable rebalancing, got enabled=%v batch=%v", enabled, batch)
	}
}

func TestDynamicScheduler_RebalanceConfig_RebalanceTrue_BatchFalse(t *testing.T) {
	s := &DynamicScheduler{config: &config.SchedulerConfig{Rebalance: boolPtr(true), RebalanceBatch: boolPtr(false)}}
	enabled, batch := s.rebalanceModeLocked()
	if !enabled || batch {
		t.Fatalf("expected enabled=true batch=false, got enabled=%v batch=%v", enabled, batch)
	}
}

func TestDynamicScheduler_RebalanceConfig_RebalanceTrue_BatchTrue(t *testing.T) {
	s := &DynamicScheduler{config: &config.SchedulerConfig{Rebalance: boolPtr(true), RebalanceBatch: boolPtr(true)}}
	enabled, batch := s.rebalanceModeLocked()
	if !enabled || !batch {
		t.Fatalf("expected enabled=true batch=true, got enabled=%v batch=%v", enabled, batch)
	}
}

func TestDynamicScheduler_RebalanceConfig_LegacyBatchTrue_Enables(t *testing.T) {
	s := &DynamicScheduler{config: &config.SchedulerConfig{RebalanceBatch: boolPtr(true)}}
	enabled, batch := s.rebalanceModeLocked()
	if !enabled || !batch {
		t.Fatalf("expected legacy rebalance_batch=true to enable batch rebalancing, got enabled=%v batch=%v", enabled, batch)
	}
}
